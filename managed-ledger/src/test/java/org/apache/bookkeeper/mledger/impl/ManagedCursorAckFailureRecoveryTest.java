/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.mledger.impl;

import static org.apache.bookkeeper.mledger.util.ManagedLedgerTestUtil.defaultConfig;
import static org.assertj.core.api.Assertions.assertThat;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ClearBacklogCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Covers the interaction of acknowledgment persistence failures, backlog clearing and cursor recovery.
 *
 * <p>The contract under test is at-least-once: a persistence failure must never make the broker believe an
 * acknowledgment is durable while it is not, so a broker restart can only redeliver entries, never skip
 * entries that were acknowledged but whose mark-delete position was not persisted. Restarts are simulated by
 * reopening the same managed ledger through a second factory, without the graceful close that would persist
 * the in-memory position and hide a lost write.
 */
public class ManagedCursorAckFailureRecoveryTest extends MockedBookKeeperTestCase {

    // Factories opened to simulate a broker restart. They must outlive the reopened ledger handles, so they
    // are shut down after the test instead of being closed inline, which would also close the shared ledgers.
    private final List<ManagedLedgerFactory> recoveryFactories = new ArrayList<>();

    @AfterMethod(alwaysRun = true)
    public void shutdownRecoveryFactories() throws Exception {
        for (ManagedLedgerFactory recoveryFactory : recoveryFactories) {
            recoveryFactory.shutdown();
        }
        recoveryFactories.clear();
    }

    /** Make every persistence attempt fail, including retries and the metadata-store fallback. */
    private void failAllPersistence() {
        // Cursor-ledger appends consume the add-entry failure queue and the metadata-ledger creation triggered
        // by the NoLedger state consumes the general failure queue. Program both so every persistence path
        // fails while the metadata store fallback is unavailable.
        for (int i = 0; i < 8; i++) {
            bkc.addEntryFailAfter(i, BKException.Code.NoBookieAvailableException);
        }
        for (int i = 0; i < 8; i++) {
            bkc.failAfter(i, BKException.Code.NoBookieAvailableException);
        }
        metadataStore.setAlwaysFail(new MetadataStoreException("injected cursor metadata failure"));
    }

    private void restorePersistence() {
        metadataStore.unsetAlwaysFail();
        // Consume the injected bookie failures that the failed operation did not need, so later successful
        // operations are not tripped by a leftover programmed failure.
        bkc.drainProgrammedFailures();
    }

    /** Reopen the managed ledger from shared storage without the first factory performing a graceful close. */
    private ManagedLedgerImpl reopenAfterCrash(String ledgerName, ManagedLedgerConfig config) throws Exception {
        ManagedLedgerFactory recoveryFactory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        recoveryFactories.add(recoveryFactory);
        return (ManagedLedgerImpl) recoveryFactory.open(ledgerName, config);
    }

    /** Keep all test entries inside one managed ledger so recovery does not move positions across ledgers. */
    private static ManagedLedgerConfig testConfig() {
        return defaultConfig().setMaxEntriesPerLedger(100);
    }

    /**
     * A failed persistence moves the in-memory mark-delete position and leaves the cursor dirty. If a periodic
     * flush of the dirty position fails and clears the dirty flag anyway, the acknowledgment progress is lost
     * on restart even though later flushes had a chance to persist it. The cursor must stay dirty until a flush
     * succeeds, so the next flush retries the persistence and the position survives a restart.
     */
    @Test(timeOut = 60000)
    public void testFailedFlushKeepsCursorDirtyUntilPersistedAndRecovered() throws Exception {
        ManagedLedgerConfig config = testConfig();
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("dirty-flush-retry", config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");

        Position firstPosition = ledger.addEntry("entry-0".getBytes(StandardCharsets.UTF_8));
        Position nextPosition = ledger.addEntry("entry-1".getBytes(StandardCharsets.UTF_8));
        // Keep messages after the acknowledged positions so recovery does not advance the cursor across
        // ledgers while opening, which would move the recovered mark-delete position.
        ledger.addEntry("entry-2".getBytes(StandardCharsets.UTF_8));
        ledger.addEntry("entry-3".getBytes(StandardCharsets.UTF_8));
        cursor.markDelete(firstPosition);
        Position persistedPosition = cursor.getPersistentMarkDeletedPosition();

        // A failed persistence moves the in-memory position and leaves the cursor dirty.
        CompletableFuture<Throwable> failedAck = new CompletableFuture<>();
        failAllPersistence();
        cursor.asyncMarkDelete(nextPosition, new MarkDeleteCallback() {
            @Override
            public void markDeleteComplete(Object ctx) {
                failedAck.complete(null);
            }

            @Override
            public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                failedAck.complete(exception);
            }
        }, null);
        assertThat(failedAck.get(30, TimeUnit.SECONDS)).isNotNull();
        assertThat(cursor.isDirty()).isTrue();
        assertThat(cursor.getMarkDeletedPosition()).isEqualTo(nextPosition);
        assertThat(cursor.getPersistentMarkDeletedPosition()).isEqualTo(persistedPosition);

        // While storage is still unavailable, a periodic flush must keep the cursor dirty instead of clearing
        // the flag and losing the only retry path.
        cursor.flush();
        Awaitility.await().untilAsserted(() -> assertThat(cursor.isDirty()).isTrue());
        assertThat(cursor.getPersistentMarkDeletedPosition()).isEqualTo(persistedPosition);
        restorePersistence();

        // A later flush succeeds: the dirty acknowledgment is finally persisted.
        cursor.flush();
        Awaitility.await().untilAsserted(() -> assertThat(cursor.isDirty()).isFalse());
        Awaitility.await().untilAsserted(() ->
                assertThat(cursor.getPersistentMarkDeletedPosition()).isEqualTo(nextPosition));

        // Simulate a broker restart: recovery reads the last persisted position, not the stale position that
        // the failed flush would have lost if the dirty flag had been cleared.
        ManagedLedgerImpl reopened = reopenAfterCrash("dirty-flush-retry", config);
        ManagedCursor recovered = reopened.openCursor("sub");
        assertThat(recovered.getMarkDeletedPosition()).isEqualTo(nextPosition);
    }

    /**
     * When both the cursor-ledger write and its metadata-store fallback fail, the mark-delete fails, the
     * in-memory position stays ahead of the persisted one, and recovery after a restart rewinds to the last
     * persisted position. Entries acknowledged in between are redelivered, but no acknowledgment progress is
     * silently lost.
     */
    @Test(timeOut = 60000)
    public void testMarkDeleteFailureRedeliversOnRestartWithoutLosingProgress() throws Exception {
        ManagedLedgerConfig config = testConfig();
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("markdelete-failure-restart", config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");

        Position p0 = ledger.addEntry("entry-0".getBytes(StandardCharsets.UTF_8));
        Position p1 = ledger.addEntry("entry-1".getBytes(StandardCharsets.UTF_8));
        cursor.markDelete(p0);
        Position persistedBeforeFailure = cursor.getPersistentMarkDeletedPosition();

        CompletableFuture<Throwable> ackResult = new CompletableFuture<>();
        failAllPersistence();
        cursor.asyncMarkDelete(p1, new MarkDeleteCallback() {
            @Override
            public void markDeleteComplete(Object ctx) {
                ackResult.complete(null);
            }

            @Override
            public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                ackResult.complete(exception);
            }
        }, null);
        assertThat(ackResult.get(30, TimeUnit.SECONDS)).isNotNull();
        restorePersistence();

        // The in-memory position advanced while the persisted position did not: the gap is the redelivery
        // window that at-least-once semantics allow.
        assertThat(cursor.getMarkDeletedPosition()).isEqualTo(p1);
        assertThat(cursor.getPersistentMarkDeletedPosition()).isEqualTo(persistedBeforeFailure);

        ManagedLedgerImpl reopened = reopenAfterCrash("markdelete-failure-restart", config);
        ManagedCursor recovered = reopened.openCursor("sub");
        assertThat(recovered.getMarkDeletedPosition()).isEqualTo(persistedBeforeFailure);
        assertThat(recovered.getNumberOfEntriesInBacklog(false)).isGreaterThanOrEqualTo(1);
    }

    /**
     * A backlog clear whose persistence fails must surface the failure instead of completing successfully, and
     * the failure callback must not throw an NPE when the exception has no cause. The backlog must still be
     * present after recovery, so no message skipped by the failed clear is lost.
     */
    @Test(timeOut = 60000)
    public void testClearBacklogFailureIsReportedAndBacklogRecovers() throws Exception {
        ManagedLedgerConfig config = testConfig();
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("clear-backlog-failure", config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");

        for (int i = 0; i < 3; i++) {
            ledger.addEntry(("entry-" + i).getBytes(StandardCharsets.UTF_8));
        }
        assertThat(cursor.getNumberOfEntriesInBacklog(false)).isEqualTo(3);

        CompletableFuture<Throwable> clearResult = new CompletableFuture<>();
        failAllPersistence();
        cursor.asyncClearBacklog(new ClearBacklogCallback() {
            @Override
            public void clearBacklogComplete(Object ctx) {
                clearResult.complete(null);
            }

            @Override
            public void clearBacklogFailed(ManagedLedgerException exception, Object ctx) {
                clearResult.complete(exception);
            }
        }, null);

        assertThat(clearResult.get(30, TimeUnit.SECONDS)).isNotNull();
        restorePersistence();

        // Restart without a graceful close: the failed clear must not be treated as durable.
        ManagedLedgerImpl reopened = reopenAfterCrash("clear-backlog-failure", config);
        ManagedCursor recovered = reopened.openCursor("sub");
        assertThat(recovered.getNumberOfEntriesInBacklog(false)).isEqualTo(3);

        // Once the storage is healthy again, clearing the backlog succeeds and is durable across another
        // crash-style restart.
        recovered.clearBacklog();
        assertThat(recovered.getNumberOfEntriesInBacklog(false)).isZero();
        ManagedLedgerImpl reopenedAgain = reopenAfterCrash("clear-backlog-failure", config);
        assertThat(reopenedAgain.openCursor("sub").getNumberOfEntriesInBacklog(false)).isZero();
    }
}
