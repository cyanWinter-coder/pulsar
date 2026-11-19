# Cursor acknowledgment persistence and restart recovery

This document describes how a managed cursor persists acknowledgment (mark-delete) progress, what happens
when that persistence fails while a backlog clear or a broker restart is in flight, and the at-least-once
contract that ties those paths together. It complements the Javadoc on `ManagedCursorImpl`; the authoritative
user-facing documentation lives in the [apache/pulsar-site](https://github.com/apache/pulsar-site) repository.

## Two views of one position

A durable cursor tracks the same acknowledgment progress in two places:

| View | Field | Meaning |
|------|-------|---------|
| In-memory position | `markDeletePosition` / `lastMarkDeleteEntry` | What the broker has accepted from consumers. It moves as soon as an acknowledgment is admitted, before the write is durable. |
| Persisted position | `persistentMarkDeletePosition` | What was durably stored, either as an entry in the cursor ledger or as a `ManagedCursorInfo` in the metadata store. |

The gap between the two is the **uncertain window**. Everything at or before the persisted position is
definitely acknowledged across a restart; anything after it may be redelivered. The in-memory position can
legitimately run ahead, because a consumer acknowledgment must not block on storage, but it can never be
treated as durable until a write succeeds.

## Why a failed write keeps the cursor dirty

When a mark-delete cannot be written immediately — the mark-delete rate limiter rejected it, a partial
batch acknowledgment changed cursor state without a write, or a previous write failed — the cursor sets its
`isDirty` flag and records the latest position in `lastMarkDeleteEntry`. The managed-ledger factory's
periodic flush task calls `ManagedCursorImpl.flush()`, which writes the latest dirty position.

The flush must **keep `isDirty` set until a write completes**:

- `flush()` no longer clears the flag before issuing the asynchronous write.
- A successful write clears the flag in its completion callback.
- A failed write leaves the flag set so the next flush retries the same position. The only exception is an
  already-mark-deleted rejection, which means a concurrent write already persisted an equivalent or later
  position and there is nothing left to flush.

Clearing the flag on a failed write would drop the only retry path: the in-memory position would keep moving
while every later flush believed there was nothing to persist, and a broker restart would recover the stale
persisted position, silently rewinding the subscription and redelivering every entry acknowledged since.

## Persistence failure handling

A mark-delete write goes through two storage layers before it is reported as failed:

1. **Cursor ledger append** (`persistPositionToLedger`). On a bookie error the cursor switches to
   `NoLedger` and tries to create a new metadata ledger, queuing further mark-deletes in
   `pendingMarkDeleteOps`.
2. **Metadata-store fallback** (`persistPositionToMetaStore`). If no writable ledger is available, the
   position and the individual-deleted ranges are written to the metadata store instead.

Only when both layers fail does `MarkDeleteCallback.markDeleteFailed` fire. On failure:

- The in-memory `markDeletePosition` is **not** rolled back. Consumers are not asked to re-acknowledge.
- `isDirty` is set so a later flush retries the persistence.
- The broker-facing `AckCallback` propagates the failure to the client/API caller. The broker never reports
  an acknowledgment as successful while its position is still inside the uncertain window.

## Backlog clears and skips

`asyncClearBacklog` and `asyncSkipEntries` are wrappers over `asyncMarkDelete`:

- A clear moves the mark-delete position to the ledger's last position; a skip moves it by a number of
  entries. Both then persist it through the same write path.
- A failure whose cause is `IllegalArgumentException` (`MarkDeletingMarkedPosition`) means a concurrent
  mark-delete already reached the same or a later position, so the operation is treated as complete.
- Every other failure is propagated through `clearBacklogFailed` / `skipEntriesFailed`. The callback checks
  the cause with a null-safe helper, so an exception without a cause cannot mask the real failure with a
  `NullPointerException`.

Because the clear is only a mark-delete, a failed clear follows the same dirty-retry rule as a failed
acknowledgment: the backlog is not considered gone until the position is persisted.

## Restart recovery

When a broker restarts (or a topic is reopened on another broker), the cursor is rebuilt from durable state
in `recover()` / `recoveredCursor()`:

1. Read the latest `ManagedCursorInfo` from the metadata store, and if it points at a cursor ledger, replay
   its `PositionInfo` entries to find the last persisted position.
2. Set both `markDeletePosition` and `persistentMarkDeletePosition` to that recovered position, rebuild the
   individual-deleted ranges and batch-index bitmaps, and recompute the consumed-message counter.
3. Position the read cursor after the recovered mark-delete position.

Recovery always starts from the **persisted** position, never from any broker's in-memory state, which does
not survive the restart. The ordering guarantees are therefore:

- **No acknowledged message is ever skipped.** An entry cannot be past the recovered mark-delete position
  unless its acknowledgment was durable before the restart.
- **Redelivery is possible.** Entries acknowledged inside the uncertain window are redelivered. Consumers
  must handle duplicates (at-least-once), the same guarantee as a normal mark-delete retry.
- A graceful `close()` persists the latest in-memory position first, narrowing the redelivery window for a
  planned shutdown; an abrupt crash cannot, which is why the dirty-retry path matters.

## Storage and interface compatibility

The recovery changes do not introduce any new on-disk or on-wire representation:

- The cursor ledger still stores light-proto `PositionInfo` records, and the metadata store still stores
  `ManagedCursorInfo` with the same fields (mark-delete ledger/entry id, individual-deleted ranges, batch
  index bitmaps, properties).
- `ManagedCursor` and `ManagedLedger` public methods, their signatures and their completion/failure
  semantics are unchanged. The dirty-retry behavior is internal to `ManagedCursorImpl`.
- Older brokers recover new cursors and vice versa, because nothing is added to the serialized formats.
