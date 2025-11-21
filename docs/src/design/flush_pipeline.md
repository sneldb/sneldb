# Flush Pipeline

## Why it matters

Every event that lands in SnelDB takes a short trip: it is accepted into memory, logged for durability, written to disk later, then published for queries. The flush pipeline is the bridge between “in memory” and “on disk”. Recent changes (segment lifecycle tracking, flush progress IDs, SHOW barriers) tightened the guarantees in this path. This doc walks through the flow in plain language.

## Cast of components

- **MemTable** – In-memory buffer per shard. Fast inserts, bounded by `flush_threshold`.
- **PassiveBufferSet** – Holds recently rotated MemTables so reads can still see them while they flush.
- **FlushProgress** – Hands out monotonic “flush tickets” and records when each one finishes.
- **FlushManager** – Lightweight dispatcher that queues flush jobs onto a shard-local worker.
- **FlushWorker** – Async worker that writes segment files, verifies them, and cleans up.
- **SegmentLifecycleTracker** – Tracks each flushing segment (Flushing → Written → Verified) and owns the associated passive buffer.
- **segment_ids** – Shared list of published segments per shard; queries rely on it to discover data.
- **SHOW / AwaitFlush** – The consumer of the new barrier logic; waits for specific flush tickets to complete before running delta queries.

## Timeline (high level)

1. **MemTable rotation** – Inserts keep filling the active MemTable. When it hits capacity we clone it into the passive set, reserve the next segment ID, and request a flush ticket from `FlushProgress`.
2. **Queue the job** – `FlushManager::queue_for_flush` sends the passive buffer, schema handle, segment ID, and ticket to the shard’s `FlushWorker`.
3. **Write & verify** – The worker registers the flush with `SegmentLifecycleTracker`, writes the segment, verifies it, and only then appends the zero-padded segment name to `segment_ids`.
4. **Lifecycle cleanup** – Once verification succeeds, the tracker marks the segment `Verified`, returns the passive buffer, and WAL files up to that point are pruned.
5. **Ticket complete** – The worker calls `flush_progress.mark_completed(ticket)` and notifies any waiter (explicit FLUSH command, tests, SHOW barrier).
6. **SHOW barrier** – When SHOW asks every shard to `AwaitFlush`, the worker snapshots the latest ticket number and waits only until `completed()` catches up. New writes can keep issuing higher tickets without blocking the wait.

## 1. MemTable rotation & ticketing

- The rotation happens inside `insert_and_maybe_flush`.
- Key steps:
  1. Copy the full MemTable into an `Arc<Mutex<MemTable>>` inside `PassiveBufferSet`.
  2. Swap in a fresh MemTable so inserts never block on disk.
  3. Fetch `flush_id = ctx.flush_progress.next_id()` – this is the ticket tied to the current passive buffer.
  4. Enqueue the job via `FlushManager`.
- Why tickets? They let downstream consumers refer to “all flushes that were already in flight when I started waiting” without affecting future writes.

## 2. Dispatch via FlushManager

- Each shard owns a `FlushManager` with a bounded async channel to its `FlushWorker`.
- `queue_for_flush` only logs intent and pushes the payload (segment id, memtable, passive buffer, schema handle, ticket, optional completion sender).
- No disk work happens here, which keeps ingestion latency low.

## 3. FlushWorker responsibilities

1. **Register lifecycle**
   - For non-empty memtables we call `SegmentLifecycleTracker::register_flush(segment_id, passive_buffer)` and mark the phase as `Flushing`.

2. **Write the segment**
   - A `Flusher` writes column files, filters, metadata, etc. This runs under a shard-level `flush_coordination_lock` so index catalogs stay consistent.

3. **Verify before publishing**
   - `SegmentVerifier` tries to open the just-written files (with retries). Only once verification succeeds do we:
     - `segment_ids.write().push(format!("{:05}", segment_id))`
     - `SegmentLifecycleTracker::mark_written` followed by `mark_verified`
     - `SegmentLifecycleTracker::clear_and_complete` to flush the passive buffer copy

4. **Cleanup**
   - `WalCleaner` prunes WAL files up to the segment’s boundary.

5. **Error handling**
   - Failures skip the `segment_ids` update and leave the passive buffer registered so the data can be retried after restart. The WAL still contains the events, so nothing is lost.

6. **Ticket completion**
   - Regardless of success, `flush_progress.mark_completed(ticket)` fires, then any optional oneshot completion channel is notified. Tickets therefore form a strict happens-after chain for SHOW barriers.

## 4. SegmentLifecycleTracker in practice

- Lifecycle phases:
  - `Flushing` – passive buffer is registered; files are being written.
  - `Written` – files exist on disk but haven’t been verified.
  - `Verified` – files passed verification; passive buffer can be cleared.
- Why we need it:
  - Queries can safely merge results from passive buffers and published segments without seeing double or missing rows.
  - Passive buffers only disappear once the durable copy is proven healthy.
  - If a flush fails midway, the tracker still owns the buffer so it can be retried or replayed from WAL.

## 5. FlushProgress & SHOW barriers

- `FlushProgress` holds two atomics: `submitted` (last ticket handed out) and `completed` (highest ticket confirmed finished).
- When SHOW wants a consistent snapshot before running its delta query:
  1. Each shard receives `ShardMessage::AwaitFlush`.
  2. The worker snapshots `target = flush_progress.snapshot()`.
  3. It waits (polling) until `flush_progress.completed() >= target`.
  4. If the shard was idle, the wait returns immediately.
- Advantages of this model:
  - SHOW doesn’t force new flushes; it simply waits for the already queued ones.
  - Continuous ingest doesn’t starve the wait, because any tickets issued *after* the snapshot are ignored for that request.
  - Any other subsystem can reuse the same barrier semantics (e.g., shutdown, diagnostics).

## 6. Failure resilience recap

- WAL keeps every event durable until its segment is safely written and verified; only then do we prune.
- `segment_ids` only references directories that already exist, which prevents CI-only races where fast readers tried to mmap files that weren’t there yet.
- `FlushProgress` ensures explicit waits and user-facing commands can deterministically block on flushes without busy-looping or issuing redundant work.
- `SegmentLifecycleTracker` is the guardrail that keeps passive buffers alive until their on-disk counterparts are proven good.

## Mental model

Think of each memtable rotation as handing a “package” plus a numbered receipt to the flush conveyor belt. The worker assembles the package, checks it, places it on the shelf (segment_ids), cleans the workbench (WAL), and finally stamps the receipt as done. Any customer (SHOW) can walk up to the desk, note the latest receipt number, and wait until all packages up to that number are on the shelf. New packages keep flowing without blocking anyone.

