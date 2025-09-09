# Threading and Async

## What it is

- Networking is handled with async tasks (Tokio) for each client connection.
- Work is executed by per-shard worker tasks, communicated via message passing.
- This separates I/O from data processing and keeps shard state isolated.

## Core pieces

- Frontends — Unix/TCP/HTTP listeners accept connections and spawn a task per client.
- Connection — reads lines, parses commands, and dispatches them for execution.
- Shard Manager — owns shards and routes work by hashing `context_id`.
- Shard (worker) — long‑lived task that owns WAL, MemTables, flush queue, and segment list; handles Store, Query, Replay, Flush.
- Channels — `tokio::sync::mpsc` for sending typed messages to shards.
- Schema Registry — shared via `Arc<tokio::sync::RwLock<SchemaRegistry>>`.

## How it works

- Startup

  - Initialize the schema registry and shard manager.
  - Bind a Unix listener and start accepting connections.
  - Spawn background workers (flush, compaction) per shard.

- Connection handling

  - Spawn a task per client.
  - Read lines, parse into commands, dispatch to the shard manager.

- Store

  - Route to shard by `context_id`.
  - Append to WAL, update active MemTable; rotate and enqueue flush when needed.

- Query

  - Broadcast to all shards.
  - Each shard scans its in‑memory and on‑disk state and returns matches; results are merged.

- Replay

  - Route to the shard for the `context_id`.
  - Stream events in original append order for that context.

- Flush

  - Broadcast; shards rotate MemTables and enqueue flush to produce a new segment.

## Why this design

- Async I/O: efficient, scalable handling of many connections.
- Shard workers: clear ownership and predictable performance.
- Separation of concerns: networking and storage logic don’t intermingle.

## Invariants

- Frontends do not perform disk I/O or modify indexes directly.
- Shard workers own shard state; cross‑shard mutable sharing is avoided.
- Schema access uses async `RwLock` for safe concurrent reads/writes.

## Operational notes

- Bounded shard mailboxes apply local backpressure; tune channel sizes as needed.
- Number of shards controls parallelism; size to match CPU/core availability.
- Monitor channel depth and lock contention to spot hotspots.

## Further Reading

- [Sharding](./sharding.md)
- [Storage Engine](./storage_engine.md)
- [Query & Replay](./query_replay.md)
- [Infrastructure](./infrastructure.md)
