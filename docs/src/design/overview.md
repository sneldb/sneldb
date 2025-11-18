# Overview

## What this section is

- A short tour of how SnelDB works inside: the big components and how data flows between them.
- Enough context for contributors to find their bearings without reading the whole codebase first.

## The big picture

- Commands enter via frontends (TCP/UNIX/HTTP/WebSocket) and are parsed, validated, and dispatched.
- Writes go through a WAL for durability, land in an in-memory table, and get flushed into immutable segments on disk.
- Reads (query/replay) scan the in-memory table and segments, skipping as much as possible using zone metadata and filters.
- Background compaction keeps segments tidy so read performance stays predictable.
- Sharding by `context_id` spreads work and makes per-context replay cheap.

## Lifecycle at a glance

- `DEFINE`: register or update the schema for an event type (used to validate STORE).
- `STORE`: validate payload → append to WAL → apply to MemTable → later flush to a new segment.
- `QUERY`: fan out to shards, prune zones and project only needed columns, evaluate predicates, merge results.
- `REPLAY`: route to the shard for the context_id, stream events in original append order (optionally narrowed by event type).
- `FLUSH`: force a MemTable flush to produce a new immutable segment (useful in tests/checkpoints).

## What runs where

- Commands and flow control: command/parser, command/dispatcher, command/handlers.
- Storage engine: engine/core/\* for WAL, memory, segments, zones, filters; engine/store, engine/query, engine/replay.
- Sharding and concurrency: engine/shard/\* (manager, worker, messages).
- Background work: engine/compactor/\* for segment merging and cleanup.
- Wiring and I/O: frontend/_ listeners; shared/_ for config, responses, logging.

## Key guarantees (high level)

- Durability once a `STORE` is acknowledged (WAL first).
- Immutability of events and on-disk segments (compaction replaces whole files, never edits in place).
- Ordered replay per `context_id`.
- Schema-validated payloads (strict by default, optional fields via union types).
- Bounded memory via shard-local backpressure.

## What this section doesn’t do

- It won’t dive into file formats or algorithmic details; those live in the focused pages that follow.
- It won’t prescribe ops/production practices; see the development/operations parts of the book.

## How to use this section

Skim this page, then jump to the piece you’re touching:

- [Changing parsing or adding a command](../commands/)
- [Touching durability/flush/segment files](./storage_engine.md)
- [Threading, channels, and routing](./sharding.md)
- [Anything read-path related](./query_replay.md)
- [Background merging/policies](./compaction.md)
- [Config, responses, logging, tests](./infrastructure.md)

That’s the map. Next pages keep the same tone and size: just enough to guide you to the right code.

## Core concepts

- Event: time-stamped, immutable fact with a typed payload
- Event type & schema: defined via DEFINE, validates payload shape
- Context: groups related events under a context_id
- Shard: independent pipeline — WAL → MemTable → Flush → Segments
- WAL: per-shard durability log; replayed on startup
- MemTable: in-memory buffer; flushed when full
- Segment: immutable on-disk unit with columns, zones, filters, indexes
- Zone: fixed-size block inside a segment with pruning metadata
- Compaction: merges small segments to keep reads predictable
