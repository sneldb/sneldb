# Introduction

SnelDB is a lightweight, high‑performance database for immutable events. You append facts, then filter or replay them—quickly and reliably.

## What it is

- Store: append events with a type, context_id, timestamp, and payload
- Query: filter by event type, context, time, and conditions
- Sequence queries: find events that occur in order for the same entity
- Replay: stream events for a context in original order

```sneldb
DEFINE payment FIELDS {"amount":"int","status":"string"}
STORE payment FOR user-123 PAYLOAD {"amount":250,"status":"verified"}
QUERY payment WHERE status="verified"
QUERY page_view FOLLOWED BY order_created LINKED BY user_id
REPLAY FOR user-123
```

## Why it exists

General-purpose databases and queues struggle with large, evolving event logs. SnelDB is built for:

- Immutable, append-only data
- Fast filtering at scale (columnar + pruning)
- Ordered replay per context
- Minimal ops overhead

## Key features

- Append-only storage (perfect audit trails; predictable recovery)
- Simple, human‑readable commands (JSON‑native)
- Fast queries at scale (shards, zones, compaction)
- Sequence matching (find ordered event pairs for funnel analysis and conversion tracking)
- Modern temporal indexing (per-field calendars and slabbed temporal indexes)
- Replay built in (time‑travel debugging, sequence modeling)
- Flexible schemas (strict validation; optional fields)
- Lightweight & safe (Rust; embeddable; no GC)

## Who it’s for

- Product analytics and auditing
- ML pipelines on event sequences
- Operational debugging and timeline reconstruction
