# SnelDB

A lightweight, high‑performance event database. Instead of rows that change over time, SnelDB treats your system as a stream of facts: things that happened, in order. You append events, then filter or replay them — quickly and reliably.

## What SnelDB is

- **A notebook of facts**: every line is an event written once, never edited.
- **A timeline you can slice**: filter by event type, context, and time.
- **A replay button**: reconstruct the story for any `context_id` in original order.

Why this matters: when events are immutable, history is trustworthy. You can rebuild state, audit behavior, and analyze sequences without worrying about hidden mutations.

Refer to the guide for narrative intros and examples:

- Introduction (what/why/how)
- Key Features (at a glance)
- A Gentle Guide for Engineers (mental model)

## Core ideas

- **Immutability**: events are append‑only. No updates or deletes.
- **Context**: choose the thing whose story you want to tell (`order-9001`, `user-42`).
- **Two reads**: replay one story (FOR a context) or query across many stories (WHERE conditions).
- **Schemas**: define event types explicitly to keep payloads trustworthy and evolvable.
- **Performance by design**: shards, zones, filters, and compaction keep queries fast as data grows.

Deep dives in the guide:

- Event design and schema evolution
- Query vs. replay: when to use each
- Scaling model: shards, zones, and compaction

## How you work with it

You interact with SnelDB using a tiny, human‑friendly command language:

- `DEFINE` — describe the shape of an event type
- `STORE` — append a new fact with a JSON payload
- `QUERY` — filter events by type, context, time, and conditions
- `REPLAY` — stream events back in original order for a context
- `FLUSH` — create an on‑disk segment checkpoint (useful in tests)

See the guide’s Commands section for syntax and examples:

- Commands overview
- Syntax & Operators
- DEFINE / STORE / QUERY / REPLAY / FLUSH

## When to use SnelDB

- You need a faithful history (audit trails, investigations, debugging).
- You model behavior as sequences (product analytics, ML on event streams).
- You want fast, simple reads without managing indexes or complex query planners.

Scenarios and stories in the guide:

- Product analytics at scale
- Machine learning pipelines on sequences
- Auditing and compliance
- Operational replay and time‑travel debugging

## What SnelDB is not

- Not a general‑purpose relational database
- Not a message broker (though it plays well with streams)
- Not a heavy data warehouse

It’s small on purpose: a focused event database that’s easy to embed and reason about.

## Learn more

Start with the guide:

- Introduction
- A Gentle Guide for Engineers
- Commands (overview and syntax)
- Design (overview, storage engine, sharding, replay & query, compaction)

---

Built with ❤️ in Rust.
