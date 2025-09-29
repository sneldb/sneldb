# SnelDB

A lightweight, high‑performance event database. Instead of rows that change over time, SnelDB treats your system as a stream of facts: things that happened, in order. You append events, then filter or replay them — quickly and reliably.

## What SnelDB is

- **A notebook of facts**: every line is an event written once, never edited.
- **A timeline you can slice**: filter by event type, context, and time.
- **A replay button**: reconstruct the story for any `context_id` in original order.

Why this matters: when events are immutable, history is trustworthy. You can rebuild state, audit behavior, and analyze sequences without worrying about hidden mutations.

Refer to the guide for narrative intros and examples:

- [Introduction](https://sneldb.com/introduction.html) (what/why/how)
- [Key Features](https://sneldb.com/introduction/key_features.html) (at a glance)
- [A Gentle Guide for Engineers](https://sneldb.com/quickstart/gentle_intro.html) (mental model)
- [Why SnelDB?](https://sneldb.com/introduction/why_sneldb.html)

## Core ideas

- **Immutability**: events are append‑only. No updates or deletes.
- **Context**: choose the thing whose story you want to tell (`order-9001`, `user-42`).
- **Two reads**: replay one story (FOR a context) or query across many stories (WHERE conditions).
- **Schemas**: define event types explicitly to keep payloads trustworthy and evolvable.
- **Performance by design**: shards, zones, filters, and compaction keep queries fast as data grows.

Deep dives in the guide:

- [Design overview](https://sneldb.com/design/overview.html) and schema evolution
- [Query vs. replay](https://sneldb.com/design/query_replay.html): when to use each
- Scaling model: [shards & zones](https://sneldb.com/design/sharding.html), [compaction](https://sneldb.com/design/compaction.html)

## How you work with it

You interact with SnelDB using a tiny, human‑friendly command language:

- [`DEFINE`](https://sneldb.com/commands/define.html) — describe the shape of an event type
- [`STORE`](https://sneldb.com/commands/store.html) — append a new fact with a JSON payload
- [`QUERY`](https://sneldb.com/commands/query.html) — filter events by type, context, time, and conditions
- [`REPLAY`](https://sneldb.com/commands/replay.html) — stream events back in original order for a context
- [`FLUSH`](https://sneldb.com/commands/flush.html) — create an on‑disk segment checkpoint (useful in tests)

See the guide’s Commands section for syntax and examples:

- [Commands overview](https://sneldb.com/commands.html)
- [Syntax & Operators](https://sneldb.com/commands/syntax.html)
- [DEFINE](https://sneldb.com/commands/define.html) / [STORE](https://sneldb.com/commands/store.html) / [QUERY](https://sneldb.com/commands/query.html) / [REPLAY](https://sneldb.com/commands/replay.html) / [FLUSH](https://sneldb.com/commands/flush.html)

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

- [Introduction](https://sneldb.com/introduction.html)
- [A Gentle Guide for Engineers](https://sneldb.com/quickstart/gentle_intro.html)
- [Commands](https://sneldb.com/commands.html) (overview and syntax)
- [Design](https://sneldb.com/design.html) (overview, storage engine, sharding, replay & query, compaction)

---

Built with ❤️ in Rust.
