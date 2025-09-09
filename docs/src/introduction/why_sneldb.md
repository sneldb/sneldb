# Why SnelDB?

Most databases were never built for events.

They're optimized for records that change: customer profiles, inventory counts, order statuses. But in the real world, especially in modern systems and data pipelines, we’re dealing more and more with **things that happened** — not things that are.

- A user signed up.
- A sensor pinged.
- A document was approved.
- A model prediction was stored.

These aren’t updates. They’re **facts**. Immutable. Time-stamped. Contextual.

## The gap

If you’ve tried to build on top of these kinds of events, you've probably run into one of these:

- **Slow queries** over millions of records because you're using a general-purpose SQL database
- **Too much ceremony**, it’s painful to rebuild a timeline of actions (what happened, when, and in what order)
- **Custom tooling** just to read back historical behavior
- **Mixing logs and storage** (Kafka for ingest, S3 for storage, Athena for queries… and duct tape in between)
- **Hard to filter**, trace, or correlate things once the data grows

And if you work in **AI or data science**, you’ve probably dealt with brittle pipelines, long joins, and the question:

> _“How do I get all the events for this user/session/date range — and trust the output?”_

## The idea

**SnelDB** was born to make event-driven storage and retrieval feel natural — for developers, data engineers, and model builders alike.

It’s a database designed from scratch for:

- **Immutable, append-only data**
- **High-throughput ingest**
- **Fast filtering and replay**
- **Event-type-aware columnar storage**
- **Schema evolution without migrations**
- **Minimal operational overhead**

You store events. You query them. You replay them. That’s it. It does the rest — segmenting, zoning, indexing, compaction — in the background.

## Why not just use X?

- **Kafka?** Great for streaming, not for historical querying.
- **PostgreSQL?** Fantastic RDBMS, but not built for multi-billion-row event logs.
- **Snowflake?** Powerful, but heavy and expensive for interactive filtering.
- **ClickHouse?** Blazing fast, but not optimized for replay semantics and evolving schemas.

SnelDB is a **sweet spot**: light like SQLite, fast like ClickHouse, event-native like Kafka — but simple to reason about.

## Built for builders

Whether you’re:

- Building product analytics dashboards from raw event logs
- Tracking user behavior over time, across sessions or contexts
- Training machine learning models on real-world event sequences
- Auditing critical flows or investigating anomalies
- Archiving time-stamped data for compliance or reporting
- Creating time-travel debugging tools or operational replay systems

SnelDB gives you a clean, reliable foundation to work with immutable facts — fast to store, easy to query, and simple to reason about.

**Simple to embed. Easy to query. Scales with clarity.**

That’s why we built SnelDB.

## Stories from the field

To see why SnelDB exists, it helps to look at a few real situations where traditional tools fall short.

- **Product analytics at scale**
  A growing SaaS company wants to track how users move through their app.
  At first, PostgreSQL is fine. But soon the tables balloon into billions of rows. Queries slow to a crawl, analysts create brittle pipelines, and nobody fully trusts the numbers.
  With SnelDB, they could store clean, immutable event streams, filter them quickly by context, and build dashboards that actually stay fast as volume grows.

- **Machine learning pipelines**
  A data science team trains fraud detection models using transaction histories.
  They struggle to rebuild consistent training sets: data is scattered across Kafka topics, S3 buckets, and ad-hoc SQL queries.
  With SnelDB, they can reliably fetch “all sequences of events leading to flagged outcomes,” ensuring reproducibility and shortening the path from raw logs to usable training data.

- **Auditing in regulated industries**
  A fintech startup needs to prove to auditors what happened, when, and by whom.
  Traditional databases allow updates and deletes, which introduces doubt.
  SnelDB’s append-only design guarantees that past events remain untouched, making it straightforward to demonstrate compliance with minimal operational effort.

- **Operational debugging**
  An infrastructure engineer gets paged at 2am for a production outage.
  Logs are rotated, metrics are sampled, and the picture is incomplete.
  With SnelDB, they can replay the exact sequence of system events leading up to the failure, reconstruct the timeline, and pinpoint the root cause without guesswork.
