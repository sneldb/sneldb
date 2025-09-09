# Key Features

SnelDB is small in surface area but powerful in practice.
Here are the highlights that make it different:

## 1. Append-only storage

Events are **immutable**.
Once stored, they’re never updated or deleted — which means:

- Perfect audit trails
- Predictable replay of past behavior
- No risk of hidden mutations breaking analysis

## 2. Simple, human-readable commands

No SQL boilerplate. No obscure APIs.
SnelDB has a compact command language that reads like plain English:

```sneldb
DEFINE payment FIELDS { "amount": "int", "status": "string" }
STORE payment FOR user-123 PAYLOAD {"amount": 250, "status":"verified"}
QUERY payment WHERE status="verified"
REPLAY FOR user-123
```

Fast to learn. Easy to remember.
Case-insensitive and JSON-native.

## 3. Fast queries at scale

Under the hood, SnelDB uses an LSM-tree design with:

- Shards for parallelism
- Zones and filters to skip irrelevant data
- Compaction to keep reads efficient over time

The result: queries stay snappy whether you have thousands or billions of events.

## 4. Replay built in

You don’t just query — you can replay events in order:

```sneldb
REPLAY order_created FOR customer-42
```

This makes debugging, time-travel analysis, and sequence modeling natural parts of the workflow.

## 5. Flexible schemas

SnelDB supports schema definitions per event type, with:

- Strict validation: payloads must match fields
- Optional fields: declared as string | null
- Clear errors when something doesn’t line up

This keeps data trustworthy without slowing you down.

## 6. Designed for AI & analytics

Because events are ordered, immutable, and replayable, SnelDB is a natural fit for:

- Training models on real-world sequences
- Feeding pipelines with reproducible datasets
- Analyzing behavior over time without complex joins
- Auditing decision processes with confidence

## 7. Lightweight & embeddable

SnelDB is written in Rust with minimal dependencies.
It runs anywhere — from a laptop dev setup to production servers — without heavyweight orchestration.

You can drop it into your stack as a focused, reliable event database.

## 8. Safety by design

SnelDB is built in Rust, which brings **memory safety, thread safety, and performance** without garbage collection.

This means:

- No segfaults or memory leaks corrupting your data
- Concurrency without data races
- Predictable performance, even under load

When you’re storing critical events, **safety is not optional** — and Rust helps guarantee it from the ground up.

---

In short:
SnelDB is designed to be small but sharp — a tool that does one thing well:
**make working with immutable events simple, fast, and reliable.**
