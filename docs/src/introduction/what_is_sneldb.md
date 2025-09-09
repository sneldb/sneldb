# What is SnelDB?

SnelDB is a lightweight, high-performance database designed for **immutable events**.

At its core, it’s a system where you can:

- **Store** events in an append-only fashion
- **Query** them efficiently by type, context, or time
- **Replay** them in order to understand what happened

That’s it. No updates. No deletes. Just fast, reliable access to a growing stream of facts.

## Not your average database

SnelDB is not a general-purpose relational database, a message broker, or a data lake.
It’s a specialized tool focused on **event-driven data**:

- Unlike a **relational database**, SnelDB doesn’t model changing rows. It treats data as a log of things that happened.
- Unlike a **message queue**, it’s built for storage and querying, not just delivery.
- Unlike a **data warehouse**, it’s lightweight and easy to embed in everyday applications.

Think of it as a **database that embraces time and immutability as first-class concepts**.

## A mental model

The easiest way to think about SnelDB is:

- **A notebook for your system’s history**: every line is a fact, recorded once, never erased.
- **A timeline you can slice and filter**: events are grouped by type, context, and time, so you can quickly zoom in.
- **A replay button**: if you need to reconstruct a past sequence, you can ask SnelDB to play it back in order.

## A simple example

Imagine you’re building a payments system.

You might store events like:

```json
{ "event_type": "payment_initiated", "context_id": "user_123",  "payload" : { "amount": 100 }, "timestamp": "2025-08-20T09:30:00Z" }
{ "event_type": "payment_verified",  "context_id": "user_123",  "payload" : { "amount": 100 }, "timestamp": "2025-08-20T09:31:00Z" }
{ "event_type": "payment_settled",   "context_id": "user_123", "payload" : { "amount": 100 }, "timestamp": "2025-08-20T09:35:00Z" }
```

Later, you might want to:

- Fetch all payment_initiated events from last week
- Replay all events for `user_123` in order
- Filter for verified payments over `$500`

And maybe even more:

- Compare the average settlement time for all payments last month
- Find all users who initiated a payment but never settled
- Retrieve the full sequence of events for a disputed transaction
- Generate a distribution of payment amounts across different countries
- Train a model using all past transactions, keeping the exact order of events intact

In a traditional setup, you’d stitch together logs, SQL queries, and custom scripts.
With SnelDB, these queries are **first-class citizens**. For example:

```sneldb
QUERY payment_initiated SINCE 2025-08-01
```

or:

```sneldb
REPLAY FOR user_123
```

even like:

```sneldb
QUERY payment_verified WHERE amount > 500
```

Instead of thinking in terms of tables and joins, you think in terms of events.
SnelDB is designed so the way you ask matches the way you think: “What happened? When? For whom?”
