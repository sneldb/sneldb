# A Gentle Guide for Engineers

SnelDB is built to be small and simple. It keeps track of what happened, in order, and makes it easy to get those facts back out quickly. That’s it. This guide will walk you through how to think about events, how to design them so they’re useful, and how to use SnelDB’s tiny set of commands—`DEFINE`, `STORE`, `QUERY`, and `REPLAY`. Along the way we’ll use a retail shop as an example, but the same ideas apply in many domains.

## Why events?

An event is just a record that something happened: _an order was created_, _a customer signed up_, _a parcel was delivered_. Events don’t change once they’re stored. By keeping them all, you get a trustworthy history. Your application can look back, replay them, and figure out the current state whenever it needs. SnelDB focuses on storing these events and letting you fetch them again quickly. The “what do these events mean?” part stays in your application.

## Two ways of reading

With SnelDB, there are really only two ways you read:

1. **Replay a timeline for one thing.** All the events for a single `context_id` (like an order, a customer, or a device) form a story. If you `REPLAY FOR order-9001`, you’ll see every event for that order in sequence. Your code can fold those into the current state.

2. **Query across many things.** Sometimes you don’t want the whole story of one order, you want a slice across all orders. For that, you use `QUERY`. For example: `QUERY order_created WHERE status="submitted"`. Behind the scenes, SnelDB uses tricks like enum bitmaps and filters to make those queries quick, so you don’t have to think about indexes.

If you remember one thing: replay for one thing’s story, query for slices across many things.

## Choosing a context

So what is this `context_id`? Think of it as “whose story am I telling?” For a retail system:

- An **order** has a start and an end, so it makes sense to use `order-<id>` as the context.
- Inventory belongs to a **SKU**, so `sku-<code>` is a context.
- A **customer profile** belongs to a customer, so `customer-<id>` works.

When you want to be able to say _“show me everything that ever happened to X”_, that X should be a context.

## Designing an event

Name events the way you’d explain them to a teammate: `order_created`, `customer_registered`, `shipment_delivered`. Keep the payload small and clear. Always include:

- The IDs you’ll need to filter by later (`order_id`, `customer_id`, `sku`).
- Enums for fixed sets of values. For example:
  ```sneldb
  "plan": ["basic", "pro", "enterprise"]
  ```
- A timestamp for when it happened.

Here are a few examples:

```sneldb
DEFINE customer_registered FIELDS {
  "customer_id":"string",
  "email":"string",
  "plan":["basic","pro","enterprise"],
  "created_at":"timestamp"
}

DEFINE order_created FIELDS {
  "order_id":"string",
  "customer_id":"string",
  "status":["pending","submitted","cancelled"],
  "created_at":"timestamp"
}

DEFINE shipment_delivered FIELDS {
  "shipment_id":"string",
  "order_id":"string",
  "carrier":["UPS","DHL","FedEx"],
  "delivered_at":"timestamp"
}
```

## Storing events

The very first need is to **record facts**: something happened, and you want to keep it. Writing an event in SnelDB is just that—adding a new fact to the timeline.

```sneldb
STORE customer_registered FOR customer-123
  PAYLOAD {"customer_id":"123","email":"a@b.com","plan":"pro"}

STORE order_created FOR order-9001
  PAYLOAD {"order_id":"9001","customer_id":"123","status":"pending"}

STORE shipment_delivered FOR ship-5001
  PAYLOAD {"shipment_id":"5001","order_id":"9001","carrier":"UPS"}
```

Later on, when dealing with retries or external systems, you might add optional fields like `idempotency_key`. But the heart of storing events is simply: write down the fact.

## Reading events

If you want to know the **current state of one thing**, replay its story:

```sneldb
REPLAY FOR order-9001
```

If you want to know **which events match a condition across many things**, query:

```sneldb
QUERY order_created WHERE customer_id="123"
```

If you need to follow a chain—like from an order to its shipment—query by the keys you included in the payload:

```sneldb
QUERY shipment_delivered WHERE order_id="9001"
```

## How to evolve

SnelDB is built on immutability. Once an event is stored it never changes. If the shape of an event needs to change, we don’t edit old events or add fields onto them. Instead, we create a new version of the schema or define a new event type that represents the new shape.

Older events remain valid and replayable; newer ones follow the updated schema. This way, every event clearly shows which version of the schema it follows, and your code can handle old and new versions side by side. Immutability guarantees that history is stable, while evolution ensures you can keep writing new chapters without breaking the old ones.

## Scaling without extra knobs

You don’t manage indexes or query planners. You simply design your events with the right fields. SnelDB takes care of compression and filtering internally. If a query feels heavy, ask yourself: _did I include the right key in the payload?_

## Streaming

If you need near‑real‑time processing, you don’t need a new command. Just poll with `SINCE` on your timestamp:

```sneldb
QUERY order_created WHERE created_at >= "2025-09-07T00:00:00Z" LIMIT 1000
```

Keep track of the last event you saw in your application and continue from there.

## Other domains

- **Billing:** replay a subscription’s events to learn its current plan; query invoices or payments by `customer_id`.
- **IoT:** replay one device’s events to see its config; query telemetry since last night.
- **Logistics:** replay one parcel’s journey; query all parcels delivered today.

## What SnelDB won’t do

SnelDB will never enforce your workflows, run aggregates, or decide who is allowed to see data. Those belong in your application or other tools. SnelDB’s job is narrower: keep facts safe, and give them back quickly.

## A closing picture

Think of two simple moves:

- **Down:** replay the whole story for one thing.
- **Across:** query slices across many things.

Nearly everything you need can be done by combining these two moves. The database is small on purpose. If you design your events carefully, SnelDB will give you speed and reliability without ever getting in your way.
