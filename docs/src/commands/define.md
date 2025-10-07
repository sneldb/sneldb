# DEFINE

## Purpose

Register the schema for an event type. `STORE` payloads must conform to this schema.

## Form

```sneldb
DEFINE <event_type:WORD> [ AS <version:NUMBER> ] FIELDS { "key_1": "type_1", ... }
```

## Field pairs

- Keys can be STRING or WORD. The parser will quote WORD keys when converting to JSON.
- Values (types) can be:
  - STRING literals, for example: "int", "string", "string | null"
  - Special logical time types:
    - "datetime" → event time instant; payload accepts ISO-8601 strings or epoch (s/ms/µs/ns) and is normalized to epoch seconds
    - "date" → calendar date; payload accepts "YYYY-MM-DD" (midnight UTC) or epoch and is normalized to epoch seconds
  - ARRAY of strings to define an enum, for example: ["pro", "basic"]
    - Enum variants are case-sensitive ("Pro" != "pro")
- Schema must be flat (no nested objects).

## Examples

```sneldb
DEFINE order_created FIELDS { "order_id": "int", "status": "string" }
```

```sneldb
DEFINE review FIELDS { rating: "int", verified: "bool" }
```

```sneldb
DEFINE order_created AS 2 FIELDS { order_id: "int", status: "string", note: "string | null" }
```

```sneldb
DEFINE subscription FIELDS { plan: ["pro", "basic"] }
```

```sneldb
DEFINE product FIELDS { name: "string", created_at: "datetime", release_date: "date" }
```

## Typical validation errors raised during STORE

- No schema defined
- Missing field `status` in payload
- Field `order_id` is expected to be one of `int`, but got `String`
- Payload contains fields not defined in schema: invalid_field
