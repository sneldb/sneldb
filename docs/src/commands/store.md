# Store

## Purpose

Append an event for a specific context.

## Form

```sneldb
STORE <event_type:WORD> FOR <context_id:WORD or STRING> PAYLOAD {"key":"value", ...}
```

## Constraints

- `<context_id>` can be a WORD (example: user-1) or a quoted STRING.
- `PAYLOAD` must be a flat JSON object (no nested objects).
- `PAYLOAD` must follow schema defined using `DEFINE` command.

## Examples

```sneldb
STORE order_created FOR customer-1 PAYLOAD {"order_id":123,"status":"confirmed"}
```

```sneldb
STORE review FOR "user:ext:42" PAYLOAD {"rating":5,"verified":true}
```

```sneldb
STORE login FOR user-7 PAYLOAD {"device":"android"}
```

## Behavior

- Validates payload against the schema of the event type.
- Rejects missing or extra fields and type mismatches.
- Durability-first: once acknowledged, the event will survive crashes.

## Errors

- `<event_type>` cannot be empty
- `<context_id>` cannot be empty
- Schema validation errors (see `DEFINE`)
- Overload/backpressure (rare): Shard is busy, try again later
