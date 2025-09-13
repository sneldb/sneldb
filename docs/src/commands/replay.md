# REPLAY

## Purpose

Stream events back in their original append order for a context, optionally restricted to one event type.

## Form

```sneldb
REPLAY [ <event_type:WORD> ]
  FOR <context_id:WORD or STRING>
  [ SINCE <timestamp:STRING> ]
  [ RETURN [ <field:WORD or STRING>, ... ] ]
```

## Variants

- All event types:

```sneldb
REPLAY FOR <context_id>
```

- Only specific event types:

```sneldb
REPLAY <event_type> FOR <context_id>
```

## Examples

```sneldb
REPLAY FOR alice
```

```sneldb
REPLAY order_shipped FOR customer-99
```

```sneldb
REPLAY FOR "user:ext:42" SINCE "2025-08-20T09:00:00Z"
```

```sneldb
REPLAY product FOR user-1 RETURN ["name"]
```

## Behavior

- Routes to the shard owning the context ID.
- Preserves original order.
- If nothing matches: No matching events found.
- `RETURN [ ... ]` limits payload fields in the replayed events. Omit or use `RETURN []` to include all payload fields. Unknown fields are ignored; core fields (`context_id`, `event_type`, `timestamp`) are always present.
