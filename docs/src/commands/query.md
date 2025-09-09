# QUERY

## Purpose

Filter events by type, optionally by context, time, predicate, and limit.

## Form

```sneldb
QUERY <event_type:WORD> [ FOR <context_id:WORD or STRING> ] [ SINCE timestamp:STRING ] [ WHERE  ] [ LIMIT  ]
```

## Examples

```sneldb
QUERY order_created WHERE status="confirmed"
```

```sneldb
QUERY order_created WHERE status=confirmed
```

```sneldb
QUERY order_created WHERE id > 13 AND id < 15
```

```sneldb
QUERY order_created WHERE country!="NL"
```

```sneldb
QUERY order_created WHERE country="NL" OR country="FR"
```

```sneldb
QUERY login FOR user-1 WHERE device="android"
```

```sneldb
QUERY payment SINCE "2025-08-01T00:00:00Z" WHERE amount >= 500 LIMIT 100
```

## Notes

- `SINCE` is a STRING timestamp.
- Works across in-memory and on-disk segments.
- If nothing matches, returns: No matching events found.

## Gotchas

- Field names used in `WHERE` must exist in the schema for that event type.
- Strings must be double-quoted when you need explicit string literals.
