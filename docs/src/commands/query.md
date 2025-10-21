# QUERY

## Purpose

Filter events by type, optionally by context, time, predicate, and limit.

## Form

```sneldb
QUERY <event_type:WORD>
  [ FOR <context_id:WORD or STRING> ]
  [ SINCE <timestamp:STRING_OR_NUMBER> ]
  [ RETURN [ <field:WORD or STRING>, ... ] ]
  [ WHERE <expr> ]
  [ LIMIT <n:NUMBER> ]
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

```sneldb
QUERY orders SINCE 1735689600000 USING created_at WHERE amount >= 10
# SINCE accepts ISO-8601 strings or numeric epoch in s/ms/µs/ns; all normalized to seconds
```

```sneldb
QUERY product RETURN [name, "price"] WHERE price > 10
```

## Notes

- `SINCE` accepts ISO-8601 strings (e.g., `2025-01-01T00:00:00Z`) or numeric epoch in seconds, milliseconds, microseconds, or nanoseconds. Inputs are normalized to epoch seconds.
- `USING <time_field>` makes `SINCE` and temporal pruning use a payload datetime field (e.g., `created_at`). Defaults to the core `timestamp` field.
- `RETURN [ ... ]` limits the payload fields included in results. Omit to return all payload fields. An empty list `RETURN []` also returns all payload fields.
- Field names in `RETURN` can be bare words or quoted strings.
- Works across in-memory and on-disk segments.
- If nothing matches, returns: No matching events found.

## Gotchas

- Field names used in `WHERE` must exist in the schema for that event type.
- Strings must be double-quoted when you need explicit string literals.
- Unknown fields in `RETURN` are ignored; only schema-defined payload fields (plus core fields `context_id`, `event_type`, `timestamp`) are returned.
- Temporal literals in `WHERE` (e.g., `created_at = "2025-01-01T00:00:01Z"`) are parsed and normalized to epoch seconds. Fractional seconds are truncated; ranges using only sub-second differences may collapse to empty after normalization.
