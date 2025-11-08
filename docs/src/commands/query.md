# QUERY

## Purpose

Filter events by type, optionally by context, time, predicate, and limit.

## Form

```sneldb
QUERY <event_type:WORD>
  [ FOR <context_id:WORD or STRING> ]
  [ SINCE <timestamp:STRING_OR_NUMBER> ]
  [ USING <time_field:WORD> ]
  [ RETURN [ <field:WORD or STRING>, ... ] ]
  [ WHERE <expr> ]
  [ <aggregations> ]
  [ PER <time_granularity: HOUR|DAY|WEEK|MONTH> [ USING <time_field:WORD> ] ]
  [ BY <field> [, <field> ...] [ USING <time_field:WORD> ] ]
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

### Aggregations

```sneldb
# Count all orders
QUERY orders COUNT

# Count unique contexts (users) per country
QUERY orders COUNT UNIQUE context_id BY country

# Sum and average amount by day using created_at field
QUERY orders TOTAL amount, AVG amount PER DAY USING created_at

# Multiple metrics with grouping
QUERY orders COUNT, TOTAL amount, AVG amount BY country

# Min/Max over comparable fields
QUERY orders MIN amount, MAX amount BY country
```

## Notes

- `SINCE` accepts ISO-8601 strings (e.g., `2025-01-01T00:00:00Z`) or numeric epoch in seconds, milliseconds, microseconds, or nanoseconds. Inputs are normalized to epoch seconds.
- `USING <time_field>` makes `SINCE` and temporal pruning use a payload datetime field (e.g., `created_at`). Defaults to the core `timestamp` field.
- `RETURN [ ... ]` limits the payload fields included in results. Omit to return all payload fields. An empty list `RETURN []` also returns all payload fields.
- Field names in `RETURN` can be bare words or quoted strings.
- Works across in-memory and on-disk segments.
- If nothing matches, returns: No matching events found.

### Aggregation notes

- Aggregations are requested via one or more of: `COUNT`, `COUNT UNIQUE <field>`, `COUNT <field>`, `TOTAL <field>`, `AVG <field>`, `MIN <field>`, `MAX <field>`.
- Optional `BY <fields...>` groups results by one or more payload fields.
- Optional `PER <HOUR|DAY|WEEK|MONTH>` buckets results by the chosen time field. You can select the time field for bucketing with `USING <time_field>`; default is `timestamp`.
- `LIMIT` on aggregation caps the number of distinct groups produced (it does not limit events scanned within those groups).
- Aggregations return a tabular result with columns: optional `bucket`, grouped fields, followed by metric columns like `count`, `total_<field>`, `avg_<field>`, `min_<field>`, `max_<field>`.

## Sequence Queries

SnelDB supports sequence matching queries that find events that occur in a specific order for the same entity. This is perfect for funnel analysis, conversion tracking, and understanding event dependencies.

### Basic Form

```sneldb
QUERY <event_type_a> FOLLOWED BY <event_type_b> LINKED BY <link_field>
QUERY <event_type_a> PRECEDED BY <event_type_b> LINKED BY <link_field>
```

### Concepts

- **FOLLOWED BY**: Finds events where `event_type_b` occurs after `event_type_a` in time
- **PRECEDED BY**: Finds events where `event_type_b` occurred before `event_type_a` in time
- **LINKED BY**: Defines the field that connects events together (e.g., `user_id`, `order_id`, `session_id`)

### Examples

**Funnel analysis**: Find users who viewed the checkout page and then created an order:

```sneldb
QUERY page_view FOLLOWED BY order_created LINKED BY user_id
```

**With WHERE clause**: Only count checkout page views:

```sneldb
QUERY page_view FOLLOWED BY order_created LINKED BY user_id WHERE page_view.page="/checkout"
```

**Event-specific filters**: Filter both events in the sequence:

```sneldb
QUERY page_view FOLLOWED BY order_created LINKED BY user_id
WHERE page_view.page="/checkout" AND order_created.status="done"
```

**PRECEDED BY**: Find orders that were preceded by a payment failure:

```sneldb
QUERY order_created PRECEDED BY payment_failed LINKED BY user_id WHERE order_created.status="done"
```

**Avoiding ambiguity**: If both event types have the same field name, use event-prefixed fields:

```sneldb
# This will return 400 Bad Request if both order_created and payment_failed have a "status" field
QUERY order_created PRECEDED BY payment_failed LINKED BY user_id WHERE status="done"

# Use event-prefixed fields to disambiguate
QUERY order_created PRECEDED BY payment_failed LINKED BY user_id WHERE order_created.status="done"
```

**Different link fields**: Use order_id instead of user_id:

```sneldb
QUERY order_created FOLLOWED BY order_cancelled LINKED BY order_id
```

### How It Works

1. **Grouping**: Events are grouped by the `link_field` value (e.g., all events for `user_id="u1"` are grouped together)
2. **Sorting**: Within each group, events are sorted by timestamp
3. **Matching**: The two-pointer algorithm finds matching sequences efficiently
4. **Filtering**: WHERE clauses are applied before matching to reduce the search space

### WHERE Clause Behavior

- **Event-prefixed fields**: Use `event_type.field` to filter specific events (e.g., `page_view.page="/checkout"`)
- **Common fields**: Fields without a prefix apply to all events (e.g., `timestamp > 1000`)
- **Combined**: You can mix event-specific and common filters with `AND`/`OR`
- **Ambiguity detection**: If a common field (without event prefix) exists in multiple event types within the sequence, the query will return a `400 Bad Request` error. Use event-prefixed fields to disambiguate (e.g., `order_created.status="done"` instead of `status="done"` when both `order_created` and `payment_failed` have a `status` field)

### Performance

Sequence queries are optimized for performance:

- **Columnar processing**: Events are processed in columnar format without materialization
- **Early filtering**: WHERE clauses are applied before grouping and matching
- **Parallel collection**: Zones for different event types are collected in parallel
- **Index usage**: Existing indexes on `link_field` and `event_type` are leveraged

### Notes

- Both events in the sequence must have the same value for the `link_field`
- For `FOLLOWED BY`, `event_type_b` must occur at the same timestamp or later than `event_type_a`
- For `PRECEDED BY`, `event_type_b` must occur strictly before `event_type_a` (same timestamp does not match)
- The query returns both events from each matched sequence
- `LIMIT` applies to the number of matched sequences, not individual events

## Gotchas

- Field names used in `WHERE` must exist in the schema for that event type.
- Strings must be double-quoted when you need explicit string literals.
- Unknown fields in `RETURN` are ignored; only schema-defined payload fields (plus core fields `context_id`, `event_type`, `timestamp`) are returned.
- Temporal literals in `WHERE` (e.g., `created_at = "2025-01-01T00:00:01Z") are parsed and normalized to epoch seconds. Fractional seconds are truncated; ranges using only sub-second differences may collapse to empty after normalization.
- In sequence queries, the `link_field` must exist in both event types' schemas.
- In sequence queries, if a WHERE clause uses a common field (without event prefix) that exists in multiple event types, you must use event-prefixed fields to disambiguate. For example, if both `order_created` and `payment_failed` have a `status` field, use `order_created.status="done"` instead of `status="done"` to avoid ambiguity errors.
