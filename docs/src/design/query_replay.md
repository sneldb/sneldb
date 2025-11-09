# Query and Replay

## Overview

SnelDB reads come in two flavors:

- `QUERY`: filter one event type by predicates, time, and optional `context_id`; may span shards.
- `REPLAY`: stream all events for one `context_id` (optionally one type) in original append order; single shard.

Both use the same internals as the write path: in‑memory MemTable, on‑disk immutable segments, per‑segment zones, and compact per‑field filters.

## When to Use Which

- Use `QUERY` for analytics, debugging slices, and ad‑hoc filters across many contexts.
- Use `REPLAY` to rebuild state or audit the exact sequence for one context.

### Examples

- QUERY

  - Investigate: "All `order_created` over $100 in the last 24h across all users"
  - Dashboard: "Errors by type this week"
  - Debug: "Sessions with `status = 'pending'` and `retries > 3`"

- REPLAY
  - Operational debugging (incident timeline)
    ```sneldb
    REPLAY system_event FOR host-123 SINCE "2024-05-01T00:00:00Z"
    ```
  - Auditing/compliance (full account trail)
    ```sneldb
    REPLAY FOR account-42 SINCE "2024-01-01T00:00:00Z"
    ```
  - ML pipelines (rebuild a customer’s transaction sequence)
    ```sneldb
    REPLAY transaction FOR user-456 SINCE "2023-01-01T00:00:00Z"
    ```
  - Product journey (single user or session in order)
    ```sneldb
    REPLAY FOR user-123
    ```

## Command Cheatsheet

```sneldb
QUERY <event_type> [FOR <context_id>] [SINCE <ts>] [WHERE <expr>] [LIMIT <n>]
```

```sneldb
REPLAY [<event_type>] FOR <context_id> [SINCE <ts>]
```

More examples: [Query](../commands/query.md) and [Replay](../commands/replay.md)

## How It Works

### QUERY (step‑by‑step)

1. Parse and validate inputs.
2. Plan shard tasks (fan‑out unless narrowed by `context_id`).
3. Per shard, scan MemTable and pick relevant segments.
4. Prune zones by time and per‑field filters; read only needed columns.
   - Range predicates (`>`, `>=`, `<`, `<=`) are pruned using Zone SuRF (`{uid}_{field}.zsrf`) when present, falling back to XOR/EBM only if unavailable. SuRF is an order‑preserving trie using succinct arrays for fast range overlap checks.
   - Equality predicates (`=`, `IN`) use Zone XOR indexes (`{uid}_{field}.zxf`) for fast zone lookup.
   - Complex WHERE clauses with parentheses, AND/OR/NOT are transformed into a FilterGroup tree, and zones are combined using set operations (intersection for AND, union for OR, complement for NOT). See [Filter Architecture](./filter_architecture.md) for details.
5. Evaluate predicates and apply `WHERE` condition.
6. If aggregations are present:

   - Build an aggregation plan (ops, optional group_by, optional time bucket and selected time field).
   - In each shard, update aggregators from both MemTable (row path) and segments (columnar path). Segment scans project only needed columns (filters, group_by, time field, agg inputs).
   - Group keys combine optional time bucket with `group_by` values; a fast prehash accelerates hashmap grouping.
   - Merge partial aggregation states across shards; finalize into a table (bucket? + group columns + metric columns). `LIMIT` caps distinct groups.

   Otherwise (selection path):

   - Merge rows; apply global `LIMIT` if set.

### Sequence Queries (step‑by‑step)

Sequence queries (`FOLLOWED BY`, `PRECEDED BY`, `LINKED BY`) follow a specialized path optimized for finding ordered event pairs:

1. **Parse sequence**: Extract event types, link field, and sequence operator from the query.
2. **Parallel zone collection**: Collect zones for all event types in parallel across shards. Each event type gets its own query plan with transformed WHERE clauses (event-prefixed fields like `page_view.page` become `page` for the `page_view` plan).
3. **Index strategy assignment**: Assign index strategies to filter plans so zone XOR indexes are used for field filters.
4. **Zone hydration**: Load column values (including the `link_field`) without materializing events.
5. **Grouping**: Group row indices by `link_field` value using columnar data. Within each group, sort by timestamp.
6. **Matching**: Apply the two-pointer algorithm to find matching sequences:
   - For `FOLLOWED BY`: find events where `event_type_b` occurs at the same timestamp or later
   - For `PRECEDED BY`: find events where `event_type_b` occurred strictly before
   - Apply WHERE clause filters during matching to avoid materializing non-matching events
7. **Materialization**: Only materialize events from matched sequences, using `EventBuilder` and `PreparedAccessor` for efficient construction.

**Performance optimizations**:
- Columnar processing avoids premature event materialization
- Early filtering reduces the search space before grouping
- Parallel zone collection for different event types
- Index usage for `link_field` and `event_type` filters
- Limit short-circuiting stops processing once enough matches are found

### REPLAY (step‑by‑step)

1. Parse and validate inputs.
2. Route to the shard owning the `context_id`.
3. Scan MemTable and relevant segments for that context.
4. Apply optional `event_type` and `SINCE` filters.
5. Stream events in original append order.

See the diagram:

![Query and Replay flow](query.svg)

## What You Get

- **Visibility**: fresh writes are visible from `MemTable` before flush.
- **Ordering**: `REPLAY` preserves append order (single shard). `QUERY` has no global ordering unless you explicitly sort at merge (costly) or scope the query narrowly.
- **LIMIT** (`QUERY`): short‑circuit per shard when possible; always cap globally during merge.

## Performance Tips

- **Prune early**: favor `event_type`, `context_id`, and `SINCE` to skip zones fast.
- **Shard wisely**: more shards increase scan parallelism but cost more on fan‑out.

## Tuning

- `events_per_zone`: smaller zones = better pruning, more metadata; larger zones = fewer skips, less metadata.
- `flush_threshold`: affects how much is in memory vs on disk, and segment cadence.
- Shard count: match to CPU and expected concurrency.

## Invariants

- Immutability: events and segments are never edited in place.
- Single‑shard replay: each `context_id` maps to exactly one shard.
- Schema validity: stored payloads conform to their event type schema.
- Atomic publication: new segments become visible all‑or‑nothing.

## Further Reading

- [Read flow overview](./storage_engine.md)
- [Filter Architecture and Zone Collection](./filter_architecture.md)
- [Segments and zones](../architecture/segments_zones.md)

SnelDB’s read path is simple to reason about: prune aggressively, read only what you need, and merge efficiently—whether you’re slicing across many contexts or replaying one.
