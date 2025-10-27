# Aggregations

## What this page is

- A focused description of how aggregation queries are parsed, planned, and executed.
- Enough detail to find code and reason about behavior without reading internals first.

## Command surface

- Metrics: `COUNT`, `COUNT UNIQUE <field>`, `COUNT <field>`, `TOTAL <field>`, `AVG <field>`, `MIN <field>`, `MAX <field>`
- Grouping: `BY <field> [, <field> ...]`
- Time bucketing: `PER HOUR|DAY|WEEK|MONTH [USING <time_field>]`
- Time selection: `USING <time_field>` (also affects SINCE and pruning)
- Limit groups: `LIMIT <n>` caps distinct groups emitted

See: Commands → Query for examples.

## Flow (step‑by‑step)

1. Parse: PEG grammar recognizes aggregation specs, grouping, and optional time bucketing/USING.

2. Plan: `AggregatePlan` captures ops, `group_by`, `time_bucket`.

   - If aggregating, the implicit SINCE filter may be removed from filter plans to avoid truncating buckets; explicit SINCE remains honored.
   - Projection strategy adds: non-core filter columns, `group_by` fields, selected time field for bucketing, and input fields for requested metrics.

3. Execute per shard:

   - MemTable events: feed directly into `AggregateSink` (row path).
   - Segments: hydrate only needed columns; evaluate predicates; for each matching row, update sink (columnar path).
   - Group key = (optional time bucket(ts, granularity, using time_field), ordered group_by values). A precomputed hash accelerates grouping.
   - Optional group limit prevents creating new groups beyond `LIMIT` but continues to update existing ones.

4. Merge and finalize:
   - Partial states are merged across shards per key.
   - Final table columns: optional `bucket`, group*by fields, then metric columns (e.g., `count`, `count_unique*<field>`, `total*<field>`, `avg*<field>`, `min*<field>`, `max*<field>`).

## Where to look in code

- Parse: `src/command/parser/commands/query.rs` (agg_clause, group_clause, time_clause)
- Plan: `src/engine/core/read/query_plan.rs`, `src/engine/core/read/aggregate/plan.rs`
- Projection: `src/engine/core/read/projection/strategies.rs` (AggregationProjection)
- Execution: `src/engine/query/execution_engine.rs` (run_aggregation)
- Sink (grouping): `src/engine/core/read/sink/aggregate/` (`sink.rs`, `group_key.rs`, `time_bucketing.rs`)
- Merge/finalize: `src/engine/core/read/result.rs`

## Invariants & notes

- Time bucketing uses calendar-aware or naive bucketing (configurable) for stable edges.
- COUNT ALL-only queries still load a core column to determine zone sizes.
- LIMIT on aggregation limits group cardinality, not scanned events.
