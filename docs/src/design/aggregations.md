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

3. Execute per shard (streaming path):

   - MemTable events: streamed via `MemTableSource` → batches → `AggregateOp` → `AggregateSink`.
   - Segments: `SegmentQueryRunner` streams columnar batches → `AggregateOp` → `AggregateSink`.
   - Group key = (optional time bucket(ts, granularity, using time_field), ordered group_by values). A precomputed hash accelerates grouping.
   - Optional group limit prevents creating new groups beyond `LIMIT` but continues to update existing ones.
   - Each shard emits partial aggregate batches (intermediate schema with sum/count for AVG, JSON arrays for COUNT UNIQUE).

4. Merge and finalize:
   - `AggregateStreamMerger` collects partial aggregate batches from all shards.
   - Partial states are merged across shards per group key using `AggState::merge`.
   - Final table columns: optional `bucket`, group_by fields, then metric columns (e.g., `count`, `count_unique*<field>`, `total*<field>`, `avg*<field>`, `min*<field>`, `max*<field>`).
   - AVG aggregations preserve sum and count throughout the pipeline (as `avg_{field}_sum` and `avg_{field}_count` columns) and only finalize to an average at the coordinator, ensuring accurate merging across shards/segments.
   - COUNT UNIQUE aggregations preserve the actual unique values (as JSON array strings) throughout the pipeline and only finalize the count at the coordinator.
   - ORDER BY and LIMIT/OFFSET are applied at the coordinator after merging all shard results.

## Where to look in code

- Parse: `src/command/parser/commands/query.rs` (agg_clause, group_clause, time_clause)
- Plan: `src/engine/core/read/query_plan.rs`, `src/engine/core/read/aggregate/plan.rs`
- Projection: `src/engine/core/read/projection/strategies.rs` (AggregationProjection)
- Execution (streaming): `src/engine/core/read/flow/operators/aggregate.rs` (`AggregateOp`), `src/engine/core/read/flow/shard_pipeline.rs` (`build_segment_stream`)
- Sink (grouping): `src/engine/core/read/sink/aggregate/` (`sink.rs`, `group_key.rs`, `time_bucketing.rs`)
- Merge/finalize: `src/command/handlers/query/merge/aggregate_stream.rs` (`AggregateStreamMerger`)
- Result formatting: `src/engine/core/read/result.rs` (`AggregateResult::finalize`)

## Invariants & notes

- Time bucketing uses calendar-aware or naive bucketing (configurable) for stable edges.
- COUNT ALL-only queries still load a core column to determine zone sizes.
- LIMIT on aggregation limits group cardinality, not scanned events.
- Aggregate queries always use the streaming execution path for efficient processing and accurate merging across shards.
- AVG aggregations maintain sum and count separately during shard processing and merge these partial states accurately at the coordinator before finalizing to the average value.
- COUNT UNIQUE aggregations maintain the actual unique values (as JSON arrays) during shard processing and merge these sets accurately at the coordinator before finalizing to the count.
