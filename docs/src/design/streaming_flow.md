# Streaming Query Flow

## What this page is

- A walk-through of the new streaming read path, from command dispatch to shard merges.
- Enough detail to map runtime behavior to the coordinating and shard-side code.

## When we stream

- Only plain `QUERY` commands without aggregations, grouping, event sequences, or time buckets.
- Triggered by the HTTP/TCP `QUERY` handler when the caller asks for streaming (e.g. client-side backpressure, large scans where batching the whole result is impractical).
- Falls back to the existing batch pipeline when the command shape or planner outcome cannot be streamed.

## Pipeline overview

1. **Coordinator** (`QueryExecutionPipeline::execute_streaming`)

   - Planner builds a `PlanOutcome` exactly like the batch path (zone picks, per-shard filters, limits).
   - `StreamingShardDispatcher` fans out a `ShardMessage::QueryStream` to every shard, bundling the plan fragments.

2. **Shard execution** (`scan_streaming` → `StreamingScan`)

   - Each shard rebuilds a `QueryPlan`, then initializes a `StreamingContext` (plan, passive snapshot, caches, `FlowContext`, effective limit = `limit + offset`).
   - `FlowBuilders` produces up to two flows:
     - `memtable_flow` wraps the active memtable plus passive buffers via `MemTableQueryRunner::stream`.
     - `segment_flow` calls `build_segment_stream` to launch a background `SegmentQueryRunner` streaming columnar batches.
   - `ShardFlowMerger` fuses those flows. If the command carries an `ORDER BY`, it spawns an ordered heap merge; otherwise, it fan-ins the channels. The result is a `ShardFlowHandle` (receiver + schema + background tasks).

3. **Coordinator merge & delivery**

   - The dispatcher hands the `ShardFlowHandle`s to the merge layer (`StreamMergerKind`).
   - `OrderedStreamMerger` uses the flow-level ordered merger to respect `ORDER BY field [DESC]`, honouring `LIMIT/OFFSET` at the coordinator.
   - `UnorderedStreamMerger` forwards batches as they arrive when no ordering is requested.
   - `QueryBatchStream` wraps the merged receiver. Dropping it aborts all shard/background tasks to avoid leaks.

## Where to look in code

- Coordinator entry: `src/command/handlers/mod.rs`, `query/orchestrator.rs` (`execute_streaming`).
- Dispatch: `src/command/handlers/query/dispatch/streaming.rs`.
- Merge: `src/command/handlers/query/merge/streaming.rs`, `query_batch_stream.rs`.
- Shard message + worker: `src/engine/shard/message.rs`, `src/engine/shard/worker.rs`.
- Shard read pipeline: `src/engine/query/streaming/{scan.rs,context.rs,builders.rs,merger.rs}`.
- Flow primitives (channels, batches, ordered merge): `src/engine/core/read/flow/` (notably `context.rs`, `channel.rs`, `ordered_merger.rs`, `shard_pipeline.rs`).

## Operational notes

- Aggregations and grouped queries are intentionally excluded because they require per-shard stateful sinks and finalize logic (tracked separately).
- `StreamingContext` snapshots passive buffers at creation; long-lived streams do not see newer passive flushes until a new stream is opened.
- Flow channels are bounded (default 32k rows per batch) to provide natural backpressure; coordinator-side consumers should `recv` promptly.
- If any shard fails while constructing the stream, the dispatcher surfaces a shard-specific error and aborts the entire streaming request.
