# Compaction

## What it is

Compaction keeps reads predictable as data grows. Instead of editing files in place, SnelDB periodically merges small, freshly-flushed segments into larger, cleaner ones. This reduces file count, tightens zone metadata, and improves pruning—without touching the logical history of events.

## Why it matters

- Fewer segments → fewer seeks and better cache behavior.
- Larger, well-formed zones → more “skip work” during queries.
- Stable tail latencies as ingestion continues day after day.

## How it runs (big picture)

- One background task per process coordinates compaction across shards with a global concurrency of 1 (configurable). Shards are compacted one at a time; within a shard, work runs serially.
- Periodically checks system IO pressure; if the system is busy, it skips.
- Uses a policy to plan compaction (k-way by uid); if the policy yields plans, a worker runs them and publishes new segments atomically.

## Shard-local by design

Each shard compacts its own segments. This keeps the work isolated, prevents cross-shard coordination, and preserves the “all events for a context live together” property.

## When it triggers

- Only if the k-way policy finds any merge plans for the shard (no threshold counter anymore).
- Skips entirely if IO pressure is high to avoid hurting foreground work.

## Safety & correctness

- Segments are immutable; compaction writes new files and then swaps pointers in one step.
- If a run fails, nothing is partially applied; the old segments remain authoritative.
- Reads continue throughout—queries see either the old set or the new set, never a half state.
- Replay order and event immutability are unaffected.

## Resource awareness

- The loop samples system state (disks/IO) before running.
- Under pressure, the compactor yields to ingestion and queries.
- This protects P99 read latencies and avoids “compaction storms.”

## What the worker does (conceptually)

- For each uid, groups L0 segments into chunks of size `k` (config).
- For each full chunk, performs a k-way merge of events sorted by `context_id`.
- Rebuilds zones at a level-aware target size: `events_per_zone * fill_factor * (level+1)`.
- Emits a new L1 (or higher-level) segment with correct naming, updates the segment index, and removes inputs from the index.

## Operator knobs

- `segments_per_merge`: number of L0 segments to merge per output.
- `compaction_max_shard_concurrency`: max shards compacted simultaneously (default 1 = serial across shards).
- `sys_io_threshold` (and related IO heuristics): how conservative to be under load.
- `events_per_zone` and `fill_factor`: base and multiplier for zone sizing; higher levels multiply by `(level+1)`.

## Invariants

- No in-place mutation; only append/replace at the segment set level.
- Queries stay available and correct while compaction runs.
- Failures are contained to the background task; foreground paths remain healthy.

## What this page is not

- A file-format spec or merge algorithm walkthrough.
- A policy recipe for every workload. The defaults aim for good general behavior; heavy write or read-mostly deployments may tune the thresholds differently.
