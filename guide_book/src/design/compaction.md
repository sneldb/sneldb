# Compaction

## What it is

Compaction keeps reads predictable as data grows. Instead of editing files in place, SnelDB periodically merges small, freshly-flushed segments into larger, cleaner ones. This reduces file count, tightens zone metadata, and improves pruning—without touching the logical history of events.

## Why it matters

- Fewer segments → fewer seeks and better cache behavior.
- Larger, well-formed zones → more “skip work” during queries.
- Stable tail latencies as ingestion continues day after day.

## How it runs (big picture)

- One background task per shard.
- Wakes up at a configurable interval.
- Checks disk/IO pressure; if the system is busy, skips this round.
- Looks at the shard’s segment index to decide if compaction is needed.
- When needed, launches a compaction worker to perform the merge and publish new segments atomically.

## Shard-local by design

Each shard compacts its own segments. This keeps the work isolated, prevents cross-shard coordination, and preserves the “all events for a context live together” property.

## When it triggers

- On a fixed timer (compaction_interval).
- Only if the segment index reports that thresholds are met (e.g., too many L0s, size/age criteria).
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

- Selects a compaction set (often recent small segments).
- Merges column files in order, rebuilding zones, filters, and indexes.
- Emits a new, leveled segment and updates the segment index.
- Schedules old segments for deletion/GC.

## Operator knobs

- compaction_interval: how often to check.
- compaction_threshold: when the segment index should say “yes, compact.”
- sys_io_threshold (and related IO heuristics): how conservative to be under load.
- events_per_zone and flush_threshold: influence zone granularity and L0 creation rate (tune together).

## Invariants

- No in-place mutation; only append/replace at the segment set level.
- Queries stay available and correct while compaction runs.
- Failures are contained to the background task; foreground paths remain healthy.

## What this page is not

- A file-format spec or merge algorithm walkthrough.
- A policy recipe for every workload. The defaults aim for good general behavior; heavy write or read-mostly deployments may tune the thresholds differently.
