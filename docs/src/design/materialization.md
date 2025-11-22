# Materialization

## What it is

Materialization lets you "remember" query results so future reads can replay the stored snapshot without re-scanning the cluster. You create a materialization with `REMEMBER QUERY`, then stream it (with incremental updates) using `SHOW`.

## Why it exists

Running the same analytical query repeatedly is wasteful: you scan the same segments, apply the same filters, and project the same columns. Materialization captures the result once, stores it durably, and incrementally refreshes it on demand. This is useful for:

- **Dashboards**: Pre-compute expensive aggregations or filters
- **Reporting**: Snapshot query results for consistent reports
- **Caching**: Store frequently-accessed query results
- **Incremental processing**: Only process new data since last materialization

## Core pieces

- **Catalog**: Lightweight index mapping materialization names to entry files
- **Entry**: Per-materialization metadata (query spec, schema, high-water mark, row/byte counts)
- **Frames**: Immutable, compressed columnar batches stored on disk
- **Manifest**: Per-materialization list of all frames with metadata
- **High-water mark**: `(timestamp, event_id)` tuple tracking what's already materialized
- **Retention policy**: Optional limits on total rows or frame age

## How it works

### Creating a materialization (REMEMBER)

1. Execute the query through the streaming pipeline
2. Capture the output schema as `SchemaSnapshot` objects
3. Open a `MaterializedStore` at `materializations/<name>/`
4. Stream batches through a `MaterializedSink`:
   - Encode batches using LZ4 compression
   - Write frames to `frames/NNNNNN.mat` files
   - Maintain a `manifest.bin` tracking all frames
   - Record high-water mark (max timestamp + event_id)
5. Register in the catalog with query spec, schema, and metadata

### Querying a materialization (SHOW)

1. Load the catalog entry for the materialization
2. Stream all existing frames from disk to the client
3. Build an incremental query by bumping `since` to the stored high-water timestamp (if newer than any existing `since`)
4. Execute the incremental query through the streaming pipeline; delta batches are deduped against the stored `(timestamp, event_id)` watermark before being returned or appended
5. Fork delta results to both:
   - Client response (streaming)
   - Materialized store (new frames appended)
6. Update catalog with new high-water mark and row/byte counts
7. Apply retention policy (if set) to prune old frames

## Storage layout

```
materializations/
├── catalog.mcat              # Lightweight index (name → entry path)
├── <name1>/
│   ├── entry.mcatentry      # Full entry (query spec, schema, metadata)
│   ├── manifest.bin          # Frame manifest (list of all frames)
│   └── frames/
│       ├── 000000.mat       # Frame 0 (LZ4 compressed)
│       ├── 000001.mat       # Frame 1
│       └── ...
└── <name2>/
    └── ...
```

### Frame format

Each `.mat` file contains:

- **Binary header**: Magic number, version, flags
- **Frame header**: Schema hash, row/column counts, timestamp range, max event_id, compression metadata, CRC32 checksum
- **Compressed payload**: LZ4-compressed columnar data (null bitmap + typed values)

Frames are immutable and append-only. Once written, they're never modified.

### Catalog system

The catalog uses a per-entry file design for scalability:

- **Index file** (`catalog.mcat`): ~100 bytes per entry, maps names to entry paths
- **Entry files** (`<name>/entry.mcatentry`): ~2KB per entry, full metadata
- **Global cache**: LRU cache for both index and entries with singleflight pattern

This design provides O(1) index load (small file, cached) and O(1) entry load (on-demand, cached), scaling to thousands of materializations efficiently.

## Incremental updates

The high-water mark enables efficient incremental updates:

- **First SHOW**: Streams all existing frames, then queries for new data
- **Subsequent SHOW**: Issues a delta query with `since` set to the stored high-water timestamp
- **Delta dedupe**: Delta batches are filtered against the stored `(timestamp, event_id)` watermark before being streamed or appended
- **Automatic append**: New data is appended to frames and high-water mark advances

This means materializations stay fresh with minimal overhead: you only process new data since the last update.

## Retention policies

Materializations can optionally enforce retention policies to limit growth:

- **Max rows**: Keep only the most recent N rows (prunes oldest frames first)
- **Max age**: Keep only frames newer than N seconds (prunes by timestamp)

Retention is enforced after each delta append. Policies are stored in the catalog and applied automatically.

## Caching

Two cache layers optimize materialization performance:

### Catalog cache

- **Index cache**: 10 entries (small, frequently accessed)
- **Entry cache**: 1,000 entries
- **Singleflight**: Prevents duplicate loads during concurrent access
- **Performance**: ~0.1ms for cached operations vs ~7-15ms for uncached

### Frame cache

- **Capacity**: 512 MiB (default, configurable)
- **Max items**: 10,000 frames
- **Eviction**: LRU with size-based limits
- **Zero-copy**: Returns `Arc<ColumnBatch>` for cache hits (no decompression needed)

## Schema evolution

Materializations are tied to a specific schema. If the source schema evolves:

- Schema hash is checked on every append
- If schema hash changes, append fails with an error
- Materialization must be recreated to pick up schema changes

This is intentional: materializations represent a snapshot of the query at a point in time. Schema changes require explicit recreation to ensure correctness.

## Performance characteristics

### Catalog operations

- **Load index**: ~0.1ms (cached), scales to 10K+ entries
- **Load entry**: ~0.1ms (cached), on-demand loading
- **Update entry**: ~2-5ms, only updates changed entry
- **List names**: ~0.1ms (cached), index-only operation

### Frame operations

- **Load manifest**: ~0.5ms (1K frames), ~2-5ms (10K frames), ~20-50ms (100K frames)
- **Read frame (cached)**: ~0.01ms, zero-copy
- **Read frame (miss)**: ~1-5ms, disk I/O + decompression
- **Append frame**: ~5-10ms, encode + write + manifest update
- **Retention scan**: O(n) scan of all frames, ~1-2ms (10K frames)

### SHOW command

- **Small materialization** (100 frames, no delta): ~10-50ms
- **Medium materialization** (1K frames, small delta): ~50-200ms
- **Large materialization** (10K frames, medium delta): ~200-1000ms

Performance depends on:

- Number of existing frames (streaming overhead)
- Size of delta query (new data to process)
- Cache hit rates (cached frames are much faster)

## Operational notes

### Creating materializations

- First run performs a full scan; ensure sufficient disk space
- Any streaming-compatible query can be materialized (selection, aggregates, ORDER BY, sequences) as long as it runs through the streaming pipeline
- Materializations are shard-agnostic; they capture results across all shards

### Updating materializations

- `SHOW` automatically updates the materialization with new data
- High-water mark ensures no duplicate data
- Delta queries are efficient (only scan new data)
- Multiple concurrent `SHOW` operations on the same materialization are not protected (may cause duplicate appends)

### Managing materializations

- Catalog is stored at `materializations/catalog.mcat`
- Individual materializations can be removed by deleting the directory
- Catalog corruption can be recovered by scanning directories
- Retention policies prevent unbounded growth

### Monitoring

- Logs include materialization alias, row counts, byte counts, and high-water mark age
- High-water mark age helps detect stale materializations
- Cache statistics available via telemetry (hits, misses, evictions)

## Limitations

### Manifest scalability

Each materialization's manifest grows linearly with frame count:

- **10K frames**: ~2-4MB manifest, ~2-5ms load
- **100K frames**: ~20-40MB manifest, ~20-50ms load
- **1M frames**: ~200-400MB manifest, ~200-500ms load

For very large materializations (>100K frames), consider:

- Using retention policies to limit frame count
- Periodic recreation to reset frame count
- Future: manifest sharding or index-based manifests

### Frame count growth

Each delta query creates new frames. Small, frequent deltas lead to many frames:

- More filesystem operations (one file per frame)
- Larger manifest files
- More cache pressure

Mitigations:

- Use retention policies
- Batch small deltas (future: frame compaction)

### Concurrent SHOW operations

Multiple concurrent `SHOW` operations on the same materialization:

- May cause duplicate delta queries
- May cause duplicate frame appends
- Manifest updates are not atomic across concurrent operations

Best practice: Avoid concurrent `SHOW` on the same materialization, or implement application-level locking.

## Further reading

- Command reference: [Remember](../commands/remember.md), [Show](../commands/show.md)
- Storage engine details: [Storage Engine](./storage_engine.md)
- Query execution: [Query and Replay](./query_replay.md)
- Streaming pipeline: [Streaming Query Flow](./streaming_flow.md)

Materialization bridges the gap between ad-hoc queries and pre-computed results: you get the freshness of queries with the performance of cached data.
