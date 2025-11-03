# Remember

## Purpose

Materialize the results of a streaming-compatible `QUERY` under a durable alias so future readers can replay the stored snapshot without re-scanning the cluster.

## Form

```sneldb
REMEMBER QUERY <query-expr> AS <name>
```

- `<query-expr>` must be a plain `QUERY` command that already works at the shell prompt.
- `<name>` becomes the filename-friendly alias used under `materializations/<name>/`.

## Constraints

- Aliases may contain ASCII letters, digits, `_`, and `-` only.
- Only selection queries without aggregates, grouping, or event sequences can be remembered (the same restriction as streaming queries).
- The first run performs a full scan; ensure the backend has enough disk for the snapshot.

## Behavior

1. The query plan is compiled and executed once through the streaming pipeline.
2. Batches are persisted to `materializations/<name>/frames/NNNNNN.mat` using the same column ordering and types as the live query.
3. A catalog entry (`materializations/catalog.bin`) records:
   - Canonical query hash and serialized query spec.
   - Stored schema snapshot.
   - Current row and byte totals.
   - High-water mark (timestamp + event_id) used for future incremental refreshes.
   - Last append deltas (rows/bytes) and timestamps.
   - Optional retention policy placeholder (future feature).
4. A short summary (rows stored, bytes, watermark age) is returned to the caller.

## Retention

Each remembered query can optionally track a retention policy (max rows or max age). Policies are recorded in the catalog for future use; the current implementation records the fields and prunes frames when they are set programmatically.

## Diagnostics & Telemetry

- Successful runs log a `sneldb::remember` event with alias, total rows, rows appended, and watermark details.
- You can inspect `materializations/catalog.bin` (bincode + JSON-encoded spec) to review metadata, or issue `SHOW <name>` to fetch both the stored snapshot and the latest delta.

## Errors

- Alias already exists.
- Query is not streaming-compatible.
- Engine is unable to write the materialization directory (disk full / permission).
- Catalog persistence failure (corrupted header or serialization error).
