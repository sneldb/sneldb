# Show

## Purpose

Stream the materialized results of a remembered query, replaying the stored snapshot and appending the latest delta in a single response.

## Form

```sneldb
SHOW <name>
```

- `<name>` must correspond to an existing materialization created with `REMEMBER QUERY … AS <name>`.

## Behavior

1. Loads the catalog entry and opens `materializations/<name>/`.
2. Streams previously stored frames into the response using the same column layout recorded at remember-time.
3. Builds an incremental query by appending `WHERE <time_field> > last_timestamp OR (<time_field> = last_timestamp AND event_id > last_event_id)`, where `<time_field>` defaults to `timestamp` unless the original query specified `USING <time_field>`.
4. Runs the incremental query through the streaming pipeline.
5. Forks each delta batch to the client response and to the materialized store, extending the snapshot on disk.
6. Updates the catalog with the new high-water mark, total rows/bytes, and last append deltas.
7. Logs a `sneldb::show` telemetry event summarizing counts, bytes, and watermark age.

## Output Format

`SHOW` reuses the streaming response format (schema header + row fragments) used by `QUERY` when streaming is enabled. Any client capable of consuming streaming query output can process a `SHOW` response without modification.

## Retention

If a retention policy (max rows / max age) is recorded in the catalog, the store will prune older frames after the delta append completes. Policies can be set programmatically via admin tooling; placeholders are stored for future command-level configuration.

## Errors

- Unknown materialization name.
- Stored schema missing or corrupted.
- Disk I/O failure while reading existing frames or appending delta batches.
- Incremental query fails (e.g., schema evolution removed required fields).

## Operational Notes

- The catalog (`materializations/catalog.mcat`) uses a per-entry file design for scalability. Deleting the catalog index removes all metadata; individual materializations can be dropped by removing both the directory and the catalog entry.
- High-water mark age is included in logs to help detect stale materializations that are not being refreshed.

## Further Reading

For details on how materialization works internally, see [Materialization](../design/materialization.md).
