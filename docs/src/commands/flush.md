# Flush

## Purpose

Force a memtable flush into an immutable segment.

## Form

```sneldb
FLUSH
```

## Notes

Useful for tests, checkpoints, or when you want on-disk segments immediately. Not required for correctness; ingestion continues during flush.
