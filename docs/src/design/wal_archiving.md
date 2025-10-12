# WAL Archiving

## What it is

WAL archiving keeps copies of your write-ahead logs before they're deleted. Instead of throwing away the WAL after a flush, SnelDB can compress and store it as a binary archive—a compact, recoverable snapshot of every event that passed through.

Think of it as insurance: if you ever need to rebuild, audit, or replay events from before your oldest segment, the archives have you covered.

## Why it matters

WAL files normally disappear after a successful flush. That's fine for daily operations—the flushed segments contain everything you need. But sometimes you want more:

- **Disaster recovery**: Rebuild the database from archives if segments are lost.
- **Audit trails**: Keep a complete, timestamped history of every event for compliance.
- **Data migration**: Export and replay events into a new system.
- **Debugging**: Inspect the exact sequence of writes leading up to an issue.

WAL archiving turns "ephemeral durability" into "long-term safety" without slowing down the write path.

## How it works

When conservative mode is enabled, SnelDB archives WAL files before deleting them:

1. A flush completes successfully.
2. The cleaner identifies old WAL files (IDs below the new segment cutoff).
3. For each file:
   - Read the JSON lines (one event per line).
   - Serialize to MessagePack (compact binary format).
   - Compress with Zstandard (fast, high-ratio compression).
   - Write to `archive_dir/shard-N/wal-LOGID-START-END.wal.zst`.
4. Only if archiving succeeds, delete the original WAL file.

If archiving fails for any reason, the WAL files are preserved—safety first.

## Configuration

Add to your config file (e.g., `config/prod.toml`):

```toml
[wal]
enabled = true
dir = "../data/wal/"
# ... other WAL settings ...

# Conservative mode
conservative_mode = true
archive_dir = "../data/wal/archived/"
compression_level = 3
compression_algorithm = "zstd"
```

### Settings explained

- **conservative_mode**: Enables archiving. Set to `false` to delete WAL files immediately (default behavior).
- **archive_dir**: Where to store compressed archives. Organized by shard: `archive_dir/shard-0/`, `shard-1/`, etc.
- **compression_level**: Zstandard level (1–22). Level 3 is fast with excellent compression (~90–95% reduction). Level 19+ is slower but maximizes compression.
- **compression_algorithm**: Currently `"zstd"` (future: `"lz4"`, `"brotli"`).

## What gets archived

Each archive is a self-contained snapshot:

- **Header**: Metadata (version, shard ID, log ID, entry count, time range, compression details, creation timestamp).
- **Body**: All `WalEntry` records with event types, context IDs, timestamps, and payloads fully preserved.

File naming example:

```
wal-00042-1700000000-1700003600.wal.zst
     ^         ^           ^
     |         |           end timestamp
     |         start timestamp
     log ID (zero-padded to 5 digits)
```

This makes it easy to identify and sort archives by time.

## Compression performance

Typical results (MessagePack + Zstandard):

- Original JSON WAL: 1.0 GB
- Compressed archive: 50–100 MB (90–95% reduction)
- Compression speed: ~500 MB/s (level 3)
- Decompression speed: ~1000 MB/s

The write path remains fast because archiving happens after the flush completes—it's background work that doesn't block ingestion.

## CLI tool

SnelDB ships with `wal_archive_manager` for inspecting and managing archives:

```bash
# List all archives for shard 0
./wal_archive_manager list 0

# Show detailed info about an archive
./wal_archive_manager info archive.wal.zst

# Export to JSON for inspection or migration
./wal_archive_manager export archive.wal.zst output.json

# Recover all entries from archives
./wal_archive_manager recover 0

# Manually archive a specific WAL log
./wal_archive_manager archive 0 5
```

Example output:

```
$ ./wal_archive_manager list 0

Found 5 archive(s) for shard 0:

  wal-00001-1700000000-1700003600.wal.zst | Log 00001 | 1000 entries | 45.23 KB
  wal-00002-1700003600-1700007200.wal.zst | Log 00002 | 1500 entries | 67.89 KB
  ...

Total: 5000 entries across 5 archives (234.56 KB)
```

## Recovery

To recover events from archives:

```rust
use snel_db::engine::core::{WalArchive, WalArchiveRecovery};

let archive_dir = PathBuf::from("../data/wal/archived/shard-0");
let recovery = WalArchiveRecovery::new(0, archive_dir);

// List all archives
let archives = recovery.list_archives()?;

// Recover all entries in chronological order
let entries = recovery.recover_all()?;

// Or recover from a specific archive
let entries = recovery.recover_from_archive(&archives[0])?;
```

Every `WalEntry` comes back with full metadata:

- Event type (e.g., `"user_signup"`, `"purchase"`)
- Context ID
- Timestamp
- Complete payload

This makes it straightforward to replay, migrate, or audit.

## Archive retention

Archives accumulate over time. Plan your retention policy:

```bash
# Find archives older than 30 days
find ../data/wal/archived/ -name "*.wal.zst" -mtime +30

# Delete old archives (after backing up to S3, tape, etc.)
find ../data/wal/archived/ -name "*.wal.zst" -mtime +30 -delete
```

Or automate with a cron job:

```bash
# Backup to S3, then delete locally
0 2 * * * find /data/wal/archived -name "*.wal.zst" -mtime +30 | \
  while read f; do aws s3 cp "$f" s3://backups/wal-archives/ && rm "$f"; done
```

## Safety guarantees

- **No data loss**: If archiving fails, WAL files are not deleted.
- **Atomic operations**: Archives are written atomically; partial writes are not used.
- **Read-only recovery**: Recovery never modifies archives.
- **Format versioning**: Archives include a format version for future compatibility.

## When to enable it

Enable conservative mode if you need:

- Long-term event history beyond your oldest segment.
- Compliance or audit trails for financial, healthcare, or legal data.
- Disaster recovery beyond segment backups.
- Data export and migration capabilities.

Skip it if:

- Disk space is tight and segments alone are sufficient.
- Events have no long-term value beyond query windows.
- You have other backup/archival processes in place.

## Trade-offs

**Pros:**

- Complete event history for recovery or compliance.
- High compression (10–20× smaller than JSON).
- Background work—no impact on write latency.
- Easy to inspect, export, and replay.

**Cons:**

- Extra disk usage (though compressed archives are small).
- One more system to monitor and rotate.
- Not a replacement for segment backups—archives complement them.

## What this page is not

- A file format specification (see `wal_archive.rs` for internals).
- A backup strategy guide (archives are one tool; combine with segment snapshots and offsite copies).
- A performance tuning deep-dive (compression level 3 is a safe default; adjust only under load testing).

Conservative mode is about **peace of mind**: you decide how long to keep event history and SnelDB makes sure it's there when you need it.
