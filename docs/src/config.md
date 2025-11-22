# Configuration

## Overview

SnelDB is configured via a single TOML file. The path is provided by the `SNELDB_CONFIG` environment variable (default: `config`, which resolves `config.toml` or similar via the `config` crate). There is no automatic environment switching—you choose the file you want (for example `config/dev.toml`, `config/prod.toml`, or a custom path).

## Configuration File Structure

The configuration is organized into logical sections:

- `[wal]` - Write-Ahead Log settings
- `[engine]` - Storage engine and compaction settings
- `[schema]` - Schema definition storage
- `[server]` - Network and server settings
- `[playground]` - Web playground settings
- `[auth]` - Authentication and authorization
- `[logging]` - Logging configuration
- `[query]` - Query execution and caching
- `[time]` - Timezone and time bucketing

## Size Specifications

Size values (buffer sizes, cache limits, memory thresholds) can be specified in two formats:

1. **Human-readable strings** (recommended): Use units like `"100KB"`, `"256MB"`, `"4GB"`, `"1TB"`
2. **Raw integers** (backward compatible): Specify exact byte counts like `102400`, `268435456`

Supported units (case-insensitive):

- `B` - Bytes
- `KB` - Kilobytes (1024 bytes)
- `MB` - Megabytes (1024² bytes)
- `GB` - Gigabytes (1024³ bytes)
- `TB` - Terabytes (1024⁴ bytes)

Examples:

```toml
buffer_size = "100KB"              # Human-readable (recommended)
buffer_size = 102400               # Raw bytes (also works)
column_block_cache_max_bytes = "256MB"
sys_memory_threshold_mb = "512MB"
```

## Configuration Sections

`query`, `auth`, and `time` sections are optional; if omitted, sane defaults are applied where available.

### WAL (Write-Ahead Log)

Controls durability and write performance.

```toml
[wal]
enabled = true                     # Enable/disable WAL
fsync = true                       # Sync to disk on each write (slower, safer)
buffered = true                    # Use buffered I/O
buffer_size = "100KB"              # Buffer size for buffered writes
dir = "../data/wal/"               # WAL directory
flush_each_write = true            # Flush buffer after each write
fsync_every_n = 1024               # Sync every N writes (if fsync=true)
conservative_mode = true           # Archive WAL files before deletion
archive_dir = "../data/wal/archived/"
compression_level = 3              # Compression level (0-9)
compression_algorithm = "zstd"     # Compression algorithm
```

**Notes**:

- `fsync = true` ensures durability but reduces throughput
- `buffered = true` with `buffer_size` improves write performance
- `conservative_mode = true` preserves WAL files in archive for recovery
- Higher compression levels reduce disk usage but increase CPU

### Engine

Storage engine behavior, compaction, and resource management.

```toml
[engine]
fill_factor = 1                    # MemTable fill factor
data_dir = "../data/cols"          # Column data directory
index_dir = "../data/index/"       # Index directory
shard_count = 8                    # Number of shards
event_per_zone = 8000              # Events per zone before flush
compaction_interval = 600          # Compaction check interval (seconds)
sys_io_threshold = 100             # System I/O threshold
sys_memory_threshold_mb = "512MB"  # Minimum available memory for compaction
max_inflight_passives = 128        # Max concurrent passive shards
segments_per_merge = 8             # Segments to merge per compaction
compaction_max_shard_concurrency = 2  # Max shards compacted concurrently
system_info_refresh_interval = 30  # System info cache refresh (seconds) (default 5)
```

**Notes**:

- `shard_count` determines parallelism and data distribution
- `event_per_zone` controls when MemTables flush to disk
- `sys_memory_threshold_mb` prevents compaction when system memory is low
- `compaction_max_shard_concurrency` limits compaction parallelism
- `system_info_refresh_interval` defaults to 5 seconds if omitted
- `sys_memory_threshold_mb` treats integer literals as MB (not bytes) when used without a unit

### Schema

Schema definition storage location.

```toml
[schema]
def_dir = "../data/schema/"
```

### Server

Network endpoints and server behavior.

```toml
[server]
socket_path = "/tmp/sneldb.sock"   # Unix socket path
log_level = "error"                # Log level: trace, debug, info, warn, error
output_format = "json"             # Default output: json, arrow, text
tcp_addr = "127.0.0.1:7171"        # TCP server address
http_addr = "127.0.0.1:8085"       # HTTP server address
ws_addr = "127.0.0.1:8086"         # WebSocket server address
auth_token = "mysecrettoken"       # Bearer token for authentication
backpressure_threshold = 90        # Backpressure threshold (0-100%)
```

**Notes**:

- `output_format` can be `json`, `arrow`, or `text`
- `backpressure_threshold` controls when to reject requests (percentage of channel capacity)
- Default `backpressure_threshold` is 80 if not set

### Playground

Web-based interactive interface.

```toml
[playground]
enabled = true                     # Enable/disable playground
allow_unauthenticated = true       # Allow access without authentication
```

**Notes**:

- Playground is available at `http://<http_addr>/` when enabled
- Set `allow_unauthenticated = false` to require authentication

### Auth

Authentication and rate limiting.

```toml
[auth]
bypass_auth = false                # Bypass authentication (dev/testing only)
initial_admin_user = "admin"       # Initial admin user ID
initial_admin_key = "admin-key-123"  # Initial admin secret key
rate_limit_per_second = 10         # Rate limit for failed auth attempts
rate_limit_enabled = true          # Enable rate limiting
session_token_expiry_seconds = 300 # Session token expiration (seconds)
```

**Notes**:

- Rate limiting applies only to **failed** authentication attempts
- Successful authentications bypass rate limiting for high throughput
- `bypass_auth = true` disables all authentication (use only in development)
- Defaults: `bypass_auth = false`, `rate_limit_per_second = 10`, `rate_limit_enabled = true`, `session_token_expiry_seconds = 300`

### Logging

Log output configuration.

```toml
[logging]
log_dir = "../data/logs"           # Log file directory
stdout_level = "debug"              # Console log level
file_level = "error"                # File log level
```

**Notes**:

- Separate levels for console and file output
- Logs are written to files in `log_dir`

### Query

Query execution and caching.

```toml
[query]
zone_index_cache_max_entries = 1024              # Zone index cache entries
column_block_cache_max_bytes = "256MB"           # Column block cache size
zone_surf_cache_max_bytes = "100MB"              # Zone surf cache size
streaming_batch_size = 1000                      # Streaming batch size (0 = per-row)
```

**Notes**:

- Caches improve query performance by reducing disk I/O
- Larger caches use more memory but improve hit rates
- `streaming_batch_size = 0` streams one row at a time
- `streaming_batch_size` defaults to 1000 if omitted

### Time

Timezone and time bucketing configuration.

```toml
[time]
timezone = "UTC"                   # Default timezone
week_start = "Mon"                 # Week start day
use_calendar_bucketing = true      # Use calendar-based bucketing
```

## Environment-Specific Configs

### Development (`config/dev.toml`)

Optimized for development with:

- Smaller cache sizes
- More verbose logging
- Playground enabled
- Conservative WAL settings

### Production (`config/prod.toml`)

Optimized for production with:

- Larger cache sizes (4GB+)
- Minimal logging
- Higher concurrency limits
- Optimized WAL settings

### Testing (`config/test.toml`)

Optimized for tests with:

- Minimal resource usage
- Fast cleanup
- Authentication bypassed
- Small cache sizes

## Loading Configuration

Configuration is loaded at startup:

```bash
# Use default config (config/dev.toml, config/prod.toml, etc.)
cargo run

# Specify custom config file
SNELDB_CONFIG=config/custom.toml cargo run

# Or set environment variable
export SNELDB_CONFIG=config/prod.toml
cargo run
```

The system looks for the config file relative to the current working directory or as an absolute path.

## Validation

Invalid configuration values will cause SnelDB to fail at startup with a clear error message. Common issues:

- Invalid size format (use `"256MB"` not `256MB` without quotes)
- Missing required directories
- Invalid network addresses
- Conflicting settings (e.g., `fsync = false` with `flush_each_write = true`)

## Best Practices

1. **Use human-readable sizes**: Prefer `"256MB"` over `268435456` for readability
2. **Environment-specific configs**: Maintain separate configs for dev, test, and prod
3. **Monitor resource usage**: Adjust cache sizes based on available memory
4. **WAL settings**: Balance durability (`fsync`) with performance (`buffered`)
5. **Shard count**: Set `shard_count` based on CPU cores and workload parallelism
6. **Compaction tuning**: Adjust `compaction_interval` and `compaction_max_shard_concurrency` based on write patterns

## Further Reading

- [Storage Engine](./design/storage_engine.md) - How the engine uses these settings
- [Compaction](./design/compaction.md) - Compaction behavior and tuning
- [Infrastructure](./design/infrastructure.md) - System resource management
