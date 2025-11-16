use serde::Deserialize;

use crate::shared::datetime::time::TimeConfig;

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub wal: WalConfig,
    pub engine: EngineConfig,
    pub server: ServerConfig,
    pub logging: LoggingConfig,
    pub schema: SchemaConfig,
    pub playground: PlaygroundConfig,
    pub auth: Option<AuthConfig>,
    pub query: Option<QueryConfig>,
    pub time: Option<TimeConfig>,
}

#[derive(Debug, Deserialize)]
pub struct SchemaConfig {
    pub def_dir: String,
}

#[derive(Debug, Deserialize)]
pub struct WalConfig {
    pub enabled: bool,
    pub dir: String,
    pub fsync: bool,
    pub buffered: bool,
    pub buffer_size: usize,
    pub flush_each_write: bool,
    pub fsync_every_n: Option<usize>,
    // Conservative mode: archive WAL files instead of deleting
    pub conservative_mode: bool,
    pub archive_dir: String,
    pub compression_level: i32,
    pub compression_algorithm: String,
}

#[derive(Debug, Deserialize)]
pub struct EngineConfig {
    pub data_dir: String,
    pub shard_count: usize,
    pub index_dir: String,
    pub fill_factor: usize,
    pub event_per_zone: usize,
    pub compaction_interval: u64,
    pub sys_io_threshold: usize,
    /// Minimum available memory in MB required to run compaction (default: 512 MB)
    #[serde(default = "default_memory_threshold_mb")]
    pub sys_memory_threshold_mb: usize,
    pub max_inflight_passives: Option<usize>,
    /// Number of L0 segments to merge per compaction unit (k-way)
    pub segments_per_merge: usize,
    /// Max number of shards compacted concurrently (default 1 for serial across shards)
    pub compaction_max_shard_concurrency: usize,
    /// System info cache refresh interval in seconds (default 5)
    pub system_info_refresh_interval: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct QueryConfig {
    pub zone_index_cache_max_entries: Option<usize>,
    pub column_block_cache_max_bytes: Option<usize>,
    pub zone_surf_cache_max_bytes: Option<usize>,
    pub streaming_enabled: Option<bool>,
    /// Batch size for streaming JSON responses (0 = per-row, >0 = batched)
    /// Defaults to 1000 if not specified
    pub streaming_batch_size: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    pub socket_path: String,
    pub log_level: String,
    pub output_format: String,
    pub tcp_addr: String,
    pub http_addr: String,
    pub auth_token: String,
    /// Backpressure threshold: percentage of shard channel capacity before rejecting requests (0-100)
    #[serde(default = "default_backpressure_threshold")]
    pub backpressure_threshold: u8,
}

fn default_backpressure_threshold() -> u8 {
    80 // Default to 80% of channel capacity
}

#[derive(Debug, Deserialize)]
pub struct PlaygroundConfig {
    pub enabled: bool,
    pub allow_unauthenticated: bool,
}

#[derive(Debug, Deserialize)]
pub struct LoggingConfig {
    pub log_dir: String,
    pub stdout_level: String,
    pub file_level: String,
}

#[derive(Debug, Deserialize)]
pub struct AuthConfig {
    /// If true, bypass authentication for all commands (useful for development/testing)
    #[serde(default = "default_bypass_auth")]
    pub bypass_auth: bool,
    /// Initial admin user ID (only used if no users exist in database)
    pub initial_admin_user: Option<String>,
    /// Initial admin secret key (only used if no users exist in database)
    /// Can use environment variable expansion: ${SNELDB_ADMIN_KEY}
    pub initial_admin_key: Option<String>,
    /// Rate limit for authentication attempts (requests per second per IP address)
    ///
    /// IMPORTANT: This rate limit is ONLY applied to:
    /// - Initial authentication (AUTH command)
    /// - Per-request authentication (inline user_id:signature:command format)
    ///
    /// It is NOT applied to authenticated operations after connection-based auth.
    /// This allows authenticated TCP clients to send commands at full speed (300K+ events/sec)
    /// while still protecting against brute-force authentication attacks.
    ///
    /// Default: 10 auth attempts/second per IP address
    #[serde(default = "default_rate_limit_per_second")]
    pub rate_limit_per_second: u32,
    /// Enable per-IP rate limiting for authentication attempts
    /// Default: true (enabled for security)
    #[serde(default = "default_rate_limit_enabled")]
    pub rate_limit_enabled: bool,
}

fn default_bypass_auth() -> bool {
    false // Default to requiring authentication
}

fn default_rate_limit_per_second() -> u32 {
    10 // Default to 10 authentication attempts per second
}

fn default_rate_limit_enabled() -> bool {
    true // Default to rate limiting enabled
}

fn default_memory_threshold_mb() -> usize {
    512 // Default to 512 MB minimum available memory
}

use std::env;

pub fn load_settings() -> Result<Settings, config::ConfigError> {
    let config_path = env::var("SNELDB_CONFIG").unwrap_or_else(|_| "config".to_string());

    let settings: Settings = config::Config::builder()
        .add_source(config::File::with_name(&config_path))
        .build()?
        .try_deserialize()?;

    Ok(settings)
}
