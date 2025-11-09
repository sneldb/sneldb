use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub wal: WalConfig,
    pub engine: EngineConfig,
    pub server: ServerConfig,
    pub logging: LoggingConfig,
    pub schema: SchemaConfig,
    pub playground: PlaygroundConfig,
    pub query: Option<QueryConfig>,
    pub time: Option<crate::shared::datetime::time::TimeConfig>,
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

use std::env;

pub fn load_settings() -> Result<Settings, config::ConfigError> {
    let config_path = env::var("SNELDB_CONFIG").unwrap_or_else(|_| "config".to_string());

    let settings: Settings = config::Config::builder()
        .add_source(config::File::with_name(&config_path))
        .build()?
        .try_deserialize()?;

    Ok(settings)
}
