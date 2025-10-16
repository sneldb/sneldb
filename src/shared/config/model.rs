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
    pub flush_threshold: usize,
    pub data_dir: String,
    pub shard_count: usize,
    pub index_dir: String,
    #[serde(default)]
    pub fill_factor: usize,
    pub event_per_zone: usize,
    pub compaction_threshold: usize,
    pub compaction_interval: u64,
    pub sys_io_threshold: usize,
    pub max_inflight_passives: Option<usize>,
}

// Removed fill_factor() method; `fill_factor` is now provided via config and
// `flush_threshold` is derived from it during settings load when present.

#[derive(Debug, Deserialize)]
pub struct QueryConfig {
    pub zone_index_cache_max_entries: Option<usize>,
    pub column_block_cache_max_bytes: Option<usize>,
    pub zone_surf_cache_max_bytes: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    pub socket_path: String,
    pub log_level: String,
    pub output_format: String,
    pub tcp_addr: String,
    pub http_addr: String,
    pub auth_token: String,
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

    let mut settings: Settings = config::Config::builder()
        .add_source(config::File::with_name(&config_path))
        .build()?
        .try_deserialize()?;

    // If fill_factor is provided (non-zero), derive flush_threshold from it.
    // Otherwise, compute fill_factor from existing flush_threshold for backward compatibility.
    if settings.engine.fill_factor > 0 {
        settings.engine.flush_threshold =
            settings.engine.fill_factor * settings.engine.event_per_zone;
    } else {
        settings.engine.fill_factor =
            (settings.engine.flush_threshold + settings.engine.event_per_zone - 1)
                / settings.engine.event_per_zone;
    }

    Ok(settings)
}
