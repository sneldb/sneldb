use serde::{Deserialize, Deserializer};
use std::fmt;

use crate::shared::datetime::time::TimeConfig;

/// Parse a human-readable size string (e.g., "256MB", "100KB", "1GB") or integer into bytes.
/// Supports: B, KB, MB, GB, TB (case-insensitive)
/// Also accepts plain integers for backward compatibility.
/// This function is public so it can be reused in test configs.
pub fn parse_size_bytes<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::{self, Visitor};

    struct SizeVisitor;

    impl<'de> Visitor<'de> for SizeVisitor {
        type Value = usize;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a size string (e.g., \"256MB\", \"100KB\") or an integer")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            parse_size_string(value).map_err(E::custom)
        }

        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            // Backward compatibility: accept raw integers as bytes
            Ok(value as usize)
        }

        fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            if value < 0 {
                return Err(E::custom("size cannot be negative"));
            }
            // Backward compatibility: accept raw integers as bytes
            Ok(value as usize)
        }
    }

    deserializer.deserialize_any(SizeVisitor)
}

/// Parse a human-readable size string into bytes.
/// Supports: B, KB, MB, GB, TB (case-insensitive, with or without space)
/// This function is public so it can be reused in test configs.
pub fn parse_size_string(s: &str) -> Result<usize, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty size string".to_string());
    }

    // Try to parse as plain number first (backward compatibility)
    if let Ok(bytes) = s.parse::<usize>() {
        return Ok(bytes);
    }

    // Parse number and unit
    let s_upper = s.to_uppercase();
    let (num_str, unit) = if s_upper.ends_with("B") {
        // Find where the number ends
        let mut num_end = s_upper.len() - 1;
        while num_end > 0
            && s_upper
                .chars()
                .nth(num_end - 1)
                .map_or(false, |c| c.is_alphabetic())
        {
            num_end -= 1;
        }
        let num_str = s[..num_end].trim();
        let unit = &s_upper[num_end..];
        (num_str, unit)
    } else {
        // No unit, treat as bytes
        return s
            .parse::<usize>()
            .map_err(|e| format!("invalid size: {}", e));
    };

    let num: f64 = num_str
        .parse()
        .map_err(|e| format!("invalid number in size string '{}': {}", s, e))?;

    let multiplier = match unit {
        "B" => 1,
        "KB" => 1024,
        "MB" => 1024 * 1024,
        "GB" => 1024 * 1024 * 1024,
        "TB" => 1024_u64 * 1024 * 1024 * 1024,
        _ => {
            return Err(format!(
                "unknown size unit '{}' in '{}'. Supported: B, KB, MB, GB, TB",
                unit, s
            ));
        }
    };

    let bytes = (num * multiplier as f64) as usize;
    Ok(bytes)
}

/// Parse a human-readable size string (e.g., "256MB", "100KB") or integer into megabytes.
/// For backward compatibility, also accepts integers which are treated as MB.
/// This function is public so it can be reused in test configs.
pub fn parse_size_mb<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::{self, Visitor};

    struct SizeVisitor;

    impl<'de> Visitor<'de> for SizeVisitor {
        type Value = usize;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str(
                "a size string (e.g., \"256MB\", \"100KB\") or an integer (treated as MB)",
            )
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            let bytes = parse_size_string(value).map_err(E::custom)?;
            Ok(bytes / (1024 * 1024)) // Convert bytes to MB
        }

        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            // Backward compatibility: accept raw integers as MB
            Ok(value as usize)
        }

        fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            if value < 0 {
                return Err(E::custom("size cannot be negative"));
            }
            // Backward compatibility: accept raw integers as MB
            Ok(value as usize)
        }
    }

    deserializer.deserialize_any(SizeVisitor)
}

/// Parse an optional human-readable size string or integer into bytes.
/// This function is public so it can be reused in test configs.
pub fn parse_optional_size_bytes<'de, D>(deserializer: D) -> Result<Option<usize>, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::{self, Visitor};

    struct OptionalSizeVisitor;

    impl<'de> Visitor<'de> for OptionalSizeVisitor {
        type Value = Option<usize>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("an optional size string (e.g., \"256MB\", \"100KB\") or integer")
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(None)
        }

        fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: Deserializer<'de>,
        {
            parse_size_bytes(deserializer).map(Some)
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            parse_size_string(value).map_err(E::custom).map(Some)
        }

        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(Some(value as usize))
        }

        fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            if value < 0 {
                return Err(E::custom("size cannot be negative"));
            }
            Ok(Some(value as usize))
        }
    }

    deserializer.deserialize_option(OptionalSizeVisitor)
}

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
    /// Buffer size in bytes. Can be specified as human-readable string (e.g., "8MB", "100KB") or integer (bytes).
    #[serde(deserialize_with = "parse_size_bytes")]
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
    /// Can be specified as human-readable string (e.g., "512MB", "256MB") or integer (MB).
    #[serde(
        default = "default_memory_threshold_mb",
        deserialize_with = "parse_size_mb"
    )]
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
    /// Column block cache size in bytes. Can be specified as human-readable string (e.g., "4GB", "256MB") or integer (bytes).
    #[serde(deserialize_with = "parse_optional_size_bytes")]
    pub column_block_cache_max_bytes: Option<usize>,
    /// Zone surf cache size in bytes. Can be specified as human-readable string (e.g., "4GB", "256MB") or integer (bytes).
    #[serde(deserialize_with = "parse_optional_size_bytes")]
    pub zone_surf_cache_max_bytes: Option<usize>,
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
    pub ws_addr: String,
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
    /// Session token expiration time in seconds
    /// Default: 300 seconds (5 minutes)
    #[serde(default = "default_session_token_expiry")]
    pub session_token_expiry_seconds: u64,
}

fn default_session_token_expiry() -> u64 {
    300 // Default to 5 minutes
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
