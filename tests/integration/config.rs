use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs;

#[derive(Serialize, Deserialize)]
pub struct TestConfig {
    pub wal: WalConfig,
    pub engine: EngineConfig,
    pub server: ServerConfig,
    pub logging: LoggingConfig,
    pub schema: SchemaConfig,
    pub playground: PlaygroundConfig,
}

#[derive(Serialize, Deserialize)]
pub struct SchemaConfig {
    pub def_dir: String,
}

#[derive(Serialize, Deserialize)]
pub struct WalConfig {
    pub enabled: bool,
    pub dir: String,
    pub fsync: bool,
    pub buffered: bool,
    pub buffer_size: usize,
    pub flush_each_write: bool,
    pub fsync_every_n: Option<usize>,
}

#[derive(Serialize, Deserialize)]
pub struct EngineConfig {
    pub flush_threshold: usize,
    pub data_dir: String,
    pub shard_count: usize,
    pub index_dir: String,
    pub event_per_zone: usize,
    pub compaction_threshold: usize,
    pub compaction_interval: u64,
    pub sys_io_threshold: usize,
    pub max_inflight_passives: usize,
}

#[derive(Serialize, Deserialize)]
pub struct ServerConfig {
    pub socket_path: String,
    pub log_level: String,
    pub output_format: String,
    pub tcp_addr: String,
    pub http_addr: String,
    pub auth_token: String,
}

#[derive(Serialize, Deserialize)]
pub struct LoggingConfig {
    pub log_dir: String,
    pub stdout_level: String,
    pub file_level: String,
}

#[derive(Serialize, Deserialize)]
pub struct PlaygroundConfig {
    pub enabled: bool,
    pub allow_unauthenticated: bool,
}

/// Paths used by the test configuration of a single scenario.
struct TestPaths {
    base_dir: String,
    config_path: String,
    socket_path: String,
}

/// Compute filesystem paths used by a scenario's test config.
fn derive_paths(name: &str) -> TestPaths {
    let base_dir = format!("tests/integration/tmp/{}", name);
    TestPaths {
        config_path: format!("{}/config.test.toml", base_dir),
        socket_path: format!("/tmp/SNELDB_{}.sock", name),
        base_dir,
    }
}

/// Construct the default test configuration for a scenario, derived from its paths.
fn default_test_config(paths: &TestPaths) -> TestConfig {
    TestConfig {
        wal: WalConfig {
            enabled: true,
            fsync: false,
            buffered: true,
            buffer_size: 65536,
            dir: format!("{}/wal", paths.base_dir),
            flush_each_write: true,
            fsync_every_n: None,
        },
        engine: EngineConfig {
            flush_threshold: 2,
            data_dir: format!("{}/data", paths.base_dir),
            index_dir: format!("{}/index", paths.base_dir),
            shard_count: 2,
            event_per_zone: 8,
            compaction_threshold: 10,
            compaction_interval: 10,
            sys_io_threshold: 10,
            max_inflight_passives: 8,
        },
        server: ServerConfig {
            socket_path: paths.socket_path.clone(),
            log_level: "debug".into(),
            output_format: "json".into(),
            tcp_addr: "127.0.0.1:8080".into(),
            http_addr: "127.0.0.1:8080".into(),
            auth_token: "test".into(),
        },
        logging: LoggingConfig {
            log_dir: format!("{}/logs", paths.base_dir),
            stdout_level: "debug".into(),
            file_level: "debug".into(),
        },
        schema: SchemaConfig {
            def_dir: format!("{}/schema", paths.base_dir),
        },
        playground: PlaygroundConfig {
            enabled: true,
            allow_unauthenticated: true,
        },
    }
}

/// Recursively merge `overrides` into `base`.
///
/// - For objects: merge keys, recursively merging nested objects.
/// - For non-objects (arrays, scalars): replace `base` with `overrides`.
fn deep_merge_json(base: &mut Value, overrides: &Value) {
    match overrides {
        Value::Object(override_map) => {
            if let Value::Object(base_map) = base {
                for (key, override_value) in override_map.iter() {
                    match base_map.get_mut(key) {
                        Some(base_value) => deep_merge_json(base_value, override_value),
                        None => {
                            base_map.insert(key.clone(), override_value.clone());
                        }
                    }
                }
            } else {
                *base = overrides.clone();
            }
        }
        _ => {
            *base = overrides.clone();
        }
    }
}

pub fn write_config_for_with_overrides(
    name: &str,
    overrides: Option<&serde_json::Value>,
) -> (String, String) {
    let paths = derive_paths(name);
    let default = default_test_config(&paths);

    let final_config = if let Some(overrides_value) = overrides {
        let mut base_json = serde_json::to_value(&default).expect("config to json");
        deep_merge_json(&mut base_json, overrides_value);
        serde_json::from_value::<TestConfig>(base_json).expect("Invalid overrides for TestConfig")
    } else {
        default
    };

    fs::create_dir_all(&paths.base_dir).unwrap();
    let toml = toml::to_string_pretty(&final_config).unwrap();
    fs::write(&paths.config_path, toml).unwrap();

    (paths.config_path.clone(), paths.socket_path.clone())
}
