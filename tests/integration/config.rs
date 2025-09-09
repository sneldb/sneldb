use serde::Serialize;
use std::fs;

#[derive(Serialize)]
pub struct TestConfig {
    pub wal: WalConfig,
    pub engine: EngineConfig,
    pub server: ServerConfig,
    pub logging: LoggingConfig,
    pub schema: SchemaConfig,
}

#[derive(Serialize)]
pub struct SchemaConfig {
    pub def_dir: String,
}

#[derive(Serialize)]
pub struct WalConfig {
    pub enabled: bool,
    pub dir: String,
    pub fsync: bool,
    pub buffered: bool,
    pub buffer_size: usize,
    pub flush_each_write: bool,
    pub fsync_every_n: Option<usize>,
}

#[derive(Serialize)]
pub struct EngineConfig {
    pub flush_threshold: usize,
    pub data_dir: String,
    pub shard_count: usize,
    pub index_dir: String,
    pub event_per_zone: usize,
    pub compaction_threshold: usize,
    pub compaction_interval: u64,
    pub sys_io_threshold: usize,
}

#[derive(Serialize)]
pub struct ServerConfig {
    pub socket_path: String,
    pub log_level: String,
    pub output_format: String,
    pub tcp_addr: String,
    pub http_addr: String,
    pub auth_token: String,
}

#[derive(Serialize)]
pub struct LoggingConfig {
    pub log_dir: String,
    pub stdout_level: String,
    pub file_level: String,
}

pub fn write_config_for(name: &str) -> (String, String) {
    let socket_path = format!("/tmp/SNELDB_{}.sock", name);
    let base_dir = format!("tests/integration/tmp/{}", name);
    let config_path = format!("{}/config.test.toml", base_dir);

    let config = TestConfig {
        wal: WalConfig {
            enabled: true,
            fsync: false,
            buffered: true,
            buffer_size: 65536,
            dir: format!("{}/wal", base_dir),
            flush_each_write: true,
            fsync_every_n: None,
        },
        engine: EngineConfig {
            flush_threshold: 6,
            data_dir: format!("{}/data", base_dir),
            index_dir: format!("{}/index", base_dir),
            shard_count: 2,
            event_per_zone: 8,
            compaction_threshold: 10,
            compaction_interval: 10,
            sys_io_threshold: 10,
        },
        server: ServerConfig {
            socket_path: socket_path.clone(),
            log_level: "debug".into(),
            output_format: "json".into(),
            tcp_addr: "127.0.0.1:8080".into(),
            http_addr: "127.0.0.1:8080".into(),
            auth_token: "test".into(),
        },
        logging: LoggingConfig {
            log_dir: format!("{}/logs", base_dir),
            stdout_level: "debug".into(),
            file_level: "debug".into(),
        },
        schema: SchemaConfig {
            def_dir: format!("{}/schema", base_dir),
        },
    };

    fs::create_dir_all(&base_dir).unwrap();
    let toml = toml::to_string_pretty(&config).unwrap();
    fs::write(&config_path, toml).unwrap();

    (config_path, socket_path)
}
