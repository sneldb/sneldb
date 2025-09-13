use std::process;
use tokio::io::{BufReader, split};
use tokio::net::UnixListener;
use tokio::task;

use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use crate::frontend::unix::connection::Connection;
use crate::shared::config::CONFIG;
use crate::shared::response::json::JsonRenderer;
use crate::shared::response::render::Renderer;
use crate::shared::response::unix::UnixRenderer;
use anyhow::Context;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{ReadHalf, WriteHalf};
use tokio::sync::RwLock;

pub async fn run_server() -> anyhow::Result<()> {
    let socket_path = &CONFIG.server.socket_path;

    eprintln!("socket_path: {}", socket_path);
    if std::fs::metadata(socket_path).is_ok() {
        std::fs::remove_file(socket_path)?;
    }

    let listener = UnixListener::bind(socket_path)
        .with_context(|| format!("Failed to bind to socket path: {}", socket_path))?;
    eprintln!("listener: {:?}", listener);
    tracing::info!("Listening on {}", socket_path);

    let registry = Arc::new(RwLock::new(
        SchemaRegistry::new().expect("Failed to initialize SchemaRegistry"),
    ));

    // Resolve base dir from config
    let base_dir = PathBuf::from(&CONFIG.engine.data_dir);
    let wal_dir = PathBuf::from(&CONFIG.wal.dir);

    let shard_manager =
        Arc::new(ShardManager::new(CONFIG.engine.shard_count, base_dir, wal_dir).await);

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                tracing::info!("Accepted new connection");
                let shard_manager = Arc::clone(&shard_manager);
                let registry = Arc::clone(&registry);
                let renderer: Arc<dyn Renderer + Send + Sync> =
                    match CONFIG.server.output_format.as_str() {
                        "json" => Arc::new(JsonRenderer),
                        _ => Arc::new(UnixRenderer),
                    };

                task::spawn(async move {
                    let (r, w): (ReadHalf<_>, WriteHalf<_>) = split(stream);
                    let mut conn = Connection {
                        pid: process::id(),
                        reader: BufReader::new(r),
                        writer: w,
                        shard_manager,
                        registry,
                        renderer,
                    };
                    if let Err(e) = conn.run().await {
                        tracing::error!("Connection error: {e}");
                    }
                });
            }
            Err(e) => {
                tracing::error!("Failed to accept connection: {e}");
            }
        }
    }
}
