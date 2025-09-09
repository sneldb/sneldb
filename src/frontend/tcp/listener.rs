use crate::command::dispatcher::dispatch_command;
use crate::command::parser::parse_command;
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use crate::shared::config::CONFIG;
use crate::shared::response::unix::UnixRenderer;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tracing::info;

pub async fn run_tcp_server() -> anyhow::Result<()> {
    let addr = &CONFIG.server.tcp_addr;
    let registry = Arc::new(RwLock::new(
        SchemaRegistry::new().expect("Failed to initialize SchemaRegistry"),
    ));

    let base_dir = PathBuf::from(&CONFIG.engine.data_dir);
    let wal_dir = PathBuf::from(&CONFIG.wal.dir);

    let shard_manager = Arc::new(
        ShardManager::new(
            CONFIG.engine.shard_count,
            Arc::clone(&registry),
            base_dir,
            wal_dir,
        )
        .await,
    );

    let listener = TcpListener::bind(addr).await?;
    info!("TCP listener active on {}", addr);

    loop {
        let (stream, _) = listener.accept().await?;
        let shard_manager = shard_manager.clone();
        let registry = registry.clone();

        tokio::spawn(async move {
            let mut reader = BufReader::new(stream);
            let mut line = String::new();

            loop {
                line.clear();
                let n = reader.read_line(&mut line).await.unwrap_or(0);
                if n == 0 {
                    break;
                }

                let trimmed = line.trim();
                match parse_command(trimmed) {
                    Ok(cmd) => {
                        if let Err(e) = dispatch_command(
                            &cmd,
                            reader.get_mut(),
                            &shard_manager,
                            &registry,
                            &UnixRenderer,
                        )
                        .await
                        {
                            tracing::error!("Dispatch error: {e}");
                        }
                    }
                    Err(e) => {
                        let _ = reader
                            .get_mut()
                            .write_all(format!("ERROR: {e}\n").as_bytes())
                            .await;
                    }
                }
            }
        });
    }
}
