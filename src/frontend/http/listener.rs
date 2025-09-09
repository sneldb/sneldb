use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use tokio::{net::TcpListener, sync::RwLock};
use tracing::info;

use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use crate::shared::config::CONFIG;

use super::handler::handle_request;

pub async fn run_http_server() -> anyhow::Result<()> {
    let addr: SocketAddr = CONFIG.server.http_addr.parse()?;
    let listener = TcpListener::bind(addr).await?;

    info!("HTTP server running at http://{addr}/command");

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

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);

        let registry = Arc::clone(&registry);
        let shard_manager = Arc::clone(&shard_manager);

        tokio::spawn(async move {
            if let Err(err) = hyper::server::conn::http1::Builder::new()
                .serve_connection(
                    io,
                    service_fn(move |req| {
                        handle_request(req, Arc::clone(&registry), Arc::clone(&shard_manager))
                    }),
                )
                .await
            {
                eprintln!("Error serving connection: {:?}", err);
            }
        });
    }
}
