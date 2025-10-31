use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::frontend::context::FrontendContext;
use crate::shared::config::CONFIG;

use super::handler::handle_request;

pub async fn run_http_server(ctx: Arc<FrontendContext>) -> anyhow::Result<()> {
    let addr: SocketAddr = CONFIG.server.http_addr.parse()?;
    let listener = TcpListener::bind(addr).await?;

    info!("HTTP server running at http://{addr}/command");

    loop {
        // Check shutdown before accepting new connections
        if ctx.server_state.is_shutting_down() {
            info!("HTTP server shutting down, not accepting new connections");
            break;
        }

        // Use select to make accept cancellable on shutdown
        let accept_result = tokio::select! {
            result = listener.accept() => result,
            _ = async {
                // Poll shutdown flag periodically
                loop {
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    if ctx.server_state.is_shutting_down() {
                        break;
                    }
                }
            } => {
                // Shutdown detected during accept wait
                info!("HTTP server shutting down, stopping accept loop");
                break;
            }
        };

        let (stream, _peer_addr) = match accept_result {
            Ok(stream) => stream,
            Err(e) => {
                warn!("Failed to accept HTTP connection: {}", e);
                continue;
            }
        };
        let io = TokioIo::new(stream);

        let ctx = Arc::clone(&ctx);

        tokio::spawn(async move {
            if let Err(err) = hyper::server::conn::http1::Builder::new()
                .serve_connection(
                    io,
                    service_fn(move |req| {
                        handle_request(
                            req,
                            Arc::clone(&ctx.registry),
                            Arc::clone(&ctx.shard_manager),
                            Arc::clone(&ctx.server_state),
                        )
                    }),
                )
                .await
            {
                eprintln!("Error serving connection: {:?}", err);
            }
        });
    }

    // After breaking from accept loop, wait briefly for active connections to finish
    if ctx.server_state.is_shutting_down() {
        info!("HTTP server waiting for active connections to complete...");
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    }

    info!("HTTP server shutdown complete");
    Ok(())
}
