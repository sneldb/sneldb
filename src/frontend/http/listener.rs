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

    // Configure keep-alive behavior
    // Default: disable keep-alive for high-throughput scenarios to prevent FD exhaustion
    // Set to "true" to enable keep-alive (connections stay open for multiple requests)
    let enable_keep_alive: bool = std::env::var("SNELDB_HTTP_KEEP_ALIVE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(false);

    // Optional connection limit (matches TCP behavior: no limit by default)
    // With keep-alive disabled (default), connections close after each request,
    // so this limit is only needed if keep-alive is enabled or for very high load scenarios
    // Set to 0 or unset to disable the limit (unlimited, like TCP)
    let max_connections: Option<usize> = std::env::var("SNELDB_MAX_HTTP_CONNECTIONS")
        .ok()
        .and_then(|s| {
            let val: usize = s.parse().ok()?;
            if val == 0 {
                None
            } else {
                Some(val)
            }
        });
    let connection_semaphore = max_connections.map(|max| Arc::new(tokio::sync::Semaphore::new(max)));

    loop {
        // Check shutdown before accepting new connections
        if ctx.server_state.is_shutting_down() {
            info!("HTTP server shutting down, not accepting new connections");
            break;
        }

        // Acquire permit if connection limit is enabled (matches TCP: no limit by default)
        let permit = if let Some(ref semaphore) = connection_semaphore {
            Some(semaphore.clone().acquire_owned().await.unwrap())
        } else {
            None
        };

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
                if let Some(p) = permit {
                    drop(p);
                }
                break;
            }
        };

        let (stream, _peer_addr) = match accept_result {
            Ok(stream) => stream,
            Err(e) => {
                warn!("Failed to accept HTTP connection: {}", e);
                if let Some(p) = permit {
                    drop(p);
                }
                continue;
            }
        };
        let io = TokioIo::new(stream);

        let ctx = Arc::clone(&ctx);

        let keep_alive_enabled = enable_keep_alive;
        // Move permit into the spawned task so it's released when connection closes
        let permit_for_task = permit;
        tokio::spawn(async move {
            // Configure HTTP/1.1 connection with keep-alive behavior
            // Disabling keep-alive prevents connections from staying open indefinitely,
            // which helps prevent file descriptor exhaustion under high load
            let mut builder = hyper::server::conn::http1::Builder::new();
            builder.keep_alive(keep_alive_enabled);

            // Connection will be closed when this task completes, releasing the permit
            if let Err(err) = builder
                .serve_connection(
                    io,
                    service_fn(move |req| {
                        handle_request(
                            req,
                            Arc::clone(&ctx.registry),
                            Arc::clone(&ctx.shard_manager),
                            Arc::clone(&ctx.server_state),
                            ctx.auth_manager.clone(),
                        )
                    }),
                )
                .await
            {
                // Only log non-connection-closed errors to reduce noise
                if !err.to_string().contains("connection closed")
                    && !err.to_string().contains("broken pipe")
                    && !err.to_string().contains("Connection reset") {
                    eprintln!("Error serving connection: {:?}", err);
                }
            }
            // Permit is released when connection task completes (if limit is enabled)
            if let Some(p) = permit_for_task {
                drop(p);
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
