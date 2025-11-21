pub mod context;
pub mod http;
pub mod server_state;
pub mod tcp;
pub mod unix;
pub mod ws;

#[cfg(test)]
mod server_state_test;

use context::FrontendContext;
use std::sync::Arc;
use tokio::signal;
use tracing::{error, info, warn};

pub async fn start_all() -> anyhow::Result<()> {
    let ctx = FrontendContext::from_config().await;

    // Spawn shutdown signal handler
    let shutdown_handle = {
        let shutdown_ctx = Arc::clone(&ctx);
        tokio::spawn(async move {
            signal::ctrl_c()
                .await
                .expect("Failed to listen for shutdown signal");
            info!("Shutdown signal received, initiating graceful shutdown");
            shutdown_ctx.server_state.signal_shutdown();

            // Wait for pending operations to complete (with timeout)
            info!("Waiting for pending operations to complete...");
            let shutdown_wait_start = std::time::Instant::now();
            const MAX_SHUTDOWN_WAIT: std::time::Duration = std::time::Duration::from_secs(30);

            loop {
                let pending = shutdown_ctx.server_state.pending_operations_count();
                if pending == 0 {
                    info!("All pending operations completed");
                    break;
                }

                if shutdown_wait_start.elapsed() > MAX_SHUTDOWN_WAIT {
                    warn!(
                        target: "frontend::shutdown",
                        pending = pending,
                        "Shutdown timeout reached, proceeding with pending operations"
                    );
                    break;
                }

                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }

            // Give a small grace period for connections to close
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

            let registry = Arc::clone(&shutdown_ctx.registry);
            let flush_errors = shutdown_ctx.shard_manager.flush_all(registry).await;
            if !flush_errors.is_empty() {
                for (shard_id, err) in &flush_errors {
                    error!(
                        target: "frontend::shutdown",
                        shard_id = *shard_id,
                        error = %err,
                        "Flush during shutdown failed"
                    );
                }
            }

            let shutdown_errors = shutdown_ctx.shard_manager.shutdown_all().await;
            if !shutdown_errors.is_empty() {
                for (shard_id, err) in &shutdown_errors {
                    error!(
                        target: "frontend::shutdown",
                        shard_id = *shard_id,
                        error = %err,
                        "Shard shutdown signalling failed"
                    );
                }
            }

            info!("Graceful shutdown complete");
        })
    };

    let servers_result = tokio::try_join!(
        tcp::listener::run_tcp_server(Arc::clone(&ctx)),
        http::listener::run_http_server(Arc::clone(&ctx)),
        ws::listener::run_ws_server(Arc::clone(&ctx)),
    );

    if ctx.server_state.is_shutting_down() {
        if let Err(err) = shutdown_handle.await {
            error!(
                target: "frontend::shutdown",
                error = %err,
                "Shutdown task failed"
            );
        }
    } else {
        shutdown_handle.abort();
    }

    servers_result?;
    Ok(())
}
