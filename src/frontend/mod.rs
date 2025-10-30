pub mod context;
pub mod http;
pub mod server_state;
pub mod tcp;
pub mod unix;

#[cfg(test)]
mod server_state_test;

use context::FrontendContext;
use std::sync::Arc;
use tokio::signal;
use tracing::info;

pub async fn start_all() -> anyhow::Result<()> {
    let ctx = FrontendContext::from_config().await;

    // Spawn shutdown signal handler
    let server_state = Arc::clone(&ctx.server_state);
    tokio::spawn(async move {
        signal::ctrl_c().await.expect("Failed to listen for shutdown signal");
        info!("Shutdown signal received, initiating graceful shutdown");
        server_state.signal_shutdown();
    });

    tokio::try_join!(
        tcp::listener::run_tcp_server(Arc::clone(&ctx)),
        http::listener::run_http_server(Arc::clone(&ctx)),
    )?;
    Ok(())
}
