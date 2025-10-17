pub mod context;
pub mod http;
pub mod tcp;
pub mod unix;

use context::FrontendContext;
use std::sync::Arc;

pub async fn start_all() -> anyhow::Result<()> {
    let ctx = FrontendContext::from_config().await;
    tokio::try_join!(
        tcp::listener::run_tcp_server(Arc::clone(&ctx)),
        http::listener::run_http_server(Arc::clone(&ctx)),
    )?;
    Ok(())
}
