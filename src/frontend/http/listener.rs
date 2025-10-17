use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use std::{net::SocketAddr, sync::Arc};
use tokio::net::TcpListener;
use tracing::info;

use crate::frontend::context::FrontendContext;
use crate::shared::config::CONFIG;

use super::handler::handle_request;

pub async fn run_http_server(ctx: Arc<FrontendContext>) -> anyhow::Result<()> {
    let addr: SocketAddr = CONFIG.server.http_addr.parse()?;
    let listener = TcpListener::bind(addr).await?;

    info!("HTTP server running at http://{addr}/command");

    let ctx = ctx;

    loop {
        let (stream, _peer_addr) = listener.accept().await?;
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
                        )
                    }),
                )
                .await
            {
                eprintln!("Error serving connection: {:?}", err);
            }
        });
    }
}
