use crate::command::dispatcher::dispatch_command;
use crate::command::parser::parse_command;
use crate::frontend::context::FrontendContext;
use crate::shared::config::CONFIG;
use crate::shared::response::unix::UnixRenderer;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tracing::{info, warn};

pub async fn run_tcp_server(ctx: Arc<FrontendContext>) -> anyhow::Result<()> {
    let addr = &CONFIG.server.tcp_addr;

    let listener = TcpListener::bind(addr).await?;
    info!("TCP listener active on {}", addr);

    loop {
        // Check shutdown before accepting new connections
        if ctx.server_state.is_shutting_down() {
            info!("TCP server shutting down, not accepting new connections");
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
                info!("TCP server shutting down, stopping accept loop");
                break;
            }
        };

        let (stream, _) = match accept_result {
            Ok(stream) => stream,
            Err(e) => {
                warn!("Failed to accept TCP connection: {}", e);
                continue;
            }
        };

        let shard_manager = ctx.shard_manager.clone();
        let registry = ctx.registry.clone();
        let server_state = ctx.server_state.clone();

        tokio::spawn(async move {
            let mut reader = BufReader::new(stream);
            let mut line = String::new();

            loop {
                line.clear();
                let n = reader.read_line(&mut line).await.unwrap_or(0);
                if n == 0 {
                    break;
                }

                // Check shutdown and backpressure before processing each command
                if server_state.is_shutting_down() {
                    let writer = reader.get_mut();
                    let _ = writer.write_all(b"ERROR: Server is shutting down\n").await;
                    let _ = writer.flush().await;
                    break;
                }

                if server_state.is_under_pressure() {
                    let writer = reader.get_mut();
                    let _ = writer
                        .write_all(b"ERROR: Server is under pressure, please retry later\n")
                        .await;
                    let _ = writer.flush().await;
                    continue;
                }

                let trimmed = line.trim();
                match parse_command(trimmed) {
                    Ok(cmd) => {
                        // Increment pending operations before dispatch
                        server_state.increment_pending();

                        let result = dispatch_command(
                            &cmd,
                            reader.get_mut(),
                            &shard_manager,
                            &registry,
                            &UnixRenderer,
                        )
                        .await;

                        // Decrement after dispatch completes
                        server_state.decrement_pending();

                        if let Err(e) = result {
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

    // After breaking from accept loop, wait briefly for active connections to finish
    if ctx.server_state.is_shutting_down() {
        info!("TCP server waiting for active connections to complete...");
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    }

    info!("TCP server shutdown complete");
    Ok(())
}
