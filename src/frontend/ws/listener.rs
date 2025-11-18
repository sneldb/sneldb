use crate::command::dispatcher::dispatch_command;
use crate::command::parser::parse_command;
use crate::frontend::context::FrontendContext;
use crate::frontend::tcp::listener::{TcpAuthState, check_auth};
use crate::shared::config::CONFIG;
use crate::shared::response::unix::UnixRenderer;
use futures_util::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::AsyncWrite;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::protocol::Message;
use tracing::{info, warn};

#[derive(Default)]
struct WsResponseBuffer {
    bytes: Vec<u8>,
}

impl AsyncWrite for WsResponseBuffer {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.bytes.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

pub async fn run_ws_server(ctx: Arc<FrontendContext>) -> anyhow::Result<()> {
    let addr: SocketAddr = CONFIG.server.ws_addr.parse()?;
    let listener = TcpListener::bind(addr).await?;

    info!("WebSocket server running at ws://{addr}/ws");

    loop {
        if ctx.server_state.is_shutting_down() {
            info!("WebSocket server shutting down, not accepting new connections");
            break;
        }

        let accept_result = tokio::select! {
            result = listener.accept() => result,
            _ = async {
                loop {
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    if ctx.server_state.is_shutting_down() {
                        break;
                    }
                }
            } => {
                info!("WebSocket server shutting down, stopping accept loop");
                break;
            }
        };

        let (stream, peer_addr) = match accept_result {
            Ok(pair) => pair,
            Err(e) => {
                warn!("Failed to accept WebSocket connection: {}", e);
                continue;
            }
        };

        let ctx = Arc::clone(&ctx);
        tokio::spawn(async move {
            handle_ws_connection(stream, peer_addr, ctx).await;
        });
    }

    if ctx.server_state.is_shutting_down() {
        info!("WebSocket server waiting for active connections to complete...");
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    }

    info!("WebSocket server shutdown complete");
    Ok(())
}

async fn handle_ws_connection(stream: TcpStream, peer_addr: SocketAddr, ctx: Arc<FrontendContext>) {
    let client_ip = peer_addr.ip().to_string();
    let shard_manager = ctx.shard_manager.clone();
    let registry = ctx.registry.clone();
    let server_state = ctx.server_state.clone();
    let auth_manager = ctx.auth_manager.clone();

    let ws_stream: WebSocketStream<TcpStream> = match accept_async(stream).await {
        Ok(ws) => ws,
        Err(e) => {
            warn!("WebSocket handshake failed from {}: {:?}", peer_addr, e);
            return;
        }
    };

    let auth_state = Arc::new(tokio::sync::Mutex::new(TcpAuthState::new(
        auth_manager.clone(),
        client_ip,
    )));

    // Split WebSocket into send and receive halves for concurrent processing
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();

    // Channel for sending responses back (bounded to prevent memory buildup, but large enough for throughput)
    // Use bounded channel with large capacity to allow backpressure without blocking command processing
    let (tx, mut rx) = mpsc::channel::<Message>(10000);

    // Spawn task to send responses
    let send_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if ws_sender.send(msg).await.is_err() {
                break;
            }
        }
    });

    // Process incoming messages concurrently
    while let Some(msg) = ws_receiver.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                if server_state.is_shutting_down() {
                    let _ = tx.send(Message::Text(
                        "ERROR: Server is shutting down\n".to_string(),
                    ));
                    break;
                }

                if server_state.is_under_pressure() {
                    let _ = tx.send(Message::Text(
                        "ERROR: Server is under pressure, please retry later\n".to_string(),
                    ));
                    continue;
                }

                let trimmed = text.trim().to_string(); // Clone for spawned task
                let tx_clone = tx.clone();
                let shard_manager_clone = shard_manager.clone();
                let registry_clone = registry.clone();
                let server_state_clone = server_state.clone();
                let auth_manager_clone = auth_manager.clone();
                let auth_state_clone = auth_state.clone();

                // Process command concurrently (spawn task)
                tokio::spawn(async move {
                    tracing::warn!(
                        target: "sneldb::ws",
                        command_preview = &trimmed[..trimmed.len().min(80)],
                        "Received WebSocket command"
                    );

                    // Fast path: Check for TOKEN format first (no lock needed)
                    if let Some(token_pos) = trimmed.rfind(" TOKEN ") {
                        let (command_without_token, token_part) = trimmed.split_at(token_pos);
                        let token = token_part.strip_prefix(" TOKEN ").unwrap_or("").trim();

                        if !token.is_empty() && token.len() <= 128 {
                            // Validate token directly (no lock needed for TOKEN auth)
                            if let Some(ref auth_mgr) = auth_manager_clone {
                                if let Some(user_id) = auth_mgr.validate_session_token(token).await
                                {
                                    let command_trimmed = command_without_token.trim();
                                    tracing::warn!(
                                        target: "sneldb::ws",
                                        user_id = user_id.as_str(),
                                        "TOKEN auth succeeded (fast path)"
                                    );

                                    // Process command without holding auth_state lock
                                    match parse_command(command_trimmed) {
                                        Ok(cmd) => {
                                            tracing::warn!(
                                                target: "sneldb::ws",
                                                "Command parsed, dispatching"
                                            );
                                            server_state_clone.increment_pending();

                                            let mut buffer = WsResponseBuffer::default();
                                            let result = dispatch_command(
                                                &cmd,
                                                &mut buffer,
                                                &shard_manager_clone,
                                                &registry_clone,
                                                auth_manager_clone.as_ref(),
                                                Some(user_id.as_str()),
                                                &UnixRenderer,
                                            )
                                            .await;

                                            server_state_clone.decrement_pending();
                                            tracing::warn!(
                                                target: "sneldb::ws",
                                                success = result.is_ok(),
                                                "Command dispatch completed"
                                            );

                                            match result {
                                                Ok(_) => {
                                                    let response =
                                                        String::from_utf8_lossy(&buffer.bytes);
                                                    let _ = tx_clone.try_send(Message::Text(
                                                        response.into_owned(),
                                                    ));
                                                }
                                                Err(e) => {
                                                    let _ = tx_clone.try_send(Message::Text(
                                                        format!("ERROR: Dispatch error: {}\n", e),
                                                    ));
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            let _ = tx_clone
                                                .try_send(Message::Text(format!("ERROR: {e}\n")));
                                        }
                                    }
                                    return; // Fast path complete, exit early
                                }
                            }
                        }
                    }

                    // Slow path: Need auth_state lock for AUTH commands or connection-scoped auth
                    let mut auth_state_guard = auth_state_clone.lock().await;
                    match check_auth(&trimmed, &mut auth_state_guard).await {
                        Some(("OK", _, _, Some(token))) => {
                            let _ =
                                tx_clone.try_send(Message::Text(format!("OK TOKEN {}\n", token)));
                        }
                        Some(("OK", _, _, None)) => {
                            let _ = tx_clone.try_send(Message::Text("OK\n".to_string()));
                        }
                        Some((command_to_parse, _, authenticated_user_id, _)) => {
                            tracing::warn!(
                                target: "sneldb::ws",
                                user_id = authenticated_user_id.as_deref().unwrap_or("unknown"),
                                command = command_to_parse,
                                "Command authenticated, parsing"
                            );
                            match parse_command(command_to_parse) {
                                Ok(cmd) => {
                                    tracing::warn!(
                                        target: "sneldb::ws",
                                        "Command parsed, dispatching"
                                    );
                                    server_state_clone.increment_pending();

                                    let mut buffer = WsResponseBuffer::default();
                                    let result = dispatch_command(
                                        &cmd,
                                        &mut buffer,
                                        &shard_manager_clone,
                                        &registry_clone,
                                        auth_manager_clone.as_ref(),
                                        authenticated_user_id.as_deref(),
                                        &UnixRenderer,
                                    )
                                    .await;

                                    server_state_clone.decrement_pending();
                                    tracing::warn!(
                                        target: "sneldb::ws",
                                        success = result.is_ok(),
                                        "Command dispatch completed"
                                    );

                                    match result {
                                        Ok(_) => {
                                            let response = String::from_utf8_lossy(&buffer.bytes);
                                            // Try to send response non-blocking (fire-and-forget for high throughput)
                                            // If channel is full, drop the response (command still succeeded)
                                            if tx_clone
                                                .try_send(Message::Text(response.into_owned()))
                                                .is_err()
                                            {
                                                // Channel full or closed - command still processed successfully
                                            }
                                        }
                                        Err(e) => {
                                            // Always try to send errors (but don't block)
                                            let _ = tx_clone.try_send(Message::Text(format!(
                                                "ERROR: Dispatch error: {}\n",
                                                e
                                            )));
                                        }
                                    }
                                }
                                Err(e) => {
                                    let _ =
                                        tx_clone.try_send(Message::Text(format!("ERROR: {e}\n")));
                                }
                            }
                        }
                        None => {
                            tracing::warn!(
                                target: "sneldb::ws",
                                command_preview = &trimmed[..trimmed.len().min(80)],
                                "Authentication failed for command"
                            );
                            let _ = tx_clone
                                .send(Message::Text("ERROR: Authentication failed\n".to_string()));
                        }
                    }
                });
            }
            Ok(Message::Ping(payload)) => {
                let _ = tx.send(Message::Pong(payload));
            }
            Ok(Message::Close(_)) => break,
            Ok(Message::Binary(_)) => {
                let _ = tx.send(Message::Text(
                    "ERROR: Binary frames are not supported\n".to_string(),
                ));
            }
            Ok(Message::Frame(_)) => {
                let _ = tx.send(Message::Text(
                    "ERROR: Fragmented frames are not supported\n".to_string(),
                ));
            }
            Ok(Message::Pong(_)) => {}
            Err(e) => {
                warn!("WebSocket error from {}: {:?}", peer_addr, e);
                break;
            }
        }
    }

    // Close send channel and wait for send task
    drop(tx);
    let _ = send_task.await;
}
