use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpListener;
use tracing::{debug, info, warn};

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

    // Connection limit with reasonable default
    // Default to 1024 concurrent connections to prevent resource exhaustion
    // With keep-alive disabled (default), connections close after each request,
    // so this mainly limits concurrent request processing
    // Set to 0 or unset to use default, or specify a custom limit
    let max_connections: usize = std::env::var("SNELDB_MAX_HTTP_CONNECTIONS")
        .ok()
        .and_then(|s| {
            let val: usize = s.parse().ok()?;
            if val == 0 { Some(1024) } else { Some(val) }
        })
        .unwrap_or(1024);
    let connection_semaphore = Arc::new(tokio::sync::Semaphore::new(max_connections));

    // Track connection statistics for periodic logging
    let connection_stats = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let connection_stats_clone = connection_stats.clone();
    let semaphore_for_stats = connection_semaphore.clone();
    let max_connections_for_stats = max_connections;

    // Spawn periodic connection statistics logger
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
        loop {
            interval.tick().await;
            let total_accepted = connection_stats_clone.load(std::sync::atomic::Ordering::Relaxed);
            let active = max_connections_for_stats - semaphore_for_stats.available_permits();
            if total_accepted > 0 || active > 0 {
                warn!(
                    "HTTP connection stats: total_accepted={}, active={}/{}",
                    total_accepted, active, max_connections_for_stats
                );
            }
        }
    });

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

        let (stream, peer_addr) = match accept_result {
            Ok((stream, addr)) => {
                // Increment connection counter
                connection_stats.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                (stream, addr)
            }
            Err(e) => {
                warn!("Failed to accept HTTP connection: {}", e);
                continue;
            }
        };

        // Optimize TCP buffer sizes for better network I/O performance
        // Larger buffers reduce syscall overhead and improve throughput
        use socket2::SockRef;

        // Set TCP_NODELAY for lower latency (disable Nagle's algorithm)
        if let Err(e) = stream.set_nodelay(true) {
            debug!("Failed to set nodelay: {}", e);
        }

        // Configure TCP buffer sizes using socket2
        #[cfg(unix)]
        {
            let sock_ref = SockRef::from(&stream);
            // Set send buffer size (1MB)
            if let Err(e) = sock_ref.set_send_buffer_size(1024 * 1024) {
                debug!("Failed to set send buffer size: {}", e);
            }
            // Set receive buffer size (1MB)
            if let Err(e) = sock_ref.set_recv_buffer_size(1024 * 1024) {
                debug!("Failed to set recv buffer size: {}", e);
            }
        }

        // Try to acquire permit - if we can't, drop the connection to prevent overload
        // This prevents unbounded task spawning when the server is under heavy load
        let available = connection_semaphore.available_permits();
        let permit = match connection_semaphore.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                // Connection limit reached - drop the connection
                // Client will retry and hopefully get through when capacity is available
                warn!(
                    "Connection limit reached (active={}/{}, max={}), dropping connection from {}",
                    max_connections - available,
                    max_connections,
                    max_connections,
                    peer_addr
                );
                continue;
            }
        };
        let io = TokioIo::new(stream);

        let ctx = Arc::clone(&ctx);
        let peer_addr_log = peer_addr;

        let keep_alive_enabled = enable_keep_alive;
        // Move permit into the spawned task so it's released when connection closes
        let permit_for_task = permit;
        tokio::spawn(async move {
            let connection_start = Instant::now();
            // Configure HTTP/1.1 connection with keep-alive behavior
            // Disabling keep-alive prevents connections from staying open indefinitely,
            // which helps prevent file descriptor exhaustion under high load
            let mut builder = hyper::server::conn::http1::Builder::new();
            builder.keep_alive(keep_alive_enabled);

            // Note: Hyper handles keep-alive timeout automatically
            // When keep-alive is enabled, connections will timeout based on the
            // underlying TCP keepalive settings and read timeouts
            // The connection will close naturally when idle or on errors

            let connection_result = builder
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
                .await;

            let duration = connection_start.elapsed();

            // Log connection lifecycle - only warn if there's an error or unusual behavior
            match connection_result {
                Ok(_) => {
                    // Connection completed successfully
                    // Only log if connection was open for an unusually long time (>1s)
                    // Fast closures (<1s) are normal with keep-alive disabled
                    if duration.as_secs_f64() > 1.0 {
                        warn!(
                            "HTTP connection from {} closed after {:.2}s (unusually long)",
                            peer_addr_log,
                            duration.as_secs_f64()
                        );
                    }
                }
                Err(err) => {
                    // Connection had an error
                    let err_str = err.to_string();
                    if !err_str.contains("connection closed")
                        && !err_str.contains("broken pipe")
                        && !err_str.contains("Connection reset")
                        && !err_str.contains("Socket is not connected")
                        && !err_str.contains("NotConnected")
                        && !err_str.contains("Shutdown")
                    {
                        warn!(
                            "Error serving HTTP connection from {} after {:.2}ms: {:?}",
                            peer_addr_log,
                            duration.as_secs_f64() * 1000.0,
                            err
                        );
                    }
                }
            }

            // Permit is released when connection task completes
            drop(permit_for_task);
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
