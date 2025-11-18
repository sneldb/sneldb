use crate::command::dispatcher::dispatch_command;
use crate::command::parser::parse_command;
use crate::engine::auth::AuthManager;
use crate::frontend::context::FrontendContext;
use crate::shared::config::CONFIG;
use crate::shared::response::unix::UnixRenderer;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tracing::{info, warn};

/// Case-insensitive byte comparison helper
#[inline]
pub(crate) fn bytes_eq_ignore_ascii_case(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b.iter())
            .all(|(x, y)| x.eq_ignore_ascii_case(y))
}

/// Connection-scoped authentication state
pub(crate) struct TcpAuthState {
    user_id: Option<String>,
    auth_manager: Option<Arc<AuthManager>>,
    client_ip: String,
}

impl TcpAuthState {
    pub(crate) fn new(auth_manager: Option<Arc<AuthManager>>, client_ip: String) -> Self {
        Self {
            user_id: None,
            auth_manager,
            client_ip,
        }
    }

    /// Authenticate the connection using AUTH command
    /// Format: AUTH user_id:signature
    /// Signature should be computed as: HMAC-SHA256(secret_key, user_id)
    ///
    /// # Security
    /// - Rate limiting is applied per IP address on initial authentication
    /// - After successful auth, subsequent commands skip rate limiting for full throughput
    /// - Returns a session token for high-throughput WebSocket authentication
    async fn authenticate(&mut self, input: &str) -> Result<String, String> {
        let parts: Vec<&str> = input.splitn(2, ' ').collect();
        if parts.len() != 2 {
            return Err("Invalid AUTH format. Use: AUTH user_id:signature".to_string());
        }

        let auth_part = parts[1].trim();
        let auth_parts: Vec<&str> = auth_part.splitn(2, ':').collect();
        if auth_parts.len() != 2 {
            return Err("Invalid AUTH format. Use: AUTH user_id:signature".to_string());
        }

        let user_id = auth_parts[0];
        let signature = auth_parts[1];

        let auth_mgr = match &self.auth_manager {
            Some(am) => am,
            None => return Err("Authentication not configured".to_string()),
        };

        // Verify signature with rate limiting (initial auth)
        // Pass client IP to enable per-IP rate limiting for brute-force protection
        match auth_mgr
            .verify_signature(user_id, user_id, signature, Some(&self.client_ip))
            .await
        {
            Ok(_) => {
                self.user_id = Some(user_id.to_string());
                // Generate session token for high-throughput authentication
                let token = auth_mgr.generate_session_token(user_id).await;
                Ok(token)
            }
            Err(e) => Err(format!("Authentication failed: {}", e)),
        }
    }

    /// Get authenticated user ID
    fn user_id(&self) -> Option<&str> {
        self.user_id.as_deref()
    }
}

/// Check authentication before parsing command
/// Supports:
/// 1. AUTH command to authenticate the connection (returns token)
/// 2. TOKEN format: STORE ... TOKEN <token> (session token auth)
/// 3. Inline format: user_id:signature:command (per-command auth)
/// 4. Connection-scoped auth (after AUTH command): signature:command
/// Returns (command, should_continue, user_id, token) if authenticated, or None if auth check failed
/// For AUTH command, returns ("OK", true, user_id, Some(token))
pub(crate) async fn check_auth<'a>(
    input: &'a str,
    auth_state: &mut TcpAuthState,
) -> Option<(&'a str, bool, Option<String>, Option<String>)> {
    // Check if authentication is bypassed via config - do this first for performance
    if CONFIG.auth.as_ref().map(|a| a.bypass_auth).unwrap_or(false) {
        return Some((input.trim(), true, Some("bypass".to_string()), None));
    }

    // Cache trimmed input to avoid multiple trim() calls
    let trimmed = input.trim();
    let trimmed_bytes = trimmed.as_bytes();

    // Handle AUTH command (case-insensitive byte check - only when needed)
    if trimmed_bytes.len() >= 5 && bytes_eq_ignore_ascii_case(&trimmed_bytes[..5], b"AUTH ") {
        tracing::warn!(
            target: "sneldb::auth",
            "Received AUTH command"
        );
        match auth_state.authenticate(trimmed).await {
            Ok(token) => {
                let user_id = auth_state.user_id().map(|s| s.to_string());
                tracing::warn!(
                    target: "sneldb::auth",
                    user_id = user_id.as_deref().unwrap_or("unknown"),
                    token_len = token.len(),
                    "AUTH succeeded, token generated"
                );
                return Some(("OK", true, user_id, Some(token))); // Return OK response with token
            }
            Err(e) => {
                tracing::warn!(
                    target: "sneldb::auth",
                    error = e,
                    "AUTH failed"
                );
                return None; // Will send error response
            }
        }
    }

    // User management commands now require authentication

    // If auth manager is not configured, allow all commands
    let auth_mgr = match &auth_state.auth_manager {
        Some(am) => am,
        None => return Some((trimmed, true, Some("no-auth".to_string()), None)),
    };

    // Check for TOKEN format: STORE ... TOKEN <token>
    // This is the high-throughput authentication method for WebSocket
    // Token must be at the end of the command (after " TOKEN ")
    // Use rfind to find the last occurrence (in case "TOKEN" appears in payload JSON)
    if let Some(token_pos) = trimmed.rfind(" TOKEN ") {
        let (command_without_token, token_part) = trimmed.split_at(token_pos);
        let token = token_part.strip_prefix(" TOKEN ")?.trim();

        tracing::warn!(
            target: "sneldb::auth",
            command_preview = &command_without_token[..command_without_token.len().min(50)],
            token_len = token.len(),
            "Detected TOKEN format in command"
        );

        // Basic validation: token should be hex string (64 chars for 32 bytes)
        // Allow any length hex string for flexibility, but validate it's not empty
        if !token.is_empty() && token.len() <= 128 {
            let command_trimmed = command_without_token.trim();

            // Validate token (fast O(1) hash lookup)
            if let Some(user_id) = auth_mgr.validate_session_token(token).await {
                tracing::warn!(
                    target: "sneldb::auth",
                    user_id = user_id,
                    "TOKEN auth succeeded"
                );
                return Some((command_trimmed, true, Some(user_id), None));
            }
            // Token invalid or expired - fall through to other auth methods
            tracing::warn!(
                target: "sneldb::auth",
                "TOKEN auth failed, falling back to other methods"
            );
        } else {
            tracing::warn!(
                target: "sneldb::auth",
                token_len = token.len(),
                "Invalid token format (empty or too long)"
            );
        }
        // If token format is invalid, fall through to other auth methods
    }

    // Try connection-scoped authentication first
    if let Some(user_id) = auth_state.user_id() {
        // For authenticated connections, check if it's in the short format: signature:command
        // No case conversion needed here - we're just checking for colon
        if let Some(colon_pos) = trimmed_bytes.iter().position(|&b| b == b':') {
            let potential_signature = &trimmed[..colon_pos];
            let command_part = &trimmed[colon_pos + 1..];
            let command_part_trimmed = command_part.trim();

            // For authenticated connections, skip rate limiting (allows full throughput)
            // Connection is already authenticated via AUTH command (which was rate limited)
            match auth_mgr
                .verify_signature(command_part_trimmed, user_id, potential_signature, None)
                .await
            {
                Ok(_) => {
                    return Some((command_part_trimmed, true, Some(user_id.to_string()), None));
                }
                Err(_) => {
                    // Early return on auth failure - don't try inline format for authenticated connections
                    return None;
                }
            }
        } else {
            // No colon found - commands without signature are rejected for authenticated connections
            return None;
        }
    }

    // Fall back to inline format: user_id:signature:command (per-command auth)
    // Apply rate limiting for inline auth as each command is an auth attempt
    match auth_mgr.parse_auth(trimmed) {
        Ok((user_id, signature, command)) => {
            // Verify signature with rate limiting (per-command auth)
            match auth_mgr
                .verify_signature(command, user_id, signature, Some(&auth_state.client_ip))
                .await
            {
                Ok(_) => Some((command, true, Some(user_id.to_string()), None)),
                Err(_) => None,
            }
        }
        Err(_) => {
            // If parsing fails, authentication is required for all commands
            None
        }
    }
}

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

        let (stream, peer_addr) = match accept_result {
            Ok(stream) => stream,
            Err(e) => {
                warn!("Failed to accept TCP connection: {}", e);
                continue;
            }
        };

        let client_ip = peer_addr.ip().to_string();
        let shard_manager = ctx.shard_manager.clone();
        let registry = ctx.registry.clone();
        let server_state = ctx.server_state.clone();
        let auth_manager = ctx.auth_manager.clone();

        tokio::spawn(async move {
            let mut reader = BufReader::new(stream);
            let mut line = String::new();
            let mut auth_state = TcpAuthState::new(auth_manager.clone(), client_ip);

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

                // Check authentication before parsing
                match check_auth(trimmed, &mut auth_state).await {
                    Some(("OK", _, _, Some(token))) => {
                        // AUTH command succeeded - return token
                        let writer = reader.get_mut();
                        let _ = writer
                            .write_all(format!("OK TOKEN {}\n", token).as_bytes())
                            .await;
                        let _ = writer.flush().await;
                        continue;
                    }
                    Some(("OK", _, _, None)) => {
                        // AUTH command succeeded (no token - should not happen)
                        let writer = reader.get_mut();
                        let _ = writer.write_all(b"OK\n").await;
                        let _ = writer.flush().await;
                        continue;
                    }
                    Some((command_to_parse, _, authenticated_user_id, _)) => {
                        match parse_command(command_to_parse) {
                            Ok(cmd) => {
                                // Increment pending operations before dispatch
                                server_state.increment_pending();

                                let result = dispatch_command(
                                    &cmd,
                                    reader.get_mut(),
                                    &shard_manager,
                                    &registry,
                                    auth_manager.as_ref(),
                                    authenticated_user_id.as_deref(),
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
                    None => {
                        let writer = reader.get_mut();
                        // Check if it was an AUTH command that failed
                        if trimmed.to_uppercase().starts_with("AUTH ") {
                            let _ = writer
                                .write_all(format!("ERROR: Authentication failed\n").as_bytes())
                                .await;
                        } else {
                            let _ = writer.write_all(b"ERROR: Authentication failed\n").await;
                        }
                        let _ = writer.flush().await;
                        continue;
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
