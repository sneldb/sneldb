use crate::command::dispatcher::dispatch_command;
use crate::command::parser::parse_command;
use crate::engine::auth::AuthManager;
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use crate::shared::config::CONFIG;
use crate::shared::response::render::Renderer;
use crate::shared::response::{Response, StatusCode};
use std::io::ErrorKind;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, BufReader};

pub struct Connection<R, W> {
    pub pid: u32,
    pub reader: BufReader<R>,
    pub writer: W,
    pub shard_manager: Arc<ShardManager>,
    pub registry: Arc<tokio::sync::RwLock<SchemaRegistry>>,
    pub renderer: Arc<dyn Renderer + Send + Sync>,
    pub auth_manager: Option<Arc<AuthManager>>,
}

impl<R, W> Connection<R, W>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    /// Check authentication before parsing command
    /// Returns (command, user_id) if authenticated, or None if auth check failed
    async fn check_auth<'a>(&self, input: &'a str) -> Option<(&'a str, Option<String>)> {
        // Check if authentication is bypassed via config - do this first for performance
        if CONFIG.auth.as_ref().map(|a| a.bypass_auth).unwrap_or(false) {
            return Some((input, Some("bypass".to_string())));
        }

        // Cache trimmed input
        let trimmed = input.trim();

        // User management commands now require authentication

        // If auth manager is not configured, allow all commands
        let auth_mgr = match &self.auth_manager {
            Some(am) => am,
            None => return Some((input, Some("no-auth".to_string()))),
        };

        // Parse auth format: user_id:signature:command
        match auth_mgr.parse_auth(trimmed) {
            Ok((user_id, signature, command)) => {
                // Verify signature (UNIX sockets: no rate limiting - local only)
                match auth_mgr
                    .verify_signature(command, user_id, signature, None)
                    .await
                {
                    Ok(_) => Some((command, Some(user_id.to_string()))),
                    Err(_) => None,
                }
            }
            Err(_) => {
                // If parsing fails, authentication is required for all commands
                None
            }
        }
    }

    pub async fn run(&mut self) -> std::io::Result<()> {
        tracing::info!("[PID {}] Connection started", self.pid);

        loop {
            let mut line = String::new();
            let bytes = self.reader.read_line(&mut line).await?;
            if bytes == 0 {
                tracing::info!("[PID {}] EOF - closing connection", self.pid);
                break;
            }

            let input = line.trim();
            if input.is_empty() {
                continue;
            }

            if input == "exit" {
                let resp = Response::ok_lines(vec!["Bye!".to_string()]);
                if let Err(e) = self.writer.write_all(&self.renderer.render(&resp)).await {
                    if e.kind() == ErrorKind::BrokenPipe {
                        tracing::info!("[PID {}] Client disconnected", self.pid);
                        break;
                    }
                    return Err(e);
                }
                break;
            }

            // Check authentication before parsing
            let (command_to_parse, authenticated_user_id) = match self.check_auth(input).await {
                Some((cmd, uid)) => (cmd, uid),
                None => {
                    let resp = Response::error(
                        StatusCode::BadRequest,
                        "Authentication failed".to_string(),
                    );
                    if let Err(e) = self.writer.write_all(&self.renderer.render(&resp)).await {
                        if e.kind() == ErrorKind::BrokenPipe {
                            tracing::info!("[PID {}] Client disconnected", self.pid);
                            break;
                        }
                        return Err(e);
                    }
                    continue;
                }
            };

            match parse_command(command_to_parse) {
                Ok(cmd) => {
                    if let Err(e) = dispatch_command(
                        &cmd,
                        &mut self.writer,
                        &self.shard_manager,
                        &self.registry,
                        self.auth_manager.as_ref(),
                        authenticated_user_id.as_deref(),
                        self.renderer.as_ref(),
                    )
                    .await
                    {
                        if e.kind() == ErrorKind::BrokenPipe {
                            tracing::info!("[PID {}] Client disconnected", self.pid);
                            break;
                        }
                        tracing::error!("Dispatch error: {e}");
                    }
                }
                Err(e) => {
                    let resp = Response::error(StatusCode::BadRequest, e.to_string());
                    if let Err(e) = self.writer.write_all(&self.renderer.render(&resp)).await {
                        if e.kind() == ErrorKind::BrokenPipe {
                            tracing::info!("[PID {}] Client disconnected", self.pid);
                            break;
                        }
                        return Err(e);
                    }
                }
            }
        }

        Ok(())
    }
}
