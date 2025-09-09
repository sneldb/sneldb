use crate::command::dispatcher::dispatch_command;
use crate::command::parser::parse_command;
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
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
}

impl<R, W> Connection<R, W>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
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

            match parse_command(input) {
                Ok(cmd) => {
                    if let Err(e) = dispatch_command(
                        &cmd,
                        &mut self.writer,
                        &self.shard_manager,
                        &self.registry,
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
