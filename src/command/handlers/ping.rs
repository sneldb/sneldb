use crate::command::types::Command;
use crate::shared::response::Response;
use crate::shared::response::render::Renderer;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tracing::debug;

pub async fn handle<W: AsyncWrite + Unpin>(
    _cmd: &Command,
    writer: &mut W,
    renderer: &dyn Renderer,
) -> std::io::Result<()> {
    debug!(target: "sneldb::ping", "Received PING command");

    let resp = Response::ok_lines(vec!["PONG".to_string()]);
    writer.write_all(&renderer.render(&resp)).await?;
    writer.flush().await?;
    Ok(())
}
