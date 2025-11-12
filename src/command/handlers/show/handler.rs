use std::sync::Arc;

use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::command::types::Command;
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use crate::shared::response::render::Renderer;
use crate::shared::response::{Response, StatusCode};

use super::catalog::FileCatalogGateway;
use super::errors::ShowError;
use super::orchestrator::ShowExecutionPipeline;

pub async fn handle<W: AsyncWrite + Unpin>(
    cmd: &Command,
    shard_manager: &ShardManager,
    registry: &Arc<tokio::sync::RwLock<SchemaRegistry>>,
    writer: &mut W,
    renderer: &dyn Renderer,
) -> std::io::Result<()> {
    let Command::ShowMaterialized { name } = cmd else {
        let resp = Response::error(StatusCode::BadRequest, "Invalid SHOW command");
        return writer.write_all(&renderer.render(&resp)).await;
    };

    let handler = match ShowCommandHandler::new(name, shard_manager, Arc::clone(registry)) {
        Ok(handler) => handler,
        Err(error) => {
            let resp = Response::error(StatusCode::InternalError, error.message());
            return writer.write_all(&renderer.render(&resp)).await;
        }
    };

    match handler.execute(writer, renderer).await {
        Ok(()) => Ok(()),
        Err(error) => {
            let resp = Response::error(StatusCode::InternalError, error.message());
            writer.write_all(&renderer.render(&resp)).await
        }
    }
}

pub struct ShowCommandHandler<'a> {
    pipeline: ShowExecutionPipeline<'a, FileCatalogGateway>,
}

impl<'a> ShowCommandHandler<'a> {
    pub fn new(
        alias: &'a str,
        shard_manager: &'a ShardManager,
        registry: Arc<tokio::sync::RwLock<SchemaRegistry>>,
    ) -> Result<Self, ShowError> {
        let pipeline = ShowExecutionPipeline::new(alias, shard_manager, registry)?;
        Ok(Self { pipeline })
    }

    #[cfg(test)]
    pub fn new_with_data_dir(
        alias: &'a str,
        shard_manager: &'a ShardManager,
        registry: Arc<tokio::sync::RwLock<SchemaRegistry>>,
        data_dir: impl AsRef<std::path::Path>,
    ) -> Result<Self, ShowError> {
        let pipeline =
            ShowExecutionPipeline::new_with_data_dir(alias, shard_manager, registry, data_dir)?;
        Ok(Self { pipeline })
    }

    pub async fn execute<W: AsyncWrite + Unpin>(
        self,
        writer: &mut W,
        renderer: &dyn Renderer,
    ) -> Result<(), ShowError> {
        self.pipeline.run(writer, renderer).await
    }
}
