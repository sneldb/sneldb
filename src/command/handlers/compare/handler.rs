use std::io;
use std::sync::Arc;

use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::command::types::Command;
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use crate::shared::response::render::Renderer;
use crate::shared::response::{Response, StatusCode};

use super::orchestrator::ComparisonExecutionPipeline;
use crate::command::handlers::query::QueryResponseWriter;

use tokio::sync::RwLock;
use tracing::{debug, warn};

pub struct ComparisonCommandHandler<'a, W: AsyncWrite + Unpin> {
    command: &'a Command,
    shard_manager: &'a ShardManager,
    registry: Arc<RwLock<SchemaRegistry>>,
    writer: &'a mut W,
    renderer: &'a dyn Renderer,
}

impl<'a, W: AsyncWrite + Unpin> ComparisonCommandHandler<'a, W> {
    pub fn new(
        command: &'a Command,
        shard_manager: &'a ShardManager,
        registry: Arc<RwLock<SchemaRegistry>>,
        writer: &'a mut W,
        renderer: &'a dyn Renderer,
    ) -> Self {
        Self {
            command,
            shard_manager,
            registry,
            writer,
            renderer,
        }
    }

    pub async fn handle(mut self) -> io::Result<()> {
        let Command::Compare { queries } = self.command else {
            warn!(target: "sneldb::compare", "Invalid Compare command received");
            return self
                .write_error(StatusCode::BadRequest, "Invalid Compare command")
                .await;
        };

        if queries.len() < 2 {
            warn!(
                target: "sneldb::compare",
                query_count = queries.len(),
                "Comparison query requires at least 2 queries"
            );
            return self
                .write_error(
                    StatusCode::BadRequest,
                    "Comparison query requires at least 2 queries",
                )
                .await;
        }

        debug!(
            target: "sneldb::compare",
            query_count = queries.len(),
            "Dispatching Comparison command to pipeline"
        );

        let pipeline = ComparisonExecutionPipeline::new(
            queries,
            self.shard_manager,
            Arc::clone(&self.registry),
        );

        match pipeline.execute_streaming().await {
            Ok(Some(stream)) => {
                let response_writer = QueryResponseWriter::new(
                    self.writer,
                    self.renderer,
                    stream.schema(),
                    None, // No limit for comparison queries
                    None, // No offset for comparison queries
                );
                response_writer.write(stream).await
            }
            Ok(None) => {
                warn!(
                    target: "sneldb::compare",
                    "Streaming not available for comparison query"
                );
                self.write_error(
                    StatusCode::InternalError,
                    "Comparison queries require streaming execution",
                )
                .await
            }
            Err(error) => {
                warn!(
                    target: "sneldb::compare",
                    error = %error,
                    "Comparison query execution failed"
                );
                self.write_error(StatusCode::InternalError, &error).await
            }
        }
    }

    async fn write_error(&mut self, status: StatusCode, message: &str) -> io::Result<()> {
        let resp = Response::error(status, message);
        self.writer.write_all(&self.renderer.render(&resp)).await
    }
}
