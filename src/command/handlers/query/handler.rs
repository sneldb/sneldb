use std::io;
use std::sync::Arc;

use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::command::types::Command;
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use crate::shared::response::render::Renderer;
use crate::shared::response::{Response, StatusCode};

use super::orchestrator::QueryExecutionPipeline;
use super::response::QueryResponseFormatter;
use super::streaming::{QueryResponseWriter, QueryStreamingConfig};

use tokio::sync::RwLock;
use tracing::{debug, warn};

pub struct QueryCommandHandler<'a, W: AsyncWrite + Unpin> {
    command: &'a Command,
    shard_manager: &'a ShardManager,
    registry: Arc<RwLock<SchemaRegistry>>,
    writer: &'a mut W,
    renderer: &'a dyn Renderer,
}

impl<'a, W: AsyncWrite + Unpin> QueryCommandHandler<'a, W> {
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
        let Command::Query {
            event_type,
            context_id,
            since,
            limit,
            offset,
            where_clause,
            ..
        } = self.command
        else {
            warn!(target: "sneldb::query", "Invalid Query command received");
            return self
                .write_error(StatusCode::BadRequest, "Invalid Query command")
                .await;
        };

        if event_type.trim().is_empty() {
            warn!(target: "sneldb::query", "Empty event_type in Query command");
            return self
                .write_error(StatusCode::BadRequest, "event_type cannot be empty")
                .await;
        }

        if offset.is_some() && limit.is_none() {
            warn!(target: "sneldb::query", "OFFSET specified without LIMIT");
            return self
                .write_error(
                    StatusCode::BadRequest,
                    "OFFSET requires LIMIT to prevent unbounded results",
                )
                .await;
        }

        debug!(
            target: "sneldb::query",
            event_type,
            context_id = context_id.as_deref().unwrap_or("<none>"),
            since = ?since,
            limit = ?limit,
            has_filter = where_clause.is_some(),
            "Dispatching Query command to pipeline"
        );

        let pipeline = QueryExecutionPipeline::new(
            self.command,
            self.shard_manager,
            Arc::clone(&self.registry),
        );

        let limit_value = *limit;
        let offset_value = *offset;

        if QueryStreamingConfig::enabled() && pipeline.streaming_supported() {
            match pipeline.execute_streaming().await {
                Ok(Some(stream)) => {
                    let response_writer = QueryResponseWriter::new(
                        self.writer,
                        self.renderer,
                        stream.schema(),
                        limit_value,
                        offset_value,
                    );
                    return response_writer.write(stream).await;
                }
                Ok(None) => {}
                Err(error) => {
                    warn!(
                        target: "sneldb::query",
                        error = %error,
                        "Streaming execution failed, falling back to buffered"
                    );
                }
            }
        }

        // Only create formatter when we actually need it (non-streaming path)
        let mut formatter = QueryResponseFormatter::new(self.renderer);
        match pipeline.execute().await {
            Ok(result) => formatter.write(self.writer, result).await,
            Err(error) => {
                warn!(
                    target: "sneldb::query",
                    error = %error,
                    "Query execution failed"
                );
                self.write_error(StatusCode::InternalError, &format!("Query failed: {error}"))
                    .await
            }
        }
    }

    async fn write_error(&mut self, status: StatusCode, message: &str) -> io::Result<()> {
        let response = Response::error(status, message);
        self.writer
            .write_all(&self.renderer.render(&response))
            .await
    }
}
