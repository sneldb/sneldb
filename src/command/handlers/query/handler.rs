use std::io;
use std::sync::Arc;

use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::command::types::Command;
use crate::engine::auth::{AuthManager, BYPASS_USER_ID};
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
    auth_manager: Option<&'a Arc<AuthManager>>,
    user_id: Option<&'a str>,
    writer: &'a mut W,
    renderer: &'a dyn Renderer,
}

impl<'a, W: AsyncWrite + Unpin> QueryCommandHandler<'a, W> {
    pub fn new(
        command: &'a Command,
        shard_manager: &'a ShardManager,
        registry: Arc<RwLock<SchemaRegistry>>,
        auth_manager: Option<&'a Arc<AuthManager>>,
        user_id: Option<&'a str>,
        writer: &'a mut W,
        renderer: &'a dyn Renderer,
    ) -> Self {
        Self {
            command,
            shard_manager,
            registry,
            auth_manager,
            user_id,
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

        // Check read permission if auth is enabled
        // Skip permission check if user_id is "bypass" (bypass_auth mode)
        if let Some(auth_mgr) = self.auth_manager {
            if let Some(uid) = self.user_id {
                // Skip permission checks for bypass user
                if uid != BYPASS_USER_ID && !auth_mgr.can_read(uid, event_type).await {
                    warn!(
                        target: "sneldb::query",
                        user_id = uid,
                        event_type,
                        "Read permission denied"
                    );
                    return self
                        .write_error(
                            StatusCode::Forbidden,
                            &format!("Read permission denied for event type '{}'", event_type),
                        )
                        .await;
                }
            } else {
                // Authentication required but no user_id provided
                warn!(target: "sneldb::query", "Authentication required for QUERY command");
                return self
                    .write_error(StatusCode::Unauthorized, "Authentication required")
                    .await;
            }
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

        // Aggregate queries must use streaming path - fail early if streaming is disabled
        if let Command::Query { aggs: Some(_), .. } = self.command {
            if !QueryStreamingConfig::enabled() {
                return self
                    .write_error(
                        StatusCode::InternalError,
                        "Aggregate queries require streaming execution (streaming is disabled)",
                    )
                    .await;
            }
        }

        if QueryStreamingConfig::enabled() && pipeline.streaming_supported() {
            match pipeline.execute_streaming().await {
                Ok(Some(stream)) => {
                    // For sequence queries, the limit is already applied at the sequence matcher level
                    // (limiting sequences, not events). We should not apply it again here to events.
                    // For ordered queries, limit and offset are already applied in the flow merger (OrderedStreamMerger),
                    // so we should not apply them again in QueryResponseWriter to avoid double-application.
                    // For unordered queries, limit and offset need to be applied in QueryResponseWriter.
                    let (response_limit, response_offset) = if pipeline.is_sequence_query() {
                        (None, None) // Sequence queries handle limits at matcher level
                    } else if let Command::Query {
                        order_by: Some(_), ..
                    } = self.command
                    {
                        (None, None) // Offset and limit already applied in flow merger for ordered queries
                    } else {
                        (limit_value, offset_value) // Apply limit and offset in response writer for unordered queries
                    };
                    let response_writer = QueryResponseWriter::new(
                        self.writer,
                        self.renderer,
                        stream.schema(),
                        response_limit,
                        response_offset,
                    );
                    return response_writer.write(stream).await;
                }
                Ok(None) => {
                    // Streaming not available - check if this is an aggregate query
                    if let Command::Query { aggs: Some(_), .. } = self.command {
                        return self
                            .write_error(
                                StatusCode::InternalError,
                                "Aggregate queries require streaming execution",
                            )
                            .await;
                    }
                }
                Err(error) => {
                    // Check if this is a validation error (WHERE clause ambiguity)
                    // Validation errors should return BadRequest, not InternalError
                    if error.contains("WHERE clause validation failed")
                        || error.contains("Ambiguous field")
                    {
                        warn!(
                            target: "sneldb::query",
                            error = %error,
                            "Query validation failed"
                        );
                        return self.write_error(StatusCode::BadRequest, &error).await;
                    }
                    // For aggregate queries, don't fall back to batch - fail fast
                    if let Command::Query { aggs: Some(_), .. } = self.command {
                        warn!(
                            target: "sneldb::query",
                            error = %error,
                            "Streaming execution failed for aggregate query"
                        );
                        return self
                            .write_error(
                                StatusCode::InternalError,
                                &format!("Aggregate query failed: {error}"),
                            )
                            .await;
                    }
                    warn!(
                        target: "sneldb::query",
                        error = %error,
                        "Streaming execution failed, falling back to buffered"
                    );
                }
            }
        }

        // Only create formatter when we actually need it (non-streaming path)
        // Aggregate queries should never reach here - they're rejected above
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
