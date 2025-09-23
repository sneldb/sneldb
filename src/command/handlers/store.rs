use crate::command::types::Command;
use crate::engine::core::Event;
use crate::engine::schema::FieldType;
use crate::engine::schema::SchemaRegistry;
use crate::engine::schema::registry::MiniSchema;
use crate::engine::shard::manager::ShardManager;
use crate::engine::shard::message::ShardMessage;
use crate::shared::response::render::Renderer;
use crate::shared::response::{Response, StatusCode};

use std::collections::HashSet;
use std::sync::Arc;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::RwLock;
use tokio::time::{Duration, timeout};
use tracing::{debug, error, info, warn};

/// Handle a `Store` command. Validates schema, dispatches to shard.
pub async fn handle<W: AsyncWrite + Unpin>(
    cmd: &Command,
    shard_manager: &ShardManager,
    registry: &Arc<RwLock<SchemaRegistry>>,
    writer: &mut W,
    renderer: &dyn Renderer,
) -> std::io::Result<()> {
    let Command::Store {
        event_type,
        context_id,
        payload,
    } = cmd
    else {
        warn!(target: "sneldb::store", "Received invalid command variant for Store");
        return write_error(
            writer,
            renderer,
            StatusCode::BadRequest,
            "Invalid command variant",
        )
        .await;
    };

    if event_type.trim().is_empty() {
        warn!(target: "sneldb::store", "Missing event_type");
        return write_error(
            writer,
            renderer,
            StatusCode::BadRequest,
            "event_type cannot be empty",
        )
        .await;
    }

    if context_id.trim().is_empty() {
        warn!(target: "sneldb::store", "Missing context_id");
        return write_error(
            writer,
            renderer,
            StatusCode::BadRequest,
            "context_id cannot be empty",
        )
        .await;
    }

    let registry_clone = Arc::clone(registry);
    let schema_read = registry.read().await;

    let Some(mini_schema) = schema_read.get(event_type) else {
        warn!(
            target: "sneldb::store",
            event_type,
            "No schema defined for event_type"
        );
        return write_error(
            writer,
            renderer,
            StatusCode::BadRequest,
            &format!("No schema defined for event type '{}'", event_type),
        )
        .await;
    };

    if let Err(e) = validate_payload(payload, mini_schema) {
        warn!(
            target: "sneldb::store",
            event_type,
            context_id,
            error = %e,
            "Payload validation failed"
        );
        return write_error(writer, renderer, StatusCode::BadRequest, &e).await;
    }

    let event = Event {
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        event_type: event_type.clone(),
        context_id: context_id.clone(),
        payload: payload.clone(),
    };

    let shard = shard_manager.get_shard(context_id);
    debug!(
        target: "sneldb::store",
        shard_id = shard.id,
        context_id,
        "Routing event to shard"
    );

    let send_result = timeout(
        Duration::from_millis(1000),
        shard.tx.send(ShardMessage::Store(event, registry_clone)),
    )
    .await;

    match send_result {
        Ok(Ok(())) => {
            info!(
                target: "sneldb::store",
                shard_id = shard.id,
                context_id,
                "Event accepted and routed to shard"
            );
            write_ok(writer, renderer, "Event accepted for storage").await
        }
        Ok(Err(e)) => {
            error!(
                target: "sneldb::store",
                shard_id = shard.id,
                context_id,
                error = %e,
                "Failed to send Store message"
            );
            write_error(
                writer,
                renderer,
                StatusCode::InternalError,
                "Failed to route store command",
            )
            .await
        }
        Err(_) => {
            error!(
                target: "sneldb::store",
                shard_id = shard.id,
                context_id,
                "Timed out sending Store message"
            );
            write_error(
                writer,
                renderer,
                StatusCode::InternalError,
                "Shard is busy, try again later",
            )
            .await
        }
    }
}

fn type_allows_value(ft: &FieldType, v: &serde_json::Value) -> bool {
    match ft {
        FieldType::String => v.is_string(),
        FieldType::U64 => v.as_u64().is_some(),
        FieldType::I64 => v.as_i64().is_some(),
        FieldType::F64 => v.as_f64().is_some(),
        FieldType::Bool => v.is_boolean(),
        FieldType::Optional(inner) => v.is_null() || type_allows_value(inner, v),
        FieldType::Enum(enum_ty) => v
            .as_str()
            .map(|s| enum_ty.variants.iter().any(|vv| vv == s))
            .unwrap_or(false),
    }
}

/// Validates that a JSON payload matches the expected MiniSchema.
fn validate_payload(payload: &serde_json::Value, schema: &MiniSchema) -> Result<(), String> {
    let obj = payload
        .as_object()
        .ok_or_else(|| "Payload must be a JSON object".to_string())?;

    for (field, field_type) in &schema.fields {
        match obj.get(field) {
            Some(value) => {
                if !type_allows_value(field_type, value) {
                    return Err(format!("Field '{}' does not match expected type", field));
                }
            }
            None => {
                if !matches!(field_type, FieldType::Optional(_)) {
                    return Err(format!("Missing field '{}' in payload", field));
                }
            }
        }
    }

    let allowed_keys: HashSet<_> = schema.fields.keys().collect();
    let actual_keys: HashSet<_> = obj.keys().collect();

    let extra_keys: Vec<_> = actual_keys.difference(&allowed_keys).cloned().collect();
    if !extra_keys.is_empty() {
        return Err(format!(
            "Payload contains fields not defined in schema: {}",
            extra_keys
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    Ok(())
}

/// Writes an error response to the writer.
async fn write_error<W: AsyncWrite + Unpin>(
    writer: &mut W,
    renderer: &dyn Renderer,
    status: StatusCode,
    message: &str,
) -> std::io::Result<()> {
    let resp = Response::error(status, message.to_string());
    writer.write_all(&renderer.render(&resp)).await
}

/// Writes a success response to the writer.
async fn write_ok<W: AsyncWrite + Unpin>(
    writer: &mut W,
    renderer: &dyn Renderer,
    message: &str,
) -> std::io::Result<()> {
    let resp = Response::ok_lines(vec![message.to_string()]);
    writer.write_all(&renderer.render(&resp)).await
}
