use crate::engine::schema::errors::SchemaError;
use crate::engine::schema::registry::{MiniSchema as EngineMiniSchema, SchemaRegistry};
use tracing::info;

/// Defines and registers a schema for a specific event type.
/// This assumes the schema has been validated and is ready to persist.
pub async fn define_schema(
    registry: &mut SchemaRegistry,
    event_type: &str,
    _version: u32,
    schema: EngineMiniSchema,
) -> Result<(), SchemaError> {
    info!("Defining schema for event_type '{}'", event_type);
    registry.define_async(event_type, schema).await
}
