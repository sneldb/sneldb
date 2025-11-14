use super::types::{AuthError, AuthResult, User};
use crate::engine::core::{Event, EventId};
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use crate::engine::shard::message::ShardMessage;
use std::collections::BTreeMap;
use std::sync::Arc;
use tracing::error;

/// Stores user in SnelDB via STORE command
pub async fn store_user_in_db(
    shard_manager: &Arc<ShardManager>,
    user: &User,
) -> AuthResult<()> {
    let context_id = "__system_auth";
    let shard = shard_manager.get_shard(context_id);

    let mut payload = BTreeMap::new();
    payload.insert(
        "user_id".to_string(),
        serde_json::Value::String(user.user_id.clone()),
    );
    payload.insert(
        "secret_key".to_string(),
        serde_json::Value::String(user.secret_key.clone()),
    );
    payload.insert("active".to_string(), serde_json::Value::Bool(user.active));
    payload.insert(
        "created_at".to_string(),
        serde_json::Value::Number(user.created_at.into()),
    );
    // Store roles as JSON string to match schema
    let roles_json = serde_json::to_string(&user.roles).unwrap_or_else(|_| "[]".to_string());
    payload.insert("roles".to_string(), serde_json::Value::String(roles_json));

    // Store permissions as JSON object
    let permissions_json =
        serde_json::to_string(&user.permissions).unwrap_or_else(|_| "{}".to_string());
    payload.insert(
        "permissions".to_string(),
        serde_json::Value::String(permissions_json),
    );

    let mut event = Event {
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        event_type: "__auth_user".to_string(),
        context_id: context_id.to_string(),
        id: EventId::default(),
        payload: BTreeMap::new(),
    };
    event.set_payload_json(serde_json::to_value(payload).unwrap());

    shard
        .tx
        .send(ShardMessage::Store(
            event,
            Arc::new(tokio::sync::RwLock::new(SchemaRegistry::new().unwrap())),
        ))
        .await
        .map_err(|e| {
            error!(target: "sneldb::auth", error = %e, "Failed to store user in DB");
            AuthError::InvalidSignature // Reuse error type for now
        })?;

    Ok(())
}

/// Loads users from SnelDB
/// This queries __auth_user events from the __system_auth context
pub async fn load_from_db(_shard_manager: &Arc<ShardManager>) -> AuthResult<()> {
    // TODO: Implement full query to load users from all shards
    // For now, this is a placeholder - users will be loaded as they're created
    // In production, you'd want to query across all shards for __auth_user events
    // and populate the cache

    tracing::info!(target: "sneldb::auth", "User loading from DB skipped - users will be loaded on-demand");
    Ok(())
}

