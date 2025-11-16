use super::types::{AuthError, AuthResult, PermissionCache, User, UserCache};
use crate::engine::core::{Event, EventId};
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use crate::engine::shard::message::ShardMessage;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::error;

/// Stores user in SnelDB via STORE command
pub async fn store_user_in_db(shard_manager: &Arc<ShardManager>, user: &User) -> AuthResult<()> {
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
            AuthError::DatabaseError(format!("Failed to store user: {}", e))
        })?;

    Ok(())
}

/// Loads users from SnelDB by querying __auth_user events.
///
/// # Implementation Notes
///
/// This function needs to:
/// 1. Query __auth_user events from the __system_auth context across all shards
/// 2. Parse each event payload to reconstruct User objects
/// 3. Populate both UserCache and PermissionCache
/// 4. Handle edge cases:
///    - Missing or corrupted user data
///    - Schema version changes
///    - Conflicting user records (use latest by timestamp)
///
/// # Current Status
///
/// **NOT IMPLEMENTED** - This is a critical TODO for production use.
/// Without this, all users are lost from cache on server restart,
/// requiring them to be recreated from scratch.
///
/// # Implementation Example
///
/// ```ignore
/// // Pseudo-code for implementation:
/// // 1. Create a Query command for __auth_user events
/// // 2. Execute query across all shards
/// // 3. For each event, deserialize user data
/// // 4. Populate caches
/// ```
///
/// # Errors
///
/// Currently returns Ok(()) but should return DatabaseError if query fails.
pub async fn load_from_db(
    _cache: &Arc<RwLock<UserCache>>,
    _permission_cache: &Arc<RwLock<PermissionCache>>,
    _shard_manager: &Arc<ShardManager>,
) -> AuthResult<()> {
    tracing::warn!(
        target: "sneldb::auth",
        "load_from_db NOT IMPLEMENTED - users will not persist across restarts"
    );
    // TODO: Implement user loading from database
    // See function documentation for implementation details
    Ok(())
}
