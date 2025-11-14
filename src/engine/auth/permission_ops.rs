use super::db_ops::store_user_in_db;
use super::types::{AuthError, AuthResult, PermissionCache, PermissionSet, User, UserCache, UserKey};
use crate::engine::shard::manager::ShardManager;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

/// Updates user caches after permission changes
async fn update_caches(
    cache: &Arc<RwLock<UserCache>>,
    permission_cache: &Arc<RwLock<PermissionCache>>,
    updated_key: UserKey,
) {
    let mut cache_guard = cache.write().await;
    cache_guard.insert(updated_key.clone());
    drop(cache_guard);

    let mut perm_cache_guard = permission_cache.write().await;
    perm_cache_guard.update_user(&updated_key);
}

/// Grants permissions to a user for specific event types
pub async fn grant_permission(
    cache: &Arc<RwLock<UserCache>>,
    permission_cache: &Arc<RwLock<PermissionCache>>,
    shard_manager: &Arc<ShardManager>,
    user_id: &str,
    event_type: &str,
    permission_set: PermissionSet,
) -> AuthResult<()> {
    let cache_guard = cache.write().await;
    let user_key = cache_guard
        .get(user_id)
        .ok_or_else(|| AuthError::UserNotFound(user_id.to_string()))?
        .clone();
    drop(cache_guard);

    // Update permissions
    let mut updated_permissions = user_key.permissions.clone();
    updated_permissions.insert(event_type.to_string(), permission_set);

    // Create updated user (preserve created_at from cache)
    let updated_user = User {
        user_id: user_id.to_string(),
        secret_key: user_key.secret_key.clone(),
        active: user_key.active,
        created_at: user_key.created_at,
        roles: user_key.roles.clone(),
        permissions: updated_permissions.clone(),
    };

    store_user_in_db(shard_manager, &updated_user).await?;

    // Update caches
    let updated_key = UserKey {
        user_id: user_id.to_string(),
        secret_key: user_key.secret_key.clone(),
        active: user_key.active,
        created_at: user_key.created_at,
        roles: user_key.roles.clone(),
        permissions: updated_permissions,
    };
    update_caches(cache, permission_cache, updated_key).await;

    if tracing::enabled!(tracing::Level::DEBUG) {
        debug!(
            target: "sneldb::auth",
            user_id,
            event_type,
            "Permissions granted"
        );
    }
    Ok(())
}

/// Revokes permissions from a user for specific event types
pub async fn revoke_permission(
    cache: &Arc<RwLock<UserCache>>,
    permission_cache: &Arc<RwLock<PermissionCache>>,
    shard_manager: &Arc<ShardManager>,
    user_id: &str,
    event_type: &str,
) -> AuthResult<()> {
    let cache_guard = cache.write().await;
    let user_key = cache_guard
        .get(user_id)
        .ok_or_else(|| AuthError::UserNotFound(user_id.to_string()))?
        .clone();
    drop(cache_guard);

    // Remove permission for event_type
    let mut updated_permissions = user_key.permissions.clone();
    updated_permissions.remove(event_type);

    // Create updated user (preserve created_at from cache)
    let updated_user = User {
        user_id: user_id.to_string(),
        secret_key: user_key.secret_key.clone(),
        active: user_key.active,
        created_at: user_key.created_at,
        roles: user_key.roles.clone(),
        permissions: updated_permissions.clone(),
    };

    store_user_in_db(shard_manager, &updated_user).await?;

    // Update caches
    let updated_key = UserKey {
        user_id: user_id.to_string(),
        secret_key: user_key.secret_key.clone(),
        active: user_key.active,
        created_at: user_key.created_at,
        roles: user_key.roles.clone(),
        permissions: updated_permissions,
    };
    update_caches(cache, permission_cache, updated_key).await;

    if tracing::enabled!(tracing::Level::DEBUG) {
        debug!(
            target: "sneldb::auth",
            user_id,
            event_type,
            "Permissions revoked"
        );
    }
    Ok(())
}

/// Gets permissions for a user
pub async fn get_permissions(
    cache: &Arc<RwLock<UserCache>>,
    user_id: &str,
) -> AuthResult<HashMap<String, PermissionSet>> {
    let cache_guard = cache.read().await;
    let user_key = cache_guard
        .get(user_id)
        .ok_or_else(|| AuthError::UserNotFound(user_id.to_string()))?;
    Ok(user_key.permissions.clone())
}

