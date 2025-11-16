use super::db_ops::store_user_in_db;
use super::types::{
    AuthError, AuthResult, PermissionCache, PermissionSet, User, UserCache, UserKey,
};
use crate::engine::shard::manager::ShardManager;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

/// Updates user caches after permission changes.
///
/// Acquires write locks sequentially to update both caches.
///
/// # Arguments
/// * `cache` - User cache to update
/// * `permission_cache` - Permission cache to update
/// * `updated_key` - The updated user key with new permissions
async fn update_caches(
    cache: &Arc<RwLock<UserCache>>,
    permission_cache: &Arc<RwLock<PermissionCache>>,
    updated_key: UserKey,
) {
    {
        let mut cache_guard = cache.write().await;
        cache_guard.insert(updated_key.clone());
    } // Drop write lock on user cache

    {
        let mut perm_cache_guard = permission_cache.write().await;
        perm_cache_guard.update_user(&updated_key);
    } // Drop write lock on permission cache
}

/// Grants permissions to a user for specific event types.
///
/// # Arguments
/// * `cache` - User cache for authentication lookups
/// * `permission_cache` - Permission cache for authorization checks
/// * `shard_manager` - Shard manager for database operations
/// * `user_id` - The user ID to grant permissions to
/// * `event_type` - The event type to grant permissions for
/// * `permission_set` - The permissions to grant (read/write)
///
/// # Returns
/// `Ok(())` on success
///
/// # Errors
/// - `UserNotFound` if user doesn't exist
/// - `DatabaseError` if database operation fails
pub async fn grant_permission(
    cache: &Arc<RwLock<UserCache>>,
    permission_cache: &Arc<RwLock<PermissionCache>>,
    shard_manager: &Arc<ShardManager>,
    user_id: &str,
    event_type: &str,
    permission_set: PermissionSet,
) -> AuthResult<()> {
    // Use read lock first to get user data
    let user_key = {
        let cache_guard = cache.read().await;
        cache_guard
            .get(user_id)
            .ok_or_else(|| {
                debug!(target: "sneldb::auth", user_id, "User not found during grant");
                AuthError::UserNotFound(user_id.to_string())
            })?
            .clone()
    }; // Drop read lock

    // Update permissions
    let mut updated_permissions = user_key.permissions.clone();
    updated_permissions.insert(event_type.to_string(), permission_set);

    // Create updated user (preserve all existing fields)
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

/// Revokes permissions from a user for specific event types.
///
/// # Arguments
/// * `cache` - User cache for authentication lookups
/// * `permission_cache` - Permission cache for authorization checks
/// * `shard_manager` - Shard manager for database operations
/// * `user_id` - The user ID to revoke permissions from
/// * `event_type` - The event type to revoke permissions for
///
/// # Returns
/// `Ok(())` on success
///
/// # Errors
/// - `UserNotFound` if user doesn't exist
/// - `DatabaseError` if database operation fails
pub async fn revoke_permission(
    cache: &Arc<RwLock<UserCache>>,
    permission_cache: &Arc<RwLock<PermissionCache>>,
    shard_manager: &Arc<ShardManager>,
    user_id: &str,
    event_type: &str,
) -> AuthResult<()> {
    // Use read lock first to get user data
    let user_key = {
        let cache_guard = cache.read().await;
        cache_guard
            .get(user_id)
            .ok_or_else(|| {
                debug!(target: "sneldb::auth", user_id, "User not found during revoke permission");
                AuthError::UserNotFound(user_id.to_string())
            })?
            .clone()
    }; // Drop read lock

    // Remove permission for event_type
    let mut updated_permissions = user_key.permissions.clone();
    updated_permissions.remove(event_type);

    // Create updated user (preserve all existing fields)
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
