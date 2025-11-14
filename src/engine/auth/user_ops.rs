use super::db_ops::store_user_in_db;
use super::types::{AuthError, AuthResult, PermissionCache, User, UserCache, UserKey};
use crate::engine::shard::manager::ShardManager;
use crate::shared::config::CONFIG;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Validates user_id format (alphanumeric, underscore, hyphen)
fn validate_user_id(user_id: &str) -> AuthResult<()> {
    if user_id.is_empty() {
        return Err(AuthError::InvalidUserId);
    }

    if !user_id
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
    {
        return Err(AuthError::InvalidUserId);
    }

    Ok(())
}

/// Generates a random secret key
fn generate_secret_key() -> String {
    use rand::RngCore;
    let mut rng = rand::thread_rng();
    let mut bytes = [0u8; 32];
    rng.fill_bytes(&mut bytes);
    hex::encode(bytes)
}

/// Creates a new user and stores in SnelDB
pub async fn create_user(
    cache: &Arc<RwLock<UserCache>>,
    permission_cache: &Arc<RwLock<PermissionCache>>,
    shard_manager: &Arc<ShardManager>,
    user_id: String,
    secret_key: Option<String>,
) -> AuthResult<String> {
    validate_user_id(&user_id)?;

    // Check if user already exists
    {
        let cache_guard = cache.read().await;
        if cache_guard.get(&user_id).is_some() {
            return Err(AuthError::UserExists(user_id));
        }
    }

    let secret = secret_key.unwrap_or_else(generate_secret_key);

    let created_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let user = User {
        user_id: user_id.clone(),
        secret_key: secret.clone(),
        active: true,
        created_at,
        roles: Vec::new(),
        permissions: HashMap::new(),
    };

    store_user_in_db(shard_manager, &user).await?;

    // Update caches
    {
        let user_key = UserKey::from(user);
        let mut cache_guard = cache.write().await;
        cache_guard.insert(user_key.clone());
        drop(cache_guard);

        let mut perm_cache_guard = permission_cache.write().await;
        perm_cache_guard.update_user(&user_key);
    }

    if tracing::enabled!(tracing::Level::DEBUG) {
        debug!(target: "sneldb::auth", user_id, "User created successfully");
    }
    Ok(secret)
}

/// Creates a user with specified roles
pub async fn create_user_with_roles(
    cache: &Arc<RwLock<UserCache>>,
    permission_cache: &Arc<RwLock<PermissionCache>>,
    shard_manager: &Arc<ShardManager>,
    user_id: String,
    secret_key: Option<String>,
    roles: Vec<String>,
) -> AuthResult<String> {
    validate_user_id(&user_id)?;

    {
        let cache_guard = cache.read().await;
        if cache_guard.get(&user_id).is_some() {
            return Err(AuthError::UserExists(user_id));
        }
    }

    let secret = secret_key.unwrap_or_else(generate_secret_key);

    let created_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let user = User {
        user_id: user_id.clone(),
        secret_key: secret.clone(),
        active: true,
        created_at,
        roles,
        permissions: HashMap::new(),
    };

    store_user_in_db(shard_manager, &user).await?;

    // Update caches
    {
        let user_key = UserKey::from(user);
        let mut cache_guard = cache.write().await;
        cache_guard.insert(user_key.clone());
        drop(cache_guard);

        let mut perm_cache_guard = permission_cache.write().await;
        perm_cache_guard.update_user(&user_key);
    }

    if tracing::enabled!(tracing::Level::DEBUG) {
        debug!(target: "sneldb::auth", user_id, "User created with roles");
    }
    Ok(secret)
}

/// Revokes a user's key (marks as inactive)
pub async fn revoke_key(
    cache: &Arc<RwLock<UserCache>>,
    permission_cache: &Arc<RwLock<PermissionCache>>,
    shard_manager: &Arc<ShardManager>,
    user_id: &str,
) -> AuthResult<()> {
    let cache_guard = cache.write().await;
    let user_key = cache_guard
        .get(user_id)
        .ok_or_else(|| AuthError::UserNotFound(user_id.to_string()))?
        .clone();

    // Update user in DB
    let updated_user = User {
        user_id: user_id.to_string(),
        secret_key: user_key.secret_key.clone(),
        active: false,
        created_at: 0, // Will be updated from DB
        roles: user_key.roles.clone(),
        permissions: user_key.permissions.clone(),
    };

    drop(cache_guard);
    store_user_in_db(shard_manager, &updated_user).await?;

    // Update cache (preserve created_at)
    let mut cache_guard = cache.write().await;
    let mut updated_key = user_key.clone();
    updated_key.active = false;
    cache_guard.insert(updated_key.clone());
    drop(cache_guard);

    // Update permission cache
    let mut perm_cache_guard = permission_cache.write().await;
    perm_cache_guard.update_user(&updated_key);

    info!(target: "sneldb::auth", user_id, "User key revoked");
    Ok(())
}

/// Lists all users
pub async fn list_users(cache: &Arc<RwLock<UserCache>>) -> Vec<UserKey> {
    let cache_guard = cache.read().await;
    cache_guard.all_users().into_iter().cloned().collect()
}

/// Bootstraps admin user from configuration if no users exist
pub async fn bootstrap_admin_user(
    cache: &Arc<RwLock<UserCache>>,
    permission_cache: &Arc<RwLock<PermissionCache>>,
    shard_manager: &Arc<ShardManager>,
) -> AuthResult<()> {
    let user_count = {
        let cache_guard = cache.read().await;
        cache_guard.all_users().len()
    };

    if user_count > 0 {
        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(target: "sneldb::auth", "Users exist, skipping bootstrap");
        }
        return Ok(());
    }

    let Some(auth_config) = &CONFIG.auth else {
        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(target: "sneldb::auth", "No auth config, skipping bootstrap");
        }
        return Ok(());
    };

    let Some(admin_user) = &auth_config.initial_admin_user else {
        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(target: "sneldb::auth", "No initial_admin_user in config, skipping bootstrap");
        }
        return Ok(());
    };

    let Some(admin_key) = &auth_config.initial_admin_key else {
        warn!(target: "sneldb::auth", "initial_admin_user set but initial_admin_key missing, skipping bootstrap");
        return Ok(());
    };

    if tracing::enabled!(tracing::Level::INFO) {
        info!(target: "sneldb::auth", user_id = admin_user, "Bootstrapping admin user from config");
    }

    create_user_with_roles(
        cache,
        permission_cache,
        shard_manager,
        admin_user.clone(),
        Some(admin_key.clone()),
        vec!["admin".to_string()],
    )
    .await?;

    info!(target: "sneldb::auth", user_id = admin_user, "Bootstrap admin user created");
    Ok(())
}

