use super::db_ops::store_user_in_db;
use super::types::{
    AuthError, AuthResult, PermissionCache, User, UserCache, UserKey, MAX_SECRET_KEY_LENGTH,
    MAX_USER_ID_LENGTH,
};
use crate::engine::shard::manager::ShardManager;
use crate::shared::config::CONFIG;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Validates user_id format and length.
///
/// # Rules
/// - Must not be empty
/// - Must not exceed MAX_USER_ID_LENGTH characters
/// - Must contain only alphanumeric characters, underscores, or hyphens
///
/// # Arguments
/// * `user_id` - The user ID to validate
///
/// # Returns
/// `Ok(())` if valid, appropriate `AuthError` if invalid
fn validate_user_id(user_id: &str) -> AuthResult<()> {
    if user_id.is_empty() {
        return Err(AuthError::InvalidUserId);
    }

    if user_id.len() > MAX_USER_ID_LENGTH {
        return Err(AuthError::UserIdTooLong {
            max: MAX_USER_ID_LENGTH,
        });
    }

    if !user_id
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
    {
        return Err(AuthError::InvalidUserId);
    }

    Ok(())
}

/// Validates secret key length.
///
/// # Arguments
/// * `secret_key` - The secret key to validate
///
/// # Returns
/// `Ok(())` if valid, appropriate `AuthError` if invalid
fn validate_secret_key(secret_key: &str) -> AuthResult<()> {
    if secret_key.len() > MAX_SECRET_KEY_LENGTH {
        return Err(AuthError::SecretKeyTooLong {
            max: MAX_SECRET_KEY_LENGTH,
        });
    }
    Ok(())
}

/// Generates a cryptographically secure random secret key.
///
/// Returns a 64-character hexadecimal string (32 bytes of randomness).
/// Uses the system's CSPRNG via `rand::thread_rng()`.
///
/// # Returns
/// A hex-encoded random secret key suitable for HMAC-SHA256
fn generate_secret_key() -> String {
    use rand::RngCore;
    let mut rng = rand::thread_rng();
    let mut bytes = [0u8; 32];
    rng.fill_bytes(&mut bytes);
    hex::encode(bytes)
}

/// Creates a new user and stores in SnelDB.
///
/// This is a convenience wrapper around `create_user_with_roles` with no roles.
///
/// # Arguments
/// * `cache` - User cache for authentication lookups
/// * `permission_cache` - Permission cache for authorization checks
/// * `shard_manager` - Shard manager for database operations
/// * `user_id` - The unique user identifier
/// * `secret_key` - Optional secret key (generated if not provided)
///
/// # Returns
/// The secret key (generated or provided) on success
pub async fn create_user(
    cache: &Arc<RwLock<UserCache>>,
    permission_cache: &Arc<RwLock<PermissionCache>>,
    shard_manager: &Arc<ShardManager>,
    user_id: String,
    secret_key: Option<String>,
) -> AuthResult<String> {
    create_user_with_roles(cache, permission_cache, shard_manager, user_id, secret_key, Vec::new()).await
}

/// Creates a user with specified roles and stores in SnelDB.
///
/// # Arguments
/// * `cache` - User cache for authentication lookups
/// * `permission_cache` - Permission cache for authorization checks
/// * `shard_manager` - Shard manager for database operations
/// * `user_id` - The unique user identifier
/// * `secret_key` - Optional secret key (generated if not provided)
/// * `roles` - List of roles to assign to the user (e.g., ["admin"])
///
/// # Returns
/// The secret key (generated or provided) on success
///
/// # Errors
/// - `InvalidUserId` if user_id format is invalid
/// - `UserIdTooLong` if user_id exceeds maximum length
/// - `SecretKeyTooLong` if provided secret_key exceeds maximum length
/// - `UserExists` if user already exists (doesn't leak user_id in error)
/// - `DatabaseError` if database operation fails
pub async fn create_user_with_roles(
    cache: &Arc<RwLock<UserCache>>,
    permission_cache: &Arc<RwLock<PermissionCache>>,
    shard_manager: &Arc<ShardManager>,
    user_id: String,
    secret_key: Option<String>,
    roles: Vec<String>,
) -> AuthResult<String> {
    // Validate user_id
    validate_user_id(&user_id)?;

    // Generate or validate secret key
    let secret = match secret_key {
        Some(key) => {
            validate_secret_key(&key)?;
            key
        }
        None => generate_secret_key(),
    };

    // Check if user already exists (use read lock)
    {
        let cache_guard = cache.read().await;
        if cache_guard.get(&user_id).is_some() {
            // Don't leak user_id in error message to prevent enumeration
            debug!(target: "sneldb::auth", user_id, "User already exists");
            return Err(AuthError::UserExists);
        }
    } // Drop read lock

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

    // Store in database first
    store_user_in_db(shard_manager, &user).await?;

    // Update caches (now acquire write locks)
    let user_key = UserKey::from(user);
    {
        let mut cache_guard = cache.write().await;
        cache_guard.insert(user_key.clone());
    } // Drop write lock on user cache

    {
        let mut perm_cache_guard = permission_cache.write().await;
        perm_cache_guard.update_user(&user_key);
    } // Drop write lock on permission cache

    if tracing::enabled!(tracing::Level::DEBUG) {
        debug!(target: "sneldb::auth", user_id, "User created with roles");
    }
    Ok(secret)
}

/// Revokes a user's key by marking it as inactive.
///
/// The user record remains in the system for audit purposes but can no longer authenticate.
///
/// # Arguments
/// * `cache` - User cache for authentication lookups
/// * `permission_cache` - Permission cache for authorization checks
/// * `shard_manager` - Shard manager for database operations
/// * `user_id` - The user ID whose key should be revoked
///
/// # Returns
/// `Ok(())` on success
///
/// # Errors
/// - Returns generic `AuthenticationFailed` if user not found (to prevent enumeration)
/// - `DatabaseError` if database operation fails
pub async fn revoke_key(
    cache: &Arc<RwLock<UserCache>>,
    permission_cache: &Arc<RwLock<PermissionCache>>,
    shard_manager: &Arc<ShardManager>,
    user_id: &str,
) -> AuthResult<()> {
    // Use read lock first to get user data
    let user_key = {
        let cache_guard = cache.read().await;
        cache_guard
            .get(user_id)
            .ok_or_else(|| {
                debug!(target: "sneldb::auth", user_id, "User not found during revoke");
                AuthError::UserNotFound(user_id.to_string())
            })?
            .clone()
    }; // Drop read lock

    // Update user in DB
    let updated_user = User {
        user_id: user_id.to_string(),
        secret_key: user_key.secret_key.clone(),
        active: false,
        created_at: user_key.created_at, // Preserve actual timestamp
        roles: user_key.roles.clone(),
        permissions: user_key.permissions.clone(),
    };

    store_user_in_db(shard_manager, &updated_user).await?;

    // Update user cache (acquire write lock)
    let updated_key = UserKey {
        user_id: user_id.to_string(),
        secret_key: user_key.secret_key.clone(),
        active: false,
        created_at: user_key.created_at,
        roles: user_key.roles.clone(),
        permissions: user_key.permissions.clone(),
    };

    {
        let mut cache_guard = cache.write().await;
        cache_guard.insert(updated_key.clone());
    } // Drop write lock on user cache

    // Update permission cache
    {
        let mut perm_cache_guard = permission_cache.write().await;
        perm_cache_guard.update_user(&updated_key);
    } // Drop write lock on permission cache

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

