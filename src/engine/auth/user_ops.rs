use super::db_ops::store_user_in_db;
use super::storage::AuthStorage;
use super::types::{
    AuthError, AuthResult, MAX_SECRET_KEY_LENGTH, MAX_USER_ID_LENGTH, PermissionCache, User,
    UserCache, UserKey,
};
use crate::shared::config::CONFIG;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Validates user_id: non-empty, alphanumeric/underscore/hyphen, max MAX_USER_ID_LENGTH.
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

/// Validates secret key length (max MAX_SECRET_KEY_LENGTH).
fn validate_secret_key(secret_key: &str) -> AuthResult<()> {
    if secret_key.len() > MAX_SECRET_KEY_LENGTH {
        return Err(AuthError::SecretKeyTooLong {
            max: MAX_SECRET_KEY_LENGTH,
        });
    }
    Ok(())
}

/// Generates a 64-char hex secret key (32 random bytes) for HMAC-SHA256.
fn generate_secret_key() -> String {
    use rand::RngCore;
    let mut rng = rand::thread_rng();
    let mut bytes = [0u8; 32];
    rng.fill_bytes(&mut bytes);
    hex::encode(bytes)
}

/// Creates a new user (wrapper around `create_user_with_roles` with no roles).
pub async fn create_user(
    cache: &Arc<RwLock<UserCache>>,
    permission_cache: &Arc<RwLock<PermissionCache>>,
    auth_storage: &Arc<dyn AuthStorage>,
    user_id: String,
    secret_key: Option<String>,
) -> AuthResult<String> {
    create_user_with_roles(
        cache,
        permission_cache,
        auth_storage,
        user_id,
        secret_key,
        Vec::new(),
    )
    .await
}

/// Creates a user with roles. Returns secret key (generated or provided).
pub async fn create_user_with_roles(
    cache: &Arc<RwLock<UserCache>>,
    permission_cache: &Arc<RwLock<PermissionCache>>,
    auth_storage: &Arc<dyn AuthStorage>,
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

    // Check if user already exists
    {
        let cache_guard = cache.read().await;
        if cache_guard.get(&user_id).is_some() {
            // Don't leak user_id to prevent enumeration
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
    store_user_in_db(auth_storage, &user).await?;

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

/// Revokes a user's key (marks inactive). User record kept for audit.
/// Note: This function only marks the user as inactive. To also revoke session tokens,
/// use AuthManager::revoke_key() which calls this function and handles session revocation.
pub async fn revoke_key(
    cache: &Arc<RwLock<UserCache>>,
    permission_cache: &Arc<RwLock<PermissionCache>>,
    auth_storage: &Arc<dyn AuthStorage>,
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
        created_at: user_key.created_at, // Preserve timestamp
        roles: user_key.roles.clone(),
        permissions: user_key.permissions.clone(),
    };

    store_user_in_db(auth_storage, &updated_user).await?;

    // Update user cache
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

/// Bootstraps admin user from config if it doesn't exist in cache or database.
pub async fn bootstrap_admin_user(
    cache: &Arc<RwLock<UserCache>>,
    permission_cache: &Arc<RwLock<PermissionCache>>,
    auth_storage: &Arc<dyn AuthStorage>,
) -> AuthResult<()> {
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

    // Check if the specific admin user already exists in cache
    let admin_exists_in_cache = {
        let cache_guard = cache.read().await;
        cache_guard.get(admin_user).is_some()
    };

    if admin_exists_in_cache {
        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(target: "sneldb::auth", user_id = admin_user, "Admin user exists in cache, skipping bootstrap");
        }
        return Ok(());
    }

    // Check if the admin user exists in the database
    // This handles the case where the user exists in DB but cache wasn't loaded yet
    let users_from_db = auth_storage.load_users()?;
    let admin_exists_in_db = users_from_db
        .iter()
        .any(|stored| stored.user.user_id == *admin_user);

    if admin_exists_in_db {
        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(target: "sneldb::auth", user_id = admin_user, "Admin user exists in database, skipping bootstrap");
        }
        return Ok(());
    }

    if tracing::enabled!(tracing::Level::INFO) {
        info!(target: "sneldb::auth", user_id = admin_user, "Bootstrapping admin user from config");
    }

    // Create the admin user since it doesn't exist in cache or database
    match create_user_with_roles(
        cache,
        permission_cache,
        auth_storage,
        admin_user.clone(),
        Some(admin_key.clone()),
        vec!["admin".to_string()],
    )
    .await
    {
        Ok(_) => {
            info!(target: "sneldb::auth", user_id = admin_user, "Bootstrap admin user created");
            Ok(())
        }
        Err(AuthError::UserExists) => {
            // User exists in cache (shouldn't happen due to check above, but handle gracefully)
            if tracing::enabled!(tracing::Level::DEBUG) {
                debug!(target: "sneldb::auth", user_id = admin_user, "Admin user already exists, skipping bootstrap");
            }
            Ok(())
        }
        Err(e) => Err(e),
    }
}
