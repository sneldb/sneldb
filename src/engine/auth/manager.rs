use crate::engine::auth::db_ops::load_from_db;
use crate::engine::auth::permission_ops::{get_permissions, grant_permission, revoke_permission};
use crate::engine::auth::signature::{parse_auth, verify_signature};
use crate::engine::auth::storage::{AuthStorage, AuthWalStorage};
use crate::engine::auth::types::{
    AuthRateLimiter, AuthResult, PermissionCache, PermissionSet, UserCache, UserKey,
    create_rate_limiter,
};
use crate::engine::auth::user_ops::{
    bootstrap_admin_user, create_user, create_user_with_roles, list_users, revoke_key,
};
use crate::engine::shard::manager::ShardManager;
use crate::shared::config::CONFIG;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

/// Manages authentication (HMAC-SHA256) and authorization (per-event permissions, RBAC).
/// Uses in-memory caches for O(1) lookups and optional rate limiting.
pub struct AuthManager {
    cache: Arc<RwLock<UserCache>>,
    permission_cache: Arc<RwLock<PermissionCache>>,
    _shard_manager: Arc<ShardManager>,
    storage: Arc<dyn AuthStorage>,
    rate_limiter: Option<Arc<Mutex<AuthRateLimiter>>>,
}

impl AuthManager {
    /// Creates a new AuthManager with rate limiting from CONFIG.auth.
    pub fn new(shard_manager: Arc<ShardManager>) -> Self {
        let storage =
            Arc::new(AuthWalStorage::new_default().expect("Failed to initialize auth storage"));
        Self::with_storage(shard_manager, storage)
    }

    /// Creates a new AuthManager with a provided auth storage (useful for tests).
    pub fn with_storage(shard_manager: Arc<ShardManager>, storage: Arc<dyn AuthStorage>) -> Self {
        // Read rate limiting config
        let rate_limiter = CONFIG.auth.as_ref().and_then(|auth_cfg| {
            if auth_cfg.rate_limit_enabled {
                Some(Arc::new(Mutex::new(create_rate_limiter(
                    auth_cfg.rate_limit_per_second,
                ))))
            } else {
                None
            }
        });

        Self {
            cache: Arc::new(RwLock::new(UserCache::new())),
            permission_cache: Arc::new(RwLock::new(PermissionCache::new())),
            _shard_manager: shard_manager,
            storage,
            rate_limiter,
        }
    }

    /// Verifies HMAC signature. Rate limits only failed attempts per IP.
    pub async fn verify_signature(
        &self,
        message: &str,
        user_id: &str,
        signature: &str,
        client_ip: Option<&str>,
    ) -> AuthResult<()> {
        // Verify signature FIRST
        let result = verify_signature(&self.cache, message, user_id, signature).await;

        // Rate limit only failed attempts; successful auths bypass
        if result.is_err() {
            if let (Some(ip), Some(rate_limiter)) = (client_ip, &self.rate_limiter) {
                let limiter = rate_limiter.lock().await;
                if limiter.check_key(&ip.to_string()).is_err() {
                    tracing::warn!(
                        target: "sneldb::auth",
                        user_id,
                        client_ip = ip,
                        "Rate limit exceeded for IP after failed authentication"
                    );
                    return Err(crate::engine::auth::AuthError::RateLimitExceeded);
                }
            }
        }

        result
    }

    /// Parses auth from "user_id:signature:command" format.
    pub fn parse_auth<'a>(&self, input: &'a str) -> AuthResult<(&'a str, &'a str, &'a str)> {
        parse_auth(input)
    }

    /// Creates a new user.
    pub async fn create_user(
        &self,
        user_id: String,
        secret_key: Option<String>,
    ) -> AuthResult<String> {
        create_user(
            &self.cache,
            &self.permission_cache,
            &self.storage,
            user_id,
            secret_key,
        )
        .await
    }

    /// Revokes a user's key.
    pub async fn revoke_key(&self, user_id: &str) -> AuthResult<()> {
        revoke_key(&self.cache, &self.permission_cache, &self.storage, user_id).await
    }

    /// Lists all users
    pub async fn list_users(&self) -> Vec<UserKey> {
        list_users(&self.cache).await
    }

    /// Checks if user is admin.
    pub async fn is_admin(&self, user_id: &str) -> bool {
        let perm_cache = self.permission_cache.read().await;
        perm_cache.is_admin(user_id)
    }

    /// Checks if user can read event_type.
    pub async fn can_read(&self, user_id: &str, event_type: &str) -> bool {
        let perm_cache = self.permission_cache.read().await;
        perm_cache.can_read(user_id, event_type)
    }

    /// Checks if user can write event_type.
    pub async fn can_write(&self, user_id: &str, event_type: &str) -> bool {
        let perm_cache = self.permission_cache.read().await;
        perm_cache.can_write(user_id, event_type)
    }

    /// Grants permissions to a user for an event type.
    pub async fn grant_permission(
        &self,
        user_id: &str,
        event_type: &str,
        permission_set: PermissionSet,
    ) -> AuthResult<()> {
        grant_permission(
            &self.cache,
            &self.permission_cache,
            &self.storage,
            user_id,
            event_type,
            permission_set,
        )
        .await
    }

    /// Revokes permissions from a user for an event type.
    pub async fn revoke_permission(&self, user_id: &str, event_type: &str) -> AuthResult<()> {
        revoke_permission(
            &self.cache,
            &self.permission_cache,
            &self.storage,
            user_id,
            event_type,
        )
        .await
    }

    /// Gets permissions for a user
    pub async fn get_permissions(
        &self,
        user_id: &str,
    ) -> AuthResult<HashMap<String, PermissionSet>> {
        get_permissions(&self.cache, user_id).await
    }

    /// Creates a user with roles.
    pub async fn create_user_with_roles(
        &self,
        user_id: String,
        secret_key: Option<String>,
        roles: Vec<String>,
    ) -> AuthResult<String> {
        create_user_with_roles(
            &self.cache,
            &self.permission_cache,
            &self.storage,
            user_id,
            secret_key,
            roles,
        )
        .await
    }

    /// Bootstraps admin user from config if no users exist.
    pub async fn bootstrap_admin_user(&self) -> AuthResult<()> {
        bootstrap_admin_user(&self.cache, &self.permission_cache, &self.storage).await
    }

    /// Loads users from storage into caches.
    pub async fn load_from_db(&self) -> AuthResult<()> {
        load_from_db(&self.cache, &self.permission_cache, &self.storage).await
    }
}
