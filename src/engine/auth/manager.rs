use crate::engine::auth::db_ops::load_from_db;
use crate::engine::auth::permission_ops::{get_permissions, grant_permission, revoke_permission};
use crate::engine::auth::signature::{parse_auth, verify_signature};
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

/// Manages user authentication and authorization.
///
/// The AuthManager provides:
/// - User authentication via HMAC-SHA256 signatures
/// - Authorization with fine-grained permissions per event type
/// - Role-based access control (RBAC) with admin role
/// - Configurable rate limiting to prevent brute-force attacks
/// - In-memory caching for O(1) authentication and authorization
///
/// # Architecture
/// - `UserCache`: Fast lookup of user credentials
/// - `PermissionCache`: O(1) permission checks with separate admin tracking
/// - `AuthRateLimiter`: Token bucket rate limiting for auth attempts (optional)
/// - Persistence via SnelDB's internal `__auth_user` events
pub struct AuthManager {
    cache: Arc<RwLock<UserCache>>,
    permission_cache: Arc<RwLock<PermissionCache>>,
    shard_manager: Arc<ShardManager>,
    rate_limiter: Option<Arc<Mutex<AuthRateLimiter>>>,
}

impl AuthManager {
    /// Creates a new AuthManager.
    ///
    /// Rate limiting configuration is read from `CONFIG.auth`:
    /// - `rate_limit_enabled`: Enable/disable rate limiting (default: true)
    /// - `rate_limit_per_second`: Max auth attempts per second (default: 10)
    ///
    /// # Arguments
    /// * `shard_manager` - The shard manager for database operations
    ///
    /// # Returns
    /// A new AuthManager with empty caches and configured rate limiting
    pub fn new(shard_manager: Arc<ShardManager>) -> Self {
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
            shard_manager,
            rate_limiter,
        }
    }

    /// Verifies HMAC signature for a message with optional per-IP rate limiting.
    ///
    /// # Arguments
    /// * `message` - The message that was signed
    /// * `user_id` - The user ID claiming to have signed the message
    /// * `signature` - The HMAC signature to verify
    /// * `client_ip` - Optional client IP address for rate limiting (None = skip rate limit)
    ///
    /// # Returns
    /// `Ok(())` if signature is valid, appropriate error otherwise
    ///
    /// # Security
    /// - Applies per-IP rate limiting ONLY on **failed** authentication attempts
    /// - Successful authentications bypass rate limiting entirely
    /// - When `client_ip` is None, rate limiting is skipped (for connection-based auth)
    /// - Uses constant-time comparison in signature verification
    /// - Returns generic errors to prevent user enumeration
    ///
    /// # Performance
    /// Legitimate clients with valid HMAC signatures get **unlimited throughput**.
    /// Only attackers attempting to brute-force signatures are rate limited.
    ///
    /// # Configuration
    /// Rate limiting is controlled by `auth.rate_limit_enabled` in config.
    /// When disabled or `client_ip` is None, no rate limiting is applied.
    ///
    /// # Errors
    /// - `RateLimitExceeded` if too many **failed** attempts from this IP
    /// - `AuthenticationFailed` for any authentication failure
    pub async fn verify_signature(
        &self,
        message: &str,
        user_id: &str,
        signature: &str,
        client_ip: Option<&str>,
    ) -> AuthResult<()> {
        // Verify signature FIRST
        let result = verify_signature(&self.cache, message, user_id, signature).await;

        // Only apply rate limiting on FAILED authentication attempts
        // Successful authentications bypass the rate limiter entirely
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

    /// Parses authentication from request
    /// Expected format: "user_id:signature:command"
    pub fn parse_auth<'a>(&self, input: &'a str) -> AuthResult<(&'a str, &'a str, &'a str)> {
        parse_auth(input)
    }

    /// Creates a new user and stores in SnelDB
    pub async fn create_user(
        &self,
        user_id: String,
        secret_key: Option<String>,
    ) -> AuthResult<String> {
        create_user(
            &self.cache,
            &self.permission_cache,
            &self.shard_manager,
            user_id,
            secret_key,
        )
        .await
    }

    /// Revokes a user's key (marks as inactive)
    pub async fn revoke_key(&self, user_id: &str) -> AuthResult<()> {
        revoke_key(
            &self.cache,
            &self.permission_cache,
            &self.shard_manager,
            user_id,
        )
        .await
    }

    /// Lists all users
    pub async fn list_users(&self) -> Vec<UserKey> {
        list_users(&self.cache).await
    }

    /// Checks if user has admin role
    pub async fn is_admin(&self, user_id: &str) -> bool {
        let perm_cache = self.permission_cache.read().await;
        perm_cache.is_admin(user_id)
    }

    /// Checks if user can read from event_type
    pub async fn can_read(&self, user_id: &str, event_type: &str) -> bool {
        let perm_cache = self.permission_cache.read().await;
        perm_cache.can_read(user_id, event_type)
    }

    /// Checks if user can write to event_type
    pub async fn can_write(&self, user_id: &str, event_type: &str) -> bool {
        let perm_cache = self.permission_cache.read().await;
        perm_cache.can_write(user_id, event_type)
    }

    /// Grants permissions to a user for specific event types
    pub async fn grant_permission(
        &self,
        user_id: &str,
        event_type: &str,
        permission_set: PermissionSet,
    ) -> AuthResult<()> {
        grant_permission(
            &self.cache,
            &self.permission_cache,
            &self.shard_manager,
            user_id,
            event_type,
            permission_set,
        )
        .await
    }

    /// Revokes permissions from a user for specific event types
    pub async fn revoke_permission(&self, user_id: &str, event_type: &str) -> AuthResult<()> {
        revoke_permission(
            &self.cache,
            &self.permission_cache,
            &self.shard_manager,
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

    /// Creates a user with specified roles
    pub async fn create_user_with_roles(
        &self,
        user_id: String,
        secret_key: Option<String>,
        roles: Vec<String>,
    ) -> AuthResult<String> {
        create_user_with_roles(
            &self.cache,
            &self.permission_cache,
            &self.shard_manager,
            user_id,
            secret_key,
            roles,
        )
        .await
    }

    /// Bootstraps admin user from configuration if no users exist
    pub async fn bootstrap_admin_user(&self) -> AuthResult<()> {
        bootstrap_admin_user(&self.cache, &self.permission_cache, &self.shard_manager).await
    }

    /// Loads users from SnelDB.
    ///
    /// This queries __auth_user events from the __system_auth context
    /// and populates both the user cache and permission cache.
    ///
    /// # Important
    /// Currently NOT IMPLEMENTED. See `db_ops::load_from_db` for details.
    ///
    /// # Returns
    /// `Ok(())` on success (or when skipped)
    ///
    /// # Errors
    /// `DatabaseError` if query fails (once implemented)
    pub async fn load_from_db(&self) -> AuthResult<()> {
        load_from_db(&self.cache, &self.permission_cache, &self.shard_manager).await
    }
}
