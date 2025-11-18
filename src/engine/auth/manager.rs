use crate::engine::auth::db_ops::load_from_db;
use crate::engine::auth::permission_ops::{get_permissions, grant_permission, revoke_permission};
use crate::engine::auth::signature::{parse_auth, verify_signature};
use crate::engine::auth::storage::{AuthStorage, AuthWalStorage};
use crate::engine::auth::types::{
    AuthRateLimiter, AuthResult, PermissionCache, PermissionSet, SessionStore, SessionToken,
    UserCache, UserKey, create_rate_limiter,
};
use crate::engine::auth::user_ops::{
    bootstrap_admin_user, create_user, create_user_with_roles, list_users, revoke_key,
};
use crate::engine::shard::manager::ShardManager;
use crate::shared::config::CONFIG;
use hex;
use rand::RngCore;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, RwLock};
use tokio::time::{Duration, interval};

/// Manages authentication (HMAC-SHA256) and authorization (per-event permissions, RBAC).
/// Uses in-memory caches for O(1) lookups and optional rate limiting.
/// Also manages session tokens for high-throughput WebSocket authentication.
pub struct AuthManager {
    cache: Arc<RwLock<UserCache>>,
    permission_cache: Arc<RwLock<PermissionCache>>,
    session_store: Arc<RwLock<SessionStore>>,
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

        let session_store = Arc::new(RwLock::new(SessionStore::new()));
        let session_store_clone = Arc::clone(&session_store);

        // Spawn background task to cleanup expired sessions every 60 seconds
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                let mut store = session_store_clone.write().await;
                let removed = store.cleanup_expired();
                if removed > 0 {
                    tracing::debug!(
                        target: "sneldb::auth",
                        removed = removed,
                        "Cleaned up expired session tokens"
                    );
                }
            }
        });

        Self {
            cache: Arc::new(RwLock::new(UserCache::new())),
            permission_cache: Arc::new(RwLock::new(PermissionCache::new())),
            session_store,
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

    /// Generate a new session token for an authenticated user.
    /// Returns the token as a hex-encoded string (32 bytes = 64 hex chars).
    pub async fn generate_session_token(&self, user_id: &str) -> String {
        let expiry_seconds = CONFIG
            .auth
            .as_ref()
            .map(|a| a.session_token_expiry_seconds)
            .unwrap_or(300); // Default 5 minutes

        let expires_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + expiry_seconds;

        // Generate 32 random bytes (256 bits) for the token
        let mut token_bytes = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut token_bytes);
        let token = hex::encode(token_bytes);

        let session = SessionToken {
            user_id: user_id.to_string(),
            expires_at,
        };

        let mut store = self.session_store.write().await;
        store.insert(token.clone(), session);

        tracing::warn!(
            target: "sneldb::auth",
            user_id = user_id,
            token_len = token.len(),
            expires_in_secs = expiry_seconds,
            "Generated session token"
        );

        token
    }

    /// Validate a session token and return the associated user_id if valid.
    /// Returns None if token is invalid or expired.
    pub async fn validate_session_token(&self, token: &str) -> Option<String> {
        let store = self.session_store.read().await;
        let session = match store.get(token) {
            Some(s) => s,
            None => {
                tracing::warn!(
                    target: "sneldb::auth",
                    token_len = token.len(),
                    "Token not found in session store"
                );
                return None;
            }
        };

        // Check expiration
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        if session.expires_at < now {
            tracing::warn!(
                target: "sneldb::auth",
                user_id = session.user_id,
                expires_at = session.expires_at,
                now = now,
                "Token expired"
            );
            return None;
        }

        tracing::warn!(
            target: "sneldb::auth",
            user_id = session.user_id,
            token_len = token.len(),
            "Token validated successfully"
        );

        Some(session.user_id.clone())
    }

    /// Revoke a session token (useful for logout or security events).
    pub async fn revoke_session_token(&self, token: &str) -> bool {
        let mut store = self.session_store.write().await;
        store.remove(token)
    }
}
