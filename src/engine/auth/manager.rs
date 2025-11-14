use crate::engine::auth::db_ops::load_from_db;
use crate::engine::auth::permission_ops::{get_permissions, grant_permission, revoke_permission};
use crate::engine::auth::signature::{parse_auth, verify_signature};
use crate::engine::auth::types::{AuthResult, PermissionCache, PermissionSet, UserCache, UserKey};
use crate::engine::auth::user_ops::{
    bootstrap_admin_user, create_user, create_user_with_roles, list_users, revoke_key,
};
use crate::engine::shard::manager::ShardManager;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Manages user authentication and authorization
/// Uses in-memory cache for fast lookups and syncs with SnelDB
pub struct AuthManager {
    cache: Arc<RwLock<UserCache>>,
    permission_cache: Arc<RwLock<PermissionCache>>,
    shard_manager: Arc<ShardManager>,
}

impl AuthManager {
    pub fn new(shard_manager: Arc<ShardManager>) -> Self {
        Self {
            cache: Arc::new(RwLock::new(UserCache::new())),
            permission_cache: Arc::new(RwLock::new(PermissionCache::new())),
            shard_manager,
        }
    }

    /// Verifies HMAC signature for a message
    /// Format: user_id:signature:message
    pub async fn verify_signature(
        &self,
        message: &str,
        user_id: &str,
        signature: &str,
    ) -> AuthResult<()> {
        verify_signature(&self.cache, message, user_id, signature).await
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

    /// Loads users from SnelDB
    /// This queries __auth_user events from the __system_auth context
    pub async fn load_from_db(&self) -> AuthResult<()> {
        load_from_db(&self.shard_manager).await
    }
}
