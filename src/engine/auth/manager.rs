use super::types::{AuthError, AuthResult, User, UserCache, UserKey};
use crate::engine::core::{Event, EventId};
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use crate::engine::shard::message::ShardMessage;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

type HmacSha256 = Hmac<Sha256>;

/// Manages user authentication and authorization
/// Uses in-memory cache for fast lookups and syncs with SnelDB
pub struct AuthManager {
    cache: Arc<RwLock<UserCache>>,
    shard_manager: Arc<ShardManager>,
}

impl AuthManager {
    pub fn new(shard_manager: Arc<ShardManager>) -> Self {
        Self {
            cache: Arc::new(RwLock::new(UserCache::new())),
            shard_manager,
        }
    }

    /// Verify HMAC signature for a message
    /// Format: user_id:signature:message
    pub async fn verify_signature(
        &self,
        message: &str,
        user_id: &str,
        signature: &str,
    ) -> AuthResult<()> {
        let cache = self.cache.read().await;
        let user_key = cache.get(user_id).ok_or_else(|| {
            debug!(target: "sneldb::auth", user_id, "User not found in cache");
            AuthError::UserNotFound(user_id.to_string())
        })?;

        if !user_key.active {
            return Err(AuthError::UserInactive(user_id.to_string()));
        }

        // Compute expected HMAC
        let mut mac = HmacSha256::new_from_slice(user_key.secret_key.as_bytes())
            .map_err(|_| AuthError::InvalidSignature)?;
        mac.update(message.as_bytes());
        let expected_signature = hex::encode(mac.finalize().into_bytes());

        // Constant-time comparison to prevent timing attacks
        if constant_time_eq(signature.as_bytes(), expected_signature.as_bytes()) {
            Ok(())
        } else {
            warn!(target: "sneldb::auth", user_id, "Invalid signature");
            Err(AuthError::InvalidSignature)
        }
    }

    /// Parse authentication from request
    /// Expected format: "user_id:signature:command"
    pub fn parse_auth<'a>(&self, input: &'a str) -> AuthResult<(&'a str, &'a str, &'a str)> {
        // Use byte-level operations for better performance
        let bytes = input.as_bytes();
        let mut first_colon = None;
        let mut second_colon = None;

        // Find both colons in a single pass
        for (i, &byte) in bytes.iter().enumerate() {
            if byte == b':' {
                if first_colon.is_none() {
                    first_colon = Some(i);
                } else if second_colon.is_none() {
                    second_colon = Some(i);
                    break;
                }
            }
        }

        let first_colon = first_colon.ok_or(AuthError::MissingSignature)?;
        let second_colon = second_colon.ok_or(AuthError::MissingSignature)?;

        let user_id = &input[..first_colon];
        let signature = &input[first_colon + 1..second_colon];
        let command = &input[second_colon + 1..];

        if user_id.is_empty() {
            return Err(AuthError::MissingUserId);
        }

        Ok((user_id, signature, command))
    }

    /// Create a new user and store in SnelDB
    pub async fn create_user(
        &self,
        user_id: String,
        secret_key: Option<String>,
    ) -> AuthResult<String> {
        // Validate user_id is not empty
        if user_id.is_empty() {
            return Err(AuthError::InvalidUserId);
        }

        // Validate user_id format (alphanumeric, underscore, hyphen)
        if !user_id
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
        {
            return Err(AuthError::InvalidUserId);
        }

        // Check if user already exists
        {
            let cache = self.cache.read().await;
            if cache.get(&user_id).is_some() {
                return Err(AuthError::UserExists(user_id));
            }
        }

        // Generate secret key if not provided
        let secret = secret_key.unwrap_or_else(|| {
            use rand::RngCore;
            let mut rng = rand::thread_rng();
            let mut bytes = [0u8; 32];
            rng.fill_bytes(&mut bytes);
            hex::encode(bytes)
        });

        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let user = User {
            user_id: user_id.clone(),
            secret_key: secret.clone(),
            active: true,
            created_at,
        };

        // Store user in SnelDB via STORE command
        self.store_user_in_db(&user).await?;

        // Update cache
        {
            let mut cache = self.cache.write().await;
            cache.insert(UserKey::from(user));
        }

        info!(target: "sneldb::auth", user_id, "User created successfully");
        Ok(secret)
    }

    /// Revoke a user's key (mark as inactive)
    pub async fn revoke_key(&self, user_id: &str) -> AuthResult<()> {
        let mut cache = self.cache.write().await;
        let user_key = cache
            .get(user_id)
            .ok_or_else(|| AuthError::UserNotFound(user_id.to_string()))?;

        // Update user in DB
        let updated_user = User {
            user_id: user_id.to_string(),
            secret_key: user_key.secret_key.clone(),
            active: false,
            created_at: 0, // Will be updated from DB
        };

        self.store_user_in_db(&updated_user).await?;

        // Update cache
        let mut updated_key = user_key.clone();
        updated_key.active = false;
        cache.insert(updated_key);

        info!(target: "sneldb::auth", user_id, "User key revoked");
        Ok(())
    }

    /// List all users
    pub async fn list_users(&self) -> Vec<UserKey> {
        let cache = self.cache.read().await;
        cache.all_users().into_iter().cloned().collect()
    }

    /// Load users from SnelDB
    /// This queries __auth_user events from the __system_auth context
    pub async fn load_from_db(&self) -> AuthResult<()> {
        // TODO: Implement full query to load users from all shards
        // For now, this is a placeholder - users will be loaded as they're created
        // In production, you'd want to query across all shards for __auth_user events
        // and populate the cache

        info!(target: "sneldb::auth", "User loading from DB skipped - users will be loaded on-demand");
        Ok(())
    }

    /// Store user in SnelDB via STORE command
    async fn store_user_in_db(&self, user: &User) -> AuthResult<()> {
        use std::collections::BTreeMap;

        let context_id = "__system_auth";
        let shard = self.shard_manager.get_shard(context_id);

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
                AuthError::InvalidSignature // Reuse error type for now
            })?;

        Ok(())
    }
}

/// Constant-time comparison to prevent timing attacks
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }

    a.iter()
        .zip(b.iter())
        .map(|(x, y)| x ^ y)
        .fold(0u8, |acc, x| acc | x)
        == 0
}
