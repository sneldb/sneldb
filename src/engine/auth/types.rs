use governor::{Quota, RateLimiter as GovernorRateLimiter};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

// Validation constants
pub const MAX_USER_ID_LENGTH: usize = 64;
pub const MAX_SECRET_KEY_LENGTH: usize = 512;
pub const MAX_SIGNATURE_LENGTH: usize = 256;

// Special user IDs for bypass modes
pub const BYPASS_USER_ID: &str = "bypass";
pub const NO_AUTH_USER_ID: &str = "no-auth";

/// Read/write permissions for an event type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermissionSet {
    pub read: bool,
    pub write: bool,
}

impl PermissionSet {
    pub fn new(read: bool, write: bool) -> Self {
        Self { read, write }
    }

    pub fn read_only() -> Self {
        Self {
            read: true,
            write: false,
        }
    }

    pub fn write_only() -> Self {
        Self {
            read: false,
            write: true,
        }
    }

    pub fn read_write() -> Self {
        Self {
            read: true,
            write: true,
        }
    }

    pub fn none() -> Self {
        Self {
            read: false,
            write: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub user_id: String,
    pub secret_key: String,
    pub active: bool,
    pub created_at: u64,
    #[serde(default)]
    pub roles: Vec<String>,
    /// Permissions per event_type (schema)
    /// Key: event_type, Value: PermissionSet
    #[serde(default)]
    pub permissions: HashMap<String, PermissionSet>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserKey {
    pub user_id: String,
    pub secret_key: String,
    pub active: bool,
    pub created_at: u64,
    pub roles: Vec<String>,
    pub permissions: HashMap<String, PermissionSet>,
}

impl From<User> for UserKey {
    fn from(user: User) -> Self {
        Self {
            user_id: user.user_id,
            secret_key: user.secret_key,
            active: user.active,
            created_at: user.created_at,
            roles: user.roles,
            permissions: user.permissions,
        }
    }
}

#[derive(Debug, Error)]
pub enum AuthError {
    // Generic error to prevent user enumeration
    #[error("Authentication failed")]
    AuthenticationFailed,
    #[error("User already exists")]
    UserExists,
    #[error("Invalid user ID format")]
    InvalidUserId,
    #[error("User ID too long (max {max} characters)")]
    UserIdTooLong { max: usize },
    #[error("Secret key too long (max {max} characters)")]
    SecretKeyTooLong { max: usize },
    #[error("Signature too long (max {max} characters)")]
    SignatureTooLong { max: usize },
    #[error("Database operation failed: {0}")]
    DatabaseError(String),
    #[error("Rate limit exceeded")]
    RateLimitExceeded,

    // Internal errors (logging only)
    #[error("User not found: {0}")]
    UserNotFound(String),
    #[error("User inactive: {0}")]
    UserInactive(String),
}

pub type AuthResult<T> = Result<T, AuthError>;

/// In-memory cache for user keys.
#[derive(Debug, Clone)]
pub struct UserCache {
    users: HashMap<String, UserKey>,
}

/// O(1) permission cache: user_id -> event_type -> PermissionSet.
/// Supports role-based access control (RBAC) with multiple role types.
#[derive(Debug, Clone)]
pub struct PermissionCache {
    /// Map of user_id -> event_type -> PermissionSet
    permissions: HashMap<String, HashMap<String, PermissionSet>>,
    /// Set of admin user IDs for fast admin checks (full access)
    admin_users: std::collections::HashSet<String>,
    /// Set of read-only user IDs (can read all event types)
    read_only_users: std::collections::HashSet<String>,
    /// Set of editor user IDs (can read/write all event types, no admin functions)
    editor_users: std::collections::HashSet<String>,
    /// Set of write-only user IDs (can write all event types, no read)
    write_only_users: std::collections::HashSet<String>,
}

impl PermissionCache {
    pub fn new() -> Self {
        Self {
            permissions: HashMap::new(),
            admin_users: std::collections::HashSet::new(),
            read_only_users: std::collections::HashSet::new(),
            editor_users: std::collections::HashSet::new(),
            write_only_users: std::collections::HashSet::new(),
        }
    }

    /// Check if user can read from event_type
    /// Priority: admin > specific permissions > roles
    /// Permissions override roles (more specific overrides broader access)
    /// If permission set grants READ, use it.
    /// If permission set exists but denies READ (and also denies WRITE), explicitly deny (override role).
    /// Otherwise fall back to role.
    pub fn can_read(&self, user_id: &str, event_type: &str) -> bool {
        // Admin users can read everything
        if self.admin_users.contains(user_id) {
            return true;
        }

        // Check specific permissions first (most granular)
        if let Some(user_perms) = self.permissions.get(user_id) {
            if let Some(perms) = user_perms.get(event_type) {
                if perms.read {
                    return true;
                }
                // Permission set exists but doesn't grant READ
                // If both read and write are false, this is an explicit denial (e.g., after REVOKE ALL)
                // In that case, override role and deny access
                if !perms.read && !perms.write {
                    return false;
                }
                // Permission set has read=false but write=true - likely from GRANT WRITE
                // Fall through to check role for READ
            }
        }

        // If no specific permissions for this event_type, check roles (broader access)
        // Read-only and editor roles can read everything
        if self.read_only_users.contains(user_id) || self.editor_users.contains(user_id) {
            return true;
        }

        // Write-only role cannot read (unless overridden by permissions above)
        if self.write_only_users.contains(user_id) {
            return false;
        }

        // No permissions and no roles
        false
    }

    /// Check if user can write to event_type
    /// Priority: admin > specific permissions > roles
    /// Permissions override roles (more specific overrides broader access)
    /// If permission set exists for event_type, it completely overrides role for WRITE.
    /// This ensures that REVOKE WRITE explicitly denies WRITE even if role would grant it.
    pub fn can_write(&self, user_id: &str, event_type: &str) -> bool {
        // Admin users can write everything
        if self.admin_users.contains(user_id) {
            return true;
        }

        // Check specific permissions first (most granular)
        // If permission set exists for this event_type, it overrides role completely
        if let Some(user_perms) = self.permissions.get(user_id) {
            if let Some(perms) = user_perms.get(event_type) {
                // Permission set exists - use it directly (overrides role)
                return perms.write;
            }
        }

        // If no specific permissions for this event_type, check roles (broader access)
        // Editor and write-only roles can write everything
        if self.editor_users.contains(user_id) || self.write_only_users.contains(user_id) {
            return true;
        }

        // Read-only role cannot write (unless overridden by permissions above)
        if self.read_only_users.contains(user_id) {
            return false;
        }

        // No permissions and no roles
        false
    }

    /// Check if user is admin
    pub fn is_admin(&self, user_id: &str) -> bool {
        self.admin_users.contains(user_id)
    }

    /// Check if user has a specific role
    #[allow(dead_code)] // Public API method, may be used by external code
    pub fn has_role(&self, user_id: &str, role: &str) -> bool {
        match role {
            "admin" => self.admin_users.contains(user_id),
            "read-only" | "viewer" => self.read_only_users.contains(user_id),
            "editor" => self.editor_users.contains(user_id),
            "write-only" => self.write_only_users.contains(user_id),
            _ => false,
        }
    }

    /// Update permissions for a user based on their roles
    pub fn update_user(&mut self, user: &UserKey) {
        let user_id = &user.user_id;

        // Update role sets - remove from all first, then add to appropriate ones
        self.admin_users.remove(user_id);
        self.read_only_users.remove(user_id);
        self.editor_users.remove(user_id);
        self.write_only_users.remove(user_id);

        // Add user to appropriate role sets
        for role in &user.roles {
            match role.as_str() {
                "admin" => {
                    self.admin_users.insert(user_id.clone());
                }
                "read-only" | "viewer" => {
                    self.read_only_users.insert(user_id.clone());
                }
                "editor" => {
                    self.editor_users.insert(user_id.clone());
                }
                "write-only" => {
                    self.write_only_users.insert(user_id.clone());
                }
                _ => {
                    // Unknown role - ignore (could log in future)
                }
            }
        }

        // Update permissions
        if user.permissions.is_empty() {
            self.permissions.remove(user_id);
        } else {
            self.permissions
                .insert(user_id.clone(), user.permissions.clone());
        }
    }
}

impl Default for PermissionCache {
    fn default() -> Self {
        Self::new()
    }
}

impl UserCache {
    pub fn new() -> Self {
        Self {
            users: HashMap::new(),
        }
    }

    pub fn get(&self, user_id: &str) -> Option<&UserKey> {
        self.users.get(user_id)
    }

    pub fn insert(&mut self, user: UserKey) {
        self.users.insert(user.user_id.clone(), user);
    }

    pub fn remove(&mut self, user_id: &str) -> bool {
        self.users.remove(user_id).is_some()
    }

    pub fn clear(&mut self) {
        self.users.clear();
    }

    pub fn all_users(&self) -> Vec<&UserKey> {
        self.users.values().collect()
    }
}

impl Default for UserCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Per-IP rate limiter for auth attempts (token bucket). Default: 10/sec per IP.
/// NOTE: Uses DefaultClock which does one-time calibration at startup (~20% of startup time)
/// but has low overhead during runtime. Acceptable tradeoff for rate limiting functionality.
pub type AuthRateLimiter = GovernorRateLimiter<
    String, // Key = IP address
    dashmap::DashMap<String, governor::state::InMemoryState>,
    governor::clock::DefaultClock,
>;

/// Creates a per-IP rate limiter. Panics if requests_per_second is 0.
pub fn create_rate_limiter(requests_per_second: u32) -> AuthRateLimiter {
    let quota = Quota::per_second(
        NonZeroU32::new(requests_per_second).expect("rate_limit_per_second must be greater than 0"),
    );
    GovernorRateLimiter::dashmap(quota)
}

/// Session token information
#[derive(Debug, Clone)]
pub struct SessionToken {
    pub user_id: String,
    pub expires_at: u64, // Unix timestamp in seconds
}

/// In-memory session token store
#[derive(Debug, Clone)]
pub struct SessionStore {
    sessions: HashMap<String, SessionToken>,
}

impl SessionStore {
    pub fn new() -> Self {
        Self {
            sessions: HashMap::new(),
        }
    }

    pub fn insert(&mut self, token: String, session: SessionToken) {
        self.sessions.insert(token, session);
    }

    pub fn get(&self, token: &str) -> Option<&SessionToken> {
        self.sessions.get(token)
    }

    pub fn remove(&mut self, token: &str) -> bool {
        self.sessions.remove(token).is_some()
    }

    /// Remove expired sessions. Returns number of sessions removed.
    pub fn cleanup_expired(&mut self) -> usize {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let expired: Vec<String> = self
            .sessions
            .iter()
            .filter(|(_, session)| session.expires_at < now)
            .map(|(token, _)| token.clone())
            .collect();

        let count = expired.len();
        for token in expired {
            self.sessions.remove(&token);
        }
        count
    }

    pub fn len(&self) -> usize {
        self.sessions.len()
    }

    /// Remove all sessions for a specific user. Returns number of sessions removed.
    pub fn revoke_user_sessions(&mut self, user_id: &str) -> usize {
        let tokens_to_remove: Vec<String> = self
            .sessions
            .iter()
            .filter(|(_, session)| session.user_id == user_id)
            .map(|(token, _)| token.clone())
            .collect();

        let count = tokens_to_remove.len();
        for token in tokens_to_remove {
            self.sessions.remove(&token);
        }
        count
    }
}

impl Default for SessionStore {
    fn default() -> Self {
        Self::new()
    }
}
