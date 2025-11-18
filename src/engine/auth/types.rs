use governor::{Quota, RateLimiter as GovernorRateLimiter};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::num::NonZeroU32;
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
#[derive(Debug, Clone)]
pub struct PermissionCache {
    /// Map of user_id -> event_type -> PermissionSet
    permissions: HashMap<String, HashMap<String, PermissionSet>>,
    /// Set of admin user IDs for fast admin checks
    admin_users: std::collections::HashSet<String>,
}

impl PermissionCache {
    pub fn new() -> Self {
        Self {
            permissions: HashMap::new(),
            admin_users: std::collections::HashSet::new(),
        }
    }

    /// Check if user can read from event_type
    pub fn can_read(&self, user_id: &str, event_type: &str) -> bool {
        // Admin users can read everything
        if self.admin_users.contains(user_id) {
            return true;
        }

        self.permissions
            .get(user_id)
            .and_then(|user_perms| user_perms.get(event_type))
            .map(|perms| perms.read)
            .unwrap_or(false)
    }

    /// Check if user can write to event_type
    pub fn can_write(&self, user_id: &str, event_type: &str) -> bool {
        // Admin users can write everything
        if self.admin_users.contains(user_id) {
            return true;
        }

        self.permissions
            .get(user_id)
            .and_then(|user_perms| user_perms.get(event_type))
            .map(|perms| perms.write)
            .unwrap_or(false)
    }

    /// Check if user is admin
    pub fn is_admin(&self, user_id: &str) -> bool {
        self.admin_users.contains(user_id)
    }

    /// Update permissions for a user
    pub fn update_user(&mut self, user: &UserKey) {
        // Update admin set
        if user.roles.contains(&"admin".to_string()) {
            self.admin_users.insert(user.user_id.clone());
        } else {
            self.admin_users.remove(&user.user_id);
        }

        // Update permissions
        if user.permissions.is_empty() {
            self.permissions.remove(&user.user_id);
        } else {
            self.permissions
                .insert(user.user_id.clone(), user.permissions.clone());
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
