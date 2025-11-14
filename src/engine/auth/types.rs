use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

/// Represents read and write permissions for a specific event type
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
    #[error("User not found: {0}")]
    UserNotFound(String),
    #[error("User inactive: {0}")]
    UserInactive(String),
    #[error("Invalid signature")]
    InvalidSignature,
    #[error("Missing user ID")]
    MissingUserId,
    #[error("Missing signature")]
    MissingSignature,
    #[error("User already exists: {0}")]
    UserExists(String),
    #[error("Invalid user ID format")]
    InvalidUserId,
}

pub type AuthResult<T> = Result<T, AuthError>;

/// In-memory cache for user keys
#[derive(Debug, Clone)]
pub struct UserCache {
    users: HashMap<String, UserKey>,
}

/// Fast permission cache for O(1) permission lookups
/// Structure: user_id -> event_type -> PermissionSet
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

    /// Remove user from cache
    pub fn remove_user(&mut self, user_id: &str) {
        self.permissions.remove(user_id);
        self.admin_users.remove(user_id);
    }

    /// Clear all cached permissions
    pub fn clear(&mut self) {
        self.permissions.clear();
        self.admin_users.clear();
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
