use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub user_id: String,
    pub secret_key: String,
    pub active: bool,
    pub created_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserKey {
    pub user_id: String,
    pub secret_key: String,
    pub active: bool,
}

impl From<User> for UserKey {
    fn from(user: User) -> Self {
        Self {
            user_id: user.user_id,
            secret_key: user.secret_key,
            active: user.active,
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
