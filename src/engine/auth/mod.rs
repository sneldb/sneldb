mod db_ops;
mod manager;
mod permission_ops;
mod signature;
mod storage;
mod types;
mod user_ops;

pub use manager::AuthManager;
pub use types::{
    AuthError, AuthRateLimiter, AuthResult, BYPASS_USER_ID, MAX_SECRET_KEY_LENGTH,
    MAX_SIGNATURE_LENGTH, MAX_USER_ID_LENGTH, NO_AUTH_USER_ID, PermissionSet, SessionStore,
    SessionToken, User, UserCache, UserKey, create_rate_limiter,
};

#[cfg(test)]
mod db_ops_test;
#[cfg(test)]
mod manager_test;
#[cfg(test)]
mod permission_ops_test;
#[cfg(test)]
mod signature_test;
#[cfg(test)]
mod storage_test;
#[cfg(test)]
mod user_ops_test;
