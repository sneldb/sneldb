mod db_ops;
mod manager;
mod permission_ops;
mod signature;
mod types;
mod user_ops;

pub use manager::AuthManager;
pub use types::{
    create_rate_limiter, AuthError, AuthRateLimiter, AuthResult, PermissionSet, User, UserCache,
    UserKey, BYPASS_USER_ID, MAX_SECRET_KEY_LENGTH, MAX_SIGNATURE_LENGTH, MAX_USER_ID_LENGTH,
    NO_AUTH_USER_ID,
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
mod user_ops_test;
