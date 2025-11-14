mod db_ops;
mod manager;
mod permission_ops;
mod signature;
mod types;
mod user_ops;

pub use manager::AuthManager;
pub use types::{AuthError, AuthResult, PermissionSet, User, UserCache, UserKey};

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
