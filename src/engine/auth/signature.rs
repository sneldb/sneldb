use super::types::{AuthError, AuthResult, UserCache};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, warn};

type HmacSha256 = Hmac<Sha256>;

/// Verifies HMAC signature for a message
/// Format: user_id:signature:message
pub async fn verify_signature(
    cache: &Arc<RwLock<UserCache>>,
    message: &str,
    user_id: &str,
    signature: &str,
) -> AuthResult<()> {
    let cache_guard = cache.read().await;
    let user_key = cache_guard.get(user_id).ok_or_else(|| {
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

/// Parses authentication from request
/// Expected format: "user_id:signature:command"
pub fn parse_auth<'a>(input: &'a str) -> AuthResult<(&'a str, &'a str, &'a str)> {
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

