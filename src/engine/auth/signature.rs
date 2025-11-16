use super::types::{
    AuthError, AuthResult, UserCache, MAX_SIGNATURE_LENGTH, MAX_USER_ID_LENGTH,
};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::sync::Arc;
use subtle::ConstantTimeEq;
use tokio::sync::RwLock;
use tracing::{debug, warn};

type HmacSha256 = Hmac<Sha256>;

/// Verifies HMAC signature for a message.
///
/// Returns a generic error to prevent user enumeration attacks.
/// Internal errors (user not found, inactive user) are logged but not exposed.
///
/// # Arguments
/// * `cache` - User cache containing authentication credentials
/// * `message` - The message that was signed
/// * `user_id` - The user ID claiming to have signed the message
/// * `signature` - The signature to verify
///
/// # Security
/// - Uses constant-time comparison to prevent timing attacks
/// - Returns generic error to prevent user enumeration
/// - Logs specific failures for debugging/auditing
pub async fn verify_signature(
    cache: &Arc<RwLock<UserCache>>,
    message: &str,
    user_id: &str,
    signature: &str,
) -> AuthResult<()> {
    // Validate input lengths to prevent DoS
    if signature.len() > MAX_SIGNATURE_LENGTH {
        warn!(target: "sneldb::auth", signature_len = signature.len(), "Signature too long");
        return Err(AuthError::AuthenticationFailed);
    }

    if user_id.len() > MAX_USER_ID_LENGTH {
        warn!(target: "sneldb::auth", user_id_len = user_id.len(), "User ID too long");
        return Err(AuthError::AuthenticationFailed);
    }

    let cache_guard = cache.read().await;
    let user_key = cache_guard.get(user_id).ok_or_else(|| {
        debug!(target: "sneldb::auth", user_id, "User not found in cache");
        AuthError::AuthenticationFailed // Return generic error
    })?;

    if !user_key.active {
        debug!(target: "sneldb::auth", user_id, "User inactive");
        return Err(AuthError::AuthenticationFailed); // Return generic error
    }

    // Compute expected HMAC
    let mut mac = HmacSha256::new_from_slice(user_key.secret_key.as_bytes())
        .map_err(|_| {
            warn!(target: "sneldb::auth", "Failed to create HMAC");
            AuthError::AuthenticationFailed
        })?;
    mac.update(message.as_bytes());
    let expected_signature = hex::encode(mac.finalize().into_bytes());

    // Constant-time comparison to prevent timing attacks
    if constant_time_eq(signature.as_bytes(), expected_signature.as_bytes()) {
        Ok(())
    } else {
        warn!(target: "sneldb::auth", user_id, "Invalid signature");
        Err(AuthError::AuthenticationFailed) // Return generic error
    }
}

/// Parses authentication from request.
///
/// Expected format: "user_id:signature:command"
///
/// # Arguments
/// * `input` - The input string to parse
///
/// # Returns
/// A tuple of (user_id, signature, command) if parsing succeeds
///
/// # Security
/// - Validates input lengths to prevent DoS attacks
/// - Returns generic errors for authentication failures
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

    let first_colon = first_colon.ok_or(AuthError::AuthenticationFailed)?;
    let second_colon = second_colon.ok_or(AuthError::AuthenticationFailed)?;

    let user_id = &input[..first_colon];
    let signature = &input[first_colon + 1..second_colon];
    let command = &input[second_colon + 1..];

    // Validate lengths
    if user_id.is_empty() || user_id.len() > MAX_USER_ID_LENGTH {
        return Err(AuthError::AuthenticationFailed);
    }

    if signature.len() > MAX_SIGNATURE_LENGTH {
        return Err(AuthError::AuthenticationFailed);
    }

    Ok((user_id, signature, command))
}

/// Constant-time comparison to prevent timing attacks.
///
/// Uses the `subtle` crate to ensure comparison happens in constant time,
/// preventing timing side-channel attacks that could leak information about
/// the expected signature.
///
/// # Arguments
/// * `a` - First byte slice to compare
/// * `b` - Second byte slice to compare
///
/// # Returns
/// `true` if the slices are equal, `false` otherwise (including length mismatch)
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    bool::from(a.ct_eq(b))
}

