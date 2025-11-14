use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

// Add hex dependency if not already present

/// Compute HMAC-SHA256 signature
pub fn compute_hmac(secret_key: &str, message: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret_key.as_bytes()).expect("HMAC can take key of any size");
    mac.update(message.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Replace HMAC_SHA256(key, message) placeholders in command with actual signatures
pub fn process_auth_placeholders(command: &str, _admin_key: Option<&str>) -> String {
    // Find HMAC_SHA256(...) patterns and replace them
    let mut result = command.to_string();

    // Simple string-based replacement for signature=HMAC_SHA256(key, message)
    while let Some(start) = result.find("HMAC_SHA256(") {
        if let Some(end) = result[start..].find(')') {
            let full_match = &result[start..start + end + 1];
            // Extract key and message from HMAC_SHA256(key, message)
            let inner = &full_match[12..full_match.len() - 1]; // Remove "HMAC_SHA256(" and ")"
            let parts: Vec<&str> = inner.splitn(2, ',').collect();
            if parts.len() == 2 {
                let key = parts[0].trim().trim_matches('"').trim();
                let message = parts[1].trim();
                let signature = compute_hmac(key, message);
                result = result.replace(full_match, &signature);
            } else {
                break; // Invalid format, stop processing
            }
        } else {
            break; // No closing paren, stop processing
        }
    }

    result
}

/// Add authentication to a command if needed
/// Returns command with inline auth format: user_id:signature:command
pub fn add_auth_to_command(command: &str, user_id: &str, secret_key: &str) -> String {
    let signature = compute_hmac(secret_key, command);
    format!("{}:{}:{}", user_id, signature, command)
}
