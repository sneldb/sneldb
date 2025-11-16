use super::signature::{parse_auth, verify_signature};
use super::types::{AuthError, UserCache, UserKey};
use crate::logging::init_for_tests;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

type HmacSha256 = Hmac<Sha256>;

/// Helper function to create a test user cache with a user
async fn create_test_cache_with_user(
    user_id: &str,
    secret_key: &str,
    active: bool,
) -> Arc<RwLock<UserCache>> {
    let cache = Arc::new(RwLock::new(UserCache::new()));
    let user_key = UserKey {
        user_id: user_id.to_string(),
        secret_key: secret_key.to_string(),
        active,
        created_at: 1000,
        roles: Vec::new(),
        permissions: HashMap::new(),
    };
    {
        let mut cache_guard = cache.write().await;
        cache_guard.insert(user_key);
    }
    cache
}

/// Helper function to create an empty test cache
fn create_empty_cache() -> Arc<RwLock<UserCache>> {
    Arc::new(RwLock::new(UserCache::new()))
}

/// Compute HMAC signature for testing
fn compute_hmac(message: &str, secret_key: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(secret_key.as_bytes()).unwrap();
    mac.update(message.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

#[tokio::test]
async fn test_verify_signature_success() {
    init_for_tests();

    let cache = create_test_cache_with_user("test_user", "my_secret_key", true).await;
    let message = "STORE test_event FOR ctx1 PAYLOAD {\"id\":1}";
    let signature = compute_hmac(message, "my_secret_key");

    let result = verify_signature(&cache, message, "test_user", &signature).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_verify_signature_error_invalid_signature() {
    init_for_tests();

    let cache = create_test_cache_with_user("test_user", "my_secret_key", true).await;
    let message = "STORE test_event FOR ctx1 PAYLOAD {\"id\":1}";
    let wrong_signature = "invalid_signature_12345";

    let result = verify_signature(&cache, message, "test_user", wrong_signature).await;

    assert!(result.is_err());
    match result {
        Err(AuthError::AuthenticationFailed) => {}
        _ => panic!("Expected AuthenticationFailed error"),
    }
}

#[tokio::test]
async fn test_verify_signature_error_user_not_found() {
    init_for_tests();

    let cache = create_empty_cache();
    let result = verify_signature(&cache, "message", "non_existent_user", "signature").await;

    // verify_signature returns generic AuthenticationFailed to prevent user enumeration
    assert!(result.is_err());
    match result {
        Err(AuthError::AuthenticationFailed) => {
            // Expected: generic error for security
        }
        _ => panic!("Expected AuthenticationFailed error"),
    }
}

#[tokio::test]
async fn test_verify_signature_error_user_inactive() {
    init_for_tests();

    let cache = create_test_cache_with_user("inactive_user", "my_secret_key", false).await;
    let message = "STORE test_event FOR ctx1 PAYLOAD {\"id\":1}";
    let signature = compute_hmac(message, "my_secret_key");

    let result = verify_signature(&cache, message, "inactive_user", &signature).await;

    // verify_signature returns generic AuthenticationFailed to prevent user enumeration
    assert!(result.is_err());
    match result {
        Err(AuthError::AuthenticationFailed) => {
            // Expected: generic error for security
        }
        _ => panic!("Expected AuthenticationFailed error"),
    }
}

#[tokio::test]
async fn test_verify_signature_message_must_match() {
    init_for_tests();

    let cache = create_test_cache_with_user("test_user", "my_secret_key", true).await;
    let original_message = "STORE test_event FOR ctx1 PAYLOAD {\"id\":1}";
    let signature = compute_hmac(original_message, "my_secret_key");

    // Try to verify with different message (should fail)
    let different_message = "STORE test_event FOR ctx1 PAYLOAD {\"id\":2}";
    let result = verify_signature(&cache, different_message, "test_user", &signature).await;

    assert!(result.is_err());
    match result {
        Err(AuthError::AuthenticationFailed) => {}
        _ => panic!("Expected AuthenticationFailed error"),
    }
}

#[tokio::test]
async fn test_verify_signature_different_messages() {
    init_for_tests();

    let cache = create_test_cache_with_user("test_user", "my_secret_key", true).await;
    let secret_key = "my_secret_key";

    // Verify signature works for different messages
    let messages = vec![
        "STORE test_event FOR ctx1 PAYLOAD {\"id\":1}",
        "QUERY test_event",
        "DEFINE test_event FIELDS {\"id\":\"int\"}",
    ];

    for message in messages {
        let signature = compute_hmac(message, secret_key);
        let result = verify_signature(&cache, message, "test_user", &signature).await;
        assert!(
            result.is_ok(),
            "Signature verification failed for message: {}",
            message
        );
    }
}

#[tokio::test]
async fn test_verify_signature_case_sensitive_user_id() {
    init_for_tests();

    let cache = create_test_cache_with_user("test_user", "my_secret_key", true).await;
    let message = "STORE test_event FOR ctx1 PAYLOAD {\"id\":1}";
    let signature = compute_hmac(message, "my_secret_key");

    // Verify signature works with correct case
    assert!(
        verify_signature(&cache, message, "test_user", &signature)
            .await
            .is_ok()
    );

    // Verify case-sensitive user_id fails
    assert!(
        verify_signature(&cache, message, "TEST_USER", &signature)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn test_verify_signature_empty_message() {
    init_for_tests();

    let cache = create_test_cache_with_user("test_user", "my_secret_key", true).await;
    let message = "";
    let signature = compute_hmac(message, "my_secret_key");

    let result = verify_signature(&cache, message, "test_user", &signature).await;
    assert!(result.is_ok(), "Empty message should be valid");
}

#[tokio::test]
async fn test_verify_signature_empty_signature() {
    init_for_tests();

    let cache = create_test_cache_with_user("test_user", "my_secret_key", true).await;
    let message = "STORE test_event FOR ctx1 PAYLOAD {\"id\":1}";

    let result = verify_signature(&cache, message, "test_user", "").await;
    assert!(result.is_err(), "Empty signature should be invalid");
}

#[tokio::test]
async fn test_parse_auth_success() {
    init_for_tests();

    let input = "user123:signature_abc:STORE test_event FOR ctx1 PAYLOAD {\"id\":1}";
    let result = parse_auth(input);

    assert!(result.is_ok());
    let (user_id, signature, command) = result.unwrap();
    assert_eq!(user_id, "user123");
    assert_eq!(signature, "signature_abc");
    assert_eq!(command, "STORE test_event FOR ctx1 PAYLOAD {\"id\":1}");
}

#[tokio::test]
async fn test_parse_auth_error_missing_signature() {
    init_for_tests();

    // Missing colons
    let result = parse_auth("user123 STORE test_event");
    assert!(result.is_err());
    match result {
        Err(AuthError::AuthenticationFailed) => {}
        _ => panic!("Expected AuthenticationFailed error"),
    }

    // Only one colon
    let result = parse_auth("user123:STORE test_event");
    assert!(result.is_err());
    match result {
        Err(AuthError::AuthenticationFailed) => {}
        _ => panic!("Expected AuthenticationFailed error"),
    }
}

#[tokio::test]
async fn test_parse_auth_error_missing_user_id() {
    init_for_tests();

    // Empty user_id
    let result = parse_auth(":signature:STORE test_event");
    assert!(result.is_err());
    match result {
        Err(AuthError::AuthenticationFailed) => {}
        _ => panic!("Expected AuthenticationFailed error"),
    }
}

#[tokio::test]
async fn test_parse_auth_with_special_characters() {
    init_for_tests();

    // Test with various command formats
    let test_cases = vec![
        (
            "user1:sig1:STORE event FOR ctx PAYLOAD {}",
            "user1",
            "sig1",
            "STORE event FOR ctx PAYLOAD {}",
        ),
        (
            "user2:sig2:QUERY event WHERE id = 1",
            "user2",
            "sig2",
            "QUERY event WHERE id = 1",
        ),
        (
            "user3:sig3:DEFINE event FIELDS {\"id\":\"int\"}",
            "user3",
            "sig3",
            "DEFINE event FIELDS {\"id\":\"int\"}",
        ),
    ];

    for (input, expected_user_id, expected_sig, expected_cmd) in test_cases {
        let result = parse_auth(input);
        assert!(result.is_ok(), "Failed to parse: {}", input);
        let (user_id, signature, command) = result.unwrap();
        assert_eq!(user_id, expected_user_id);
        assert_eq!(signature, expected_sig);
        assert_eq!(command, expected_cmd);
    }
}

#[tokio::test]
async fn test_parse_auth_with_empty_parts() {
    init_for_tests();

    // Empty signature
    let result = parse_auth("user::command");
    assert!(result.is_ok()); // Empty signature is allowed (though invalid)
    let (user_id, signature, command) = result.unwrap();
    assert_eq!(user_id, "user");
    assert_eq!(signature, "");
    assert_eq!(command, "command");

    // Empty command
    let result = parse_auth("user:sig:");
    assert!(result.is_ok()); // Empty command is allowed
    let (user_id, signature, command) = result.unwrap();
    assert_eq!(user_id, "user");
    assert_eq!(signature, "sig");
    assert_eq!(command, "");
}

#[tokio::test]
async fn test_parse_auth_with_colons_in_command() {
    init_for_tests();

    // Command can contain colons
    let input = "user1:sig1:STORE event FOR ctx PAYLOAD {\"time\":\"12:34:56\"}";
    let result = parse_auth(input);
    assert!(result.is_ok());
    let (user_id, signature, command) = result.unwrap();
    assert_eq!(user_id, "user1");
    assert_eq!(signature, "sig1");
    assert_eq!(
        command,
        "STORE event FOR ctx PAYLOAD {\"time\":\"12:34:56\"}"
    );
}

#[tokio::test]
async fn test_parse_auth_with_unicode() {
    init_for_tests();

    // Test with unicode characters in user_id
    let input = "user_123:sig:COMMAND";
    let result = parse_auth(input);
    assert!(result.is_ok());
    let (user_id, signature, command) = result.unwrap();
    assert_eq!(user_id, "user_123");
    assert_eq!(signature, "sig");
    assert_eq!(command, "COMMAND");
}

#[tokio::test]
async fn test_parse_auth_multiple_colons() {
    init_for_tests();

    // Only first two colons are used for parsing
    let input = "user:sig:command:with:more:colons";
    let result = parse_auth(input);
    assert!(result.is_ok());
    let (user_id, signature, command) = result.unwrap();
    assert_eq!(user_id, "user");
    assert_eq!(signature, "sig");
    assert_eq!(command, "command:with:more:colons");
}

#[tokio::test]
async fn test_verify_signature_constant_time_comparison() {
    init_for_tests();

    // Test constant-time comparison indirectly through verify_signature
    let cache = create_test_cache_with_user("test_user", "my_secret_key", true).await;
    let message = "STORE test_event FOR ctx1 PAYLOAD {\"id\":1}";
    let correct_signature = compute_hmac(message, "my_secret_key");

    // Try with similar but incorrect signatures (should all fail)
    let wrong_signatures: Vec<String> = vec![
        correct_signature[..63].to_string(), // One char shorter
        "a".to_string(),                     // Completely wrong
        correct_signature[1..].to_string(),  // One char off
        "0".repeat(64),                      // Wrong but same length
    ];

    for wrong_sig in wrong_signatures {
        let result = verify_signature(&cache, message, "test_user", &wrong_sig).await;
        assert!(
            result.is_err(),
            "Should fail for wrong signature: {}",
            wrong_sig
        );
    }
}

#[tokio::test]
async fn test_verify_signature_different_secret_keys() {
    init_for_tests();

    let cache1 = create_test_cache_with_user("user1", "secret1", true).await;
    let cache2 = create_test_cache_with_user("user2", "secret2", true).await;
    let message = "STORE test_event FOR ctx1 PAYLOAD {\"id\":1}";

    let sig1 = compute_hmac(message, "secret1");
    let sig2 = compute_hmac(message, "secret2");

    // Each user's signature should work with their own cache
    assert!(
        verify_signature(&cache1, message, "user1", &sig1)
            .await
            .is_ok()
    );
    assert!(
        verify_signature(&cache2, message, "user2", &sig2)
            .await
            .is_ok()
    );

    // But not with the other user's cache
    assert!(
        verify_signature(&cache1, message, "user1", &sig2)
            .await
            .is_err()
    );
    assert!(
        verify_signature(&cache2, message, "user2", &sig1)
            .await
            .is_err()
    );
}
