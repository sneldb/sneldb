use crate::engine::auth::{AuthError, AuthManager, AuthResult, UserKey};
use crate::engine::shard::manager::ShardManager;
use crate::logging::init_for_tests;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::sync::Arc;
use tempfile::tempdir;

type HmacSha256 = Hmac<Sha256>;

/// Helper function to create a test AuthManager
async fn create_test_auth_manager() -> (Arc<AuthManager>, tempfile::TempDir) {
    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();
    let shard_manager = Arc::new(ShardManager::new(1, base_dir, wal_dir).await);
    let auth_manager = Arc::new(AuthManager::new(shard_manager));
    let temp_dir = tempdir().unwrap();
    (auth_manager, temp_dir)
}

/// Compute HMAC signature for testing
fn compute_hmac(message: &str, secret_key: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(secret_key.as_bytes()).unwrap();
    mac.update(message.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

#[tokio::test]
async fn test_auth_manager_new() {
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();
    let shard_manager = Arc::new(ShardManager::new(1, base_dir, wal_dir).await);
    let auth_manager = AuthManager::new(shard_manager);

    // Verify initial state - no users
    let users = auth_manager.list_users().await;
    assert_eq!(users.len(), 0);
}

#[tokio::test]
async fn test_create_user_success_without_secret_key() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    let result = auth_manager
        .create_user("test_user".to_string(), None)
        .await;

    assert!(result.is_ok());
    let secret_key = result.unwrap();

    // Verify secret key is generated (64 hex chars = 32 bytes)
    assert_eq!(secret_key.len(), 64);
    assert!(secret_key.chars().all(|c| c.is_ascii_hexdigit()));

    // Verify user was created in cache
    let users = auth_manager.list_users().await;
    assert_eq!(users.len(), 1);
    assert_eq!(users[0].user_id, "test_user");
    assert_eq!(users[0].secret_key, secret_key);
    assert!(users[0].active);
}

#[tokio::test]
async fn test_create_user_success_with_secret_key() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;
    let provided_key = "my_custom_secret_key_12345";

    let result = auth_manager
        .create_user("test_user2".to_string(), Some(provided_key.to_string()))
        .await;

    assert!(result.is_ok());
    let returned_key = result.unwrap();
    assert_eq!(returned_key, provided_key);

    // Verify user was created with provided key
    let users = auth_manager.list_users().await;
    assert_eq!(users.len(), 1);
    assert_eq!(users[0].user_id, "test_user2");
    assert_eq!(users[0].secret_key, provided_key);
    assert!(users[0].active);
}

#[tokio::test]
async fn test_create_user_error_user_exists() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Create first user
    auth_manager
        .create_user("existing_user".to_string(), None)
        .await
        .expect("First creation should succeed");

    // Try to create same user again
    let result = auth_manager
        .create_user("existing_user".to_string(), None)
        .await;

    assert!(result.is_err());
    match result {
        Err(AuthError::UserExists(user_id)) => assert_eq!(user_id, "existing_user"),
        _ => panic!("Expected UserExists error"),
    }

    // Verify only one user exists
    let users = auth_manager.list_users().await;
    assert_eq!(users.len(), 1);
}

#[tokio::test]
async fn test_create_user_error_invalid_user_id() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Test various invalid user IDs
    let invalid_ids = vec!["user@domain", "user#123", "user space", "user.dot", ""];

    for invalid_id in invalid_ids {
        let result = auth_manager.create_user(invalid_id.to_string(), None).await;
        assert!(
            result.is_err(),
            "Expected error for invalid user ID: '{}'",
            invalid_id
        );
        match result {
            Err(AuthError::InvalidUserId) => {}
            other => panic!(
                "Expected InvalidUserId for '{}', got {:?}",
                invalid_id, other
            ),
        }
    }
}

#[tokio::test]
async fn test_create_user_valid_user_id_formats() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Test valid user ID formats
    let valid_ids = vec![
        "user123",
        "user_123",
        "user-456",
        "user_123-456",
        "test-user_name",
    ];

    for (i, valid_id) in valid_ids.iter().enumerate() {
        let result = auth_manager.create_user(valid_id.to_string(), None).await;
        assert!(result.is_ok(), "Failed to create user '{}'", valid_id);
    }

    // Verify all users were created
    let users = auth_manager.list_users().await;
    assert_eq!(users.len(), valid_ids.len());
}

#[tokio::test]
async fn test_revoke_key_success() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Create a user first
    auth_manager
        .create_user("user_to_revoke".to_string(), None)
        .await
        .expect("User creation should succeed");

    // Verify user is active
    let users_before = auth_manager.list_users().await;
    assert_eq!(users_before.len(), 1);
    assert!(users_before[0].active);

    // Revoke the key
    let result = auth_manager.revoke_key("user_to_revoke").await;
    assert!(result.is_ok());

    // Verify user is now inactive
    let users_after = auth_manager.list_users().await;
    assert_eq!(users_after.len(), 1);
    assert!(!users_after[0].active);
    assert_eq!(users_after[0].user_id, "user_to_revoke");
}

#[tokio::test]
async fn test_revoke_key_error_user_not_found() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    let result = auth_manager.revoke_key("non_existent_user").await;

    assert!(result.is_err());
    match result {
        Err(AuthError::UserNotFound(user_id)) => assert_eq!(user_id, "non_existent_user"),
        _ => panic!("Expected UserNotFound error"),
    }
}

#[tokio::test]
async fn test_list_users_empty() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    let users = auth_manager.list_users().await;
    assert_eq!(users.len(), 0);
}

#[tokio::test]
async fn test_list_users_with_multiple_users() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Create multiple users
    let user_ids = vec!["user1", "user2", "user3"];
    for user_id in &user_ids {
        auth_manager
            .create_user(user_id.to_string(), None)
            .await
            .expect("User creation should succeed");
    }

    // List users
    let users = auth_manager.list_users().await;
    assert_eq!(users.len(), user_ids.len());

    // Verify all users are present
    let found_ids: Vec<String> = users.iter().map(|u| u.user_id.clone()).collect();
    for user_id in &user_ids {
        assert!(found_ids.contains(&user_id.to_string()));
    }
}

#[tokio::test]
async fn test_list_users_shows_active_and_inactive() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Create two users
    auth_manager
        .create_user("active_user".to_string(), None)
        .await
        .expect("User creation should succeed");
    auth_manager
        .create_user("inactive_user".to_string(), None)
        .await
        .expect("User creation should succeed");

    // Revoke one user's key
    auth_manager
        .revoke_key("inactive_user")
        .await
        .expect("Revoke should succeed");

    // List users
    let users = auth_manager.list_users().await;
    assert_eq!(users.len(), 2);

    // Verify active/inactive status
    let active_user = users.iter().find(|u| u.user_id == "active_user").unwrap();
    let inactive_user = users.iter().find(|u| u.user_id == "inactive_user").unwrap();
    assert!(active_user.active);
    assert!(!inactive_user.active);
}

#[tokio::test]
async fn test_verify_signature_success() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Create a user
    let secret_key = auth_manager
        .create_user("test_user".to_string(), Some("my_secret_key".to_string()))
        .await
        .expect("User creation should succeed");

    // Compute signature for a message
    let message = "STORE test_event FOR ctx1 PAYLOAD {\"id\":1}";
    let signature = compute_hmac(message, &secret_key);

    // Verify signature
    let result = auth_manager
        .verify_signature(message, "test_user", &signature)
        .await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_verify_signature_error_invalid_signature() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Create a user
    auth_manager
        .create_user("test_user".to_string(), Some("my_secret_key".to_string()))
        .await
        .expect("User creation should succeed");

    // Try to verify with wrong signature
    let message = "STORE test_event FOR ctx1 PAYLOAD {\"id\":1}";
    let wrong_signature = "invalid_signature_12345";

    let result = auth_manager
        .verify_signature(message, "test_user", wrong_signature)
        .await;

    assert!(result.is_err());
    match result {
        Err(AuthError::InvalidSignature) => {}
        _ => panic!("Expected InvalidSignature error"),
    }
}

#[tokio::test]
async fn test_verify_signature_error_user_not_found() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    let result = auth_manager
        .verify_signature("message", "non_existent_user", "signature")
        .await;

    assert!(result.is_err());
    match result {
        Err(AuthError::UserNotFound(user_id)) => assert_eq!(user_id, "non_existent_user"),
        _ => panic!("Expected UserNotFound error"),
    }
}

#[tokio::test]
async fn test_verify_signature_error_user_inactive() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Create and revoke user
    let secret_key = auth_manager
        .create_user(
            "inactive_user".to_string(),
            Some("my_secret_key".to_string()),
        )
        .await
        .expect("User creation should succeed");

    auth_manager
        .revoke_key("inactive_user")
        .await
        .expect("Revoke should succeed");

    // Try to verify signature with inactive user
    let message = "STORE test_event FOR ctx1 PAYLOAD {\"id\":1}";
    let signature = compute_hmac(message, &secret_key);

    let result = auth_manager
        .verify_signature(message, "inactive_user", &signature)
        .await;

    assert!(result.is_err());
    match result {
        Err(AuthError::UserInactive(user_id)) => assert_eq!(user_id, "inactive_user"),
        _ => panic!("Expected UserInactive error"),
    }
}

#[tokio::test]
async fn test_verify_signature_different_messages() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Create a user
    let secret_key = auth_manager
        .create_user("test_user".to_string(), Some("my_secret_key".to_string()))
        .await
        .expect("User creation should succeed");

    // Verify signature works for different messages
    let messages = vec![
        "STORE test_event FOR ctx1 PAYLOAD {\"id\":1}",
        "QUERY test_event",
        "DEFINE test_event FIELDS {\"id\":\"int\"}",
    ];

    for message in messages {
        let signature = compute_hmac(message, &secret_key);
        let result = auth_manager
            .verify_signature(message, "test_user", &signature)
            .await;
        assert!(
            result.is_ok(),
            "Signature verification failed for message: {}",
            message
        );
    }
}

#[tokio::test]
async fn test_verify_signature_message_must_match() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Create a user
    let secret_key = auth_manager
        .create_user("test_user".to_string(), Some("my_secret_key".to_string()))
        .await
        .expect("User creation should succeed");

    // Compute signature for one message
    let original_message = "STORE test_event FOR ctx1 PAYLOAD {\"id\":1}";
    let signature = compute_hmac(original_message, &secret_key);

    // Try to verify with different message (should fail)
    let different_message = "STORE test_event FOR ctx1 PAYLOAD {\"id\":2}";
    let result = auth_manager
        .verify_signature(different_message, "test_user", &signature)
        .await;

    assert!(result.is_err());
    match result {
        Err(AuthError::InvalidSignature) => {}
        _ => panic!("Expected InvalidSignature error"),
    }
}

#[tokio::test]
async fn test_parse_auth_success() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    let input = "user123:signature_abc:STORE test_event FOR ctx1 PAYLOAD {\"id\":1}";
    let result = auth_manager.parse_auth(input);

    assert!(result.is_ok());
    let (user_id, signature, command) = result.unwrap();
    assert_eq!(user_id, "user123");
    assert_eq!(signature, "signature_abc");
    assert_eq!(command, "STORE test_event FOR ctx1 PAYLOAD {\"id\":1}");
}

#[tokio::test]
async fn test_parse_auth_error_missing_signature() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Missing colons
    let result = auth_manager.parse_auth("user123 STORE test_event");
    assert!(result.is_err());
    match result {
        Err(AuthError::MissingSignature) => {}
        _ => panic!("Expected MissingSignature error"),
    }

    // Only one colon
    let result = auth_manager.parse_auth("user123:STORE test_event");
    assert!(result.is_err());
    match result {
        Err(AuthError::MissingSignature) => {}
        _ => panic!("Expected MissingSignature error"),
    }
}

#[tokio::test]
async fn test_parse_auth_error_missing_user_id() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Empty user_id
    let result = auth_manager.parse_auth(":signature:STORE test_event");
    assert!(result.is_err());
    match result {
        Err(AuthError::MissingUserId) => {}
        _ => panic!("Expected MissingUserId error"),
    }
}

#[tokio::test]
async fn test_parse_auth_with_special_characters() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

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
        let result = auth_manager.parse_auth(input);
        assert!(result.is_ok(), "Failed to parse: {}", input);
        let (user_id, signature, command) = result.unwrap();
        assert_eq!(user_id, expected_user_id);
        assert_eq!(signature, expected_sig);
        assert_eq!(command, expected_cmd);
    }
}

#[tokio::test]
async fn test_create_user_then_revoke_then_create_again_fails() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Create user
    auth_manager
        .create_user("recreate_user".to_string(), None)
        .await
        .expect("User creation should succeed");

    // Revoke key
    auth_manager
        .revoke_key("recreate_user")
        .await
        .expect("Revoke should succeed");

    // Try to create same user again (should fail - user still exists)
    let result = auth_manager
        .create_user("recreate_user".to_string(), None)
        .await;

    assert!(result.is_err());
    match result {
        Err(AuthError::UserExists(user_id)) => assert_eq!(user_id, "recreate_user"),
        _ => panic!("Expected UserExists error"),
    }
}

#[tokio::test]
async fn test_multiple_operations_sequence() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Create user1
    let secret_key1 = auth_manager
        .create_user("user1".to_string(), Some("key1".to_string()))
        .await
        .expect("User creation should succeed");

    // Create user2
    let secret_key2 = auth_manager
        .create_user("user2".to_string(), Some("key2".to_string()))
        .await
        .expect("User creation should succeed");

    // List users (should show 2)
    let users = auth_manager.list_users().await;
    assert_eq!(users.len(), 2);

    // Verify signatures work for both users
    let message1 = "STORE event1 FOR ctx1 PAYLOAD {\"id\":1}";
    let signature1 = compute_hmac(message1, &secret_key1);
    assert!(
        auth_manager
            .verify_signature(message1, "user1", &signature1)
            .await
            .is_ok()
    );

    let message2 = "STORE event2 FOR ctx2 PAYLOAD {\"id\":2}";
    let signature2 = compute_hmac(message2, &secret_key2);
    assert!(
        auth_manager
            .verify_signature(message2, "user2", &signature2)
            .await
            .is_ok()
    );

    // Revoke user1
    auth_manager
        .revoke_key("user1")
        .await
        .expect("Revoke should succeed");

    // Verify user1 signature no longer works
    assert!(
        auth_manager
            .verify_signature(message1, "user1", &signature1)
            .await
            .is_err()
    );

    // Verify user2 signature still works
    assert!(
        auth_manager
            .verify_signature(message2, "user2", &signature2)
            .await
            .is_ok()
    );

    // List users again (should still show 2, but user1 inactive)
    let users_after = auth_manager.list_users().await;
    assert_eq!(users_after.len(), 2);
    let user1 = users_after.iter().find(|u| u.user_id == "user1").unwrap();
    let user2 = users_after.iter().find(|u| u.user_id == "user2").unwrap();
    assert!(!user1.active);
    assert!(user2.active);
}

#[tokio::test]
async fn test_load_from_db() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // load_from_db is currently a placeholder that always succeeds
    let result = auth_manager.load_from_db().await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_create_user_generates_unique_keys() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Create multiple users without providing keys
    let mut keys = Vec::new();
    for i in 0..10 {
        let key = auth_manager
            .create_user(format!("user{}", i), None)
            .await
            .expect("User creation should succeed");
        keys.push(key);
    }

    // Verify all keys are unique
    for (i, key1) in keys.iter().enumerate() {
        for (j, key2) in keys.iter().enumerate() {
            if i != j {
                assert_ne!(key1, key2, "Keys should be unique");
            }
        }
    }
}

#[tokio::test]
async fn test_verify_signature_case_sensitive() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Create a user
    let secret_key = auth_manager
        .create_user("test_user".to_string(), Some("my_secret_key".to_string()))
        .await
        .expect("User creation should succeed");

    // Compute signature for a message
    let message = "STORE test_event FOR ctx1 PAYLOAD {\"id\":1}";
    let signature = compute_hmac(message, &secret_key);

    // Verify signature works
    assert!(
        auth_manager
            .verify_signature(message, "test_user", &signature)
            .await
            .is_ok()
    );

    // Verify case-sensitive user_id fails
    assert!(
        auth_manager
            .verify_signature(message, "TEST_USER", &signature)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn test_revoke_key_preserves_secret_key() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Create a user
    let original_secret = "my_secret_key_12345";
    auth_manager
        .create_user("test_user".to_string(), Some(original_secret.to_string()))
        .await
        .expect("User creation should succeed");

    // Revoke key
    auth_manager
        .revoke_key("test_user")
        .await
        .expect("Revoke should succeed");

    // Verify secret key is preserved
    let users = auth_manager.list_users().await;
    let user = users.iter().find(|u| u.user_id == "test_user").unwrap();
    assert_eq!(user.secret_key, original_secret);
    assert!(!user.active);
}

#[tokio::test]
async fn test_create_user_empty_string() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    let result = auth_manager.create_user("".to_string(), None).await;

    assert!(result.is_err());
    match result {
        Err(AuthError::InvalidUserId) => {}
        _ => panic!("Expected InvalidUserId error"),
    }
}

#[tokio::test]
async fn test_parse_auth_with_empty_parts() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Empty signature
    let result = auth_manager.parse_auth("user::command");
    assert!(result.is_ok()); // Empty signature is allowed (though invalid)

    // Empty command
    let result = auth_manager.parse_auth("user:sig:");
    assert!(result.is_ok()); // Empty command is allowed
}

#[tokio::test]
async fn test_constant_time_comparison() {
    init_for_tests();

    // Test constant_time_eq function indirectly through verify_signature
    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    let secret_key = auth_manager
        .create_user("test_user".to_string(), Some("my_secret_key".to_string()))
        .await
        .expect("User creation should succeed");

    let message = "STORE test_event FOR ctx1 PAYLOAD {\"id\":1}";
    let correct_signature = compute_hmac(message, &secret_key);

    // Try with similar but incorrect signatures (should all fail)
    let wrong_signatures = vec![
        &correct_signature[..63], // One char shorter
        "a",                      // Completely wrong
        &correct_signature[1..],  // One char off
    ];

    for wrong_sig in wrong_signatures {
        let result = auth_manager
            .verify_signature(message, "test_user", wrong_sig)
            .await;
        assert!(result.is_err());
    }
}
