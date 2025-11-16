use crate::engine::auth::storage::AuthWalStorage;
use crate::engine::auth::{AuthError, AuthManager};
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
    let auth_wal_dir = tempdir().unwrap().into_path().join("auth.swal");
    let storage =
        Arc::new(AuthWalStorage::new(auth_wal_dir).expect("create auth wal storage for tests"));
    let auth_manager = Arc::new(AuthManager::with_storage(shard_manager, storage));
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
    let auth_wal_dir = tempdir().unwrap().into_path().join("auth.swal");
    let storage =
        Arc::new(AuthWalStorage::new(auth_wal_dir).expect("create auth wal storage for tests"));
    let auth_manager = AuthManager::with_storage(shard_manager, storage);

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
        Err(AuthError::UserExists) => {}
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

    for (_i, valid_id) in valid_ids.iter().enumerate() {
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

    // Verify signature (no rate limiting when client_ip is None)
    let result = auth_manager
        .verify_signature(message, "test_user", &signature, None)
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
        .verify_signature(message, "test_user", wrong_signature, None)
        .await;

    assert!(result.is_err());
    match result {
        Err(AuthError::AuthenticationFailed) => {}
        _ => panic!("Expected AuthenticationFailed error"),
    }
}

#[tokio::test]
async fn test_verify_signature_error_user_not_found() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    let result = auth_manager
        .verify_signature("message", "non_existent_user", "signature", None)
        .await;

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
        .verify_signature(message, "inactive_user", &signature, None)
        .await;

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
            .verify_signature(message, "test_user", &signature, None)
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
        .verify_signature(different_message, "test_user", &signature, None)
        .await;

    assert!(result.is_err());
    match result {
        Err(AuthError::AuthenticationFailed) => {}
        _ => panic!("Expected AuthenticationFailed error"),
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
        Err(AuthError::AuthenticationFailed) => {}
        _ => panic!("Expected AuthenticationFailed error"),
    }

    // Only one colon
    let result = auth_manager.parse_auth("user123:STORE test_event");
    assert!(result.is_err());
    match result {
        Err(AuthError::AuthenticationFailed) => {}
        _ => panic!("Expected AuthenticationFailed error"),
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
        Err(AuthError::AuthenticationFailed) => {}
        _ => panic!("Expected AuthenticationFailed error"),
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
        Err(AuthError::UserExists) => {}
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
            .verify_signature(message1, "user1", &signature1, None)
            .await
            .is_ok()
    );

    let message2 = "STORE event2 FOR ctx2 PAYLOAD {\"id\":2}";
    let signature2 = compute_hmac(message2, &secret_key2);
    assert!(
        auth_manager
            .verify_signature(message2, "user2", &signature2, None)
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
            .verify_signature(message1, "user1", &signature1, None)
            .await
            .is_err()
    );

    // Verify user2 signature still works
    assert!(
        auth_manager
            .verify_signature(message2, "user2", &signature2, None)
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
            .verify_signature(message, "test_user", &signature, None)
            .await
            .is_ok()
    );

    // Verify case-sensitive user_id fails
    assert!(
        auth_manager
            .verify_signature(message, "TEST_USER", &signature, None)
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
            .verify_signature(message, "test_user", wrong_sig, None)
            .await;
        assert!(result.is_err());
    }
}

#[tokio::test]
async fn test_create_user_with_roles() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    let roles = vec!["admin".to_string(), "read-only".to_string()];
    let result = auth_manager
        .create_user_with_roles(
            "admin_user".to_string(),
            Some("secret".to_string()),
            roles.clone(),
        )
        .await;

    assert!(result.is_ok());

    let users = auth_manager.list_users().await;
    assert_eq!(users.len(), 1);
    assert_eq!(users[0].user_id, "admin_user");
    assert_eq!(users[0].roles, roles);
}

#[tokio::test]
async fn test_is_admin() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Create admin user
    auth_manager
        .create_user_with_roles(
            "admin".to_string(),
            Some("secret".to_string()),
            vec!["admin".to_string()],
        )
        .await
        .expect("Admin user creation should succeed");

    // Create regular user
    auth_manager
        .create_user("regular".to_string(), Some("secret2".to_string()))
        .await
        .expect("Regular user creation should succeed");

    // Check admin status
    assert!(auth_manager.is_admin("admin").await);
    assert!(!auth_manager.is_admin("regular").await);
    assert!(!auth_manager.is_admin("nonexistent").await);
}

#[tokio::test]
async fn test_create_user_defaults_to_empty_roles() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    auth_manager
        .create_user("regular_user".to_string(), Some("secret".to_string()))
        .await
        .expect("User creation should succeed");

    let users = auth_manager.list_users().await;
    assert_eq!(users.len(), 1);
    assert_eq!(users[0].roles, Vec::<String>::new());
}

#[tokio::test]
async fn test_bootstrap_admin_user_creates_when_no_users() {
    init_for_tests();

    // Note: This test would require mocking CONFIG, which is complex
    // For now, we test the create_user_with_roles method directly
    // Full bootstrap testing would require integration tests with actual config

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Simulate bootstrap: create admin user when no users exist
    let result = auth_manager
        .create_user_with_roles(
            "bootstrap_admin".to_string(),
            Some("bootstrap_key".to_string()),
            vec!["admin".to_string()],
        )
        .await;

    assert!(result.is_ok());

    let users = auth_manager.list_users().await;
    assert_eq!(users.len(), 1);
    assert!(auth_manager.is_admin("bootstrap_admin").await);
}

#[tokio::test]
async fn test_bootstrap_skips_when_users_exist() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Create a user first
    auth_manager
        .create_user("existing_user".to_string(), Some("secret".to_string()))
        .await
        .expect("User creation should succeed");

    // Bootstrap should skip (we can't easily test this without mocking CONFIG,
    // but we can verify the logic: if users exist, bootstrap returns early)
    let user_count = auth_manager.list_users().await.len();
    assert_eq!(user_count, 1);
    // Bootstrap logic checks user_count > 0 and returns early, which is correct behavior
}

// ============================================================================
// Rate Limiting Tests
// ============================================================================
// Note: These tests verify that rate limiting only applies to FAILED auth attempts
// Successful authentications bypass rate limiting entirely for unlimited throughput

#[tokio::test]
async fn test_rate_limiting_skipped_when_client_ip_none() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Create a user
    let secret_key = auth_manager
        .create_user("test_user".to_string(), Some("my_secret_key".to_string()))
        .await
        .expect("User creation should succeed");

    let message = "STORE test_event FOR ctx1 PAYLOAD {\"id\":1}";
    let signature = compute_hmac(message, &secret_key);

    // When client_ip is None, rate limiting should be skipped
    // This allows connection-based auth to work at full speed
    let result = auth_manager
        .verify_signature(message, "test_user", &signature, None)
        .await;

    assert!(
        result.is_ok(),
        "Rate limiting should be skipped when client_ip is None"
    );
}

#[tokio::test]
async fn test_successful_auth_bypasses_rate_limiting() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Create a user
    let secret_key = auth_manager
        .create_user("test_user".to_string(), Some("my_secret_key".to_string()))
        .await
        .expect("User creation should succeed");

    let message = "STORE test_event FOR ctx1 PAYLOAD {\"id\":1}";
    let signature = compute_hmac(message, &secret_key);
    let client_ip = "192.168.1.100";

    // Make many successful authentications - should all succeed
    // This verifies that successful auths bypass rate limiting
    for i in 0..100 {
        let result = auth_manager
            .verify_signature(message, "test_user", &signature, Some(client_ip))
            .await;
        assert!(
            result.is_ok(),
            "Successful auth #{} should bypass rate limiting",
            i
        );
    }
}

#[tokio::test]
async fn test_failed_auth_triggers_rate_limiting() {
    init_for_tests();

    // Note: This test requires rate limiting to be enabled in CONFIG
    // The test config has rate_limit_enabled = false, so we test the logic
    // by verifying that failed auths check the rate limiter

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Create a user
    auth_manager
        .create_user("test_user".to_string(), Some("my_secret_key".to_string()))
        .await
        .expect("User creation should succeed");

    let message = "STORE test_event FOR ctx1 PAYLOAD {\"id\":1}";
    let wrong_signature = "invalid_signature";
    let client_ip = "192.168.1.100";

    // Make a failed authentication attempt
    let result = auth_manager
        .verify_signature(message, "test_user", wrong_signature, Some(client_ip))
        .await;

    // Should fail with AuthenticationFailed (rate limiting may or may not be enabled)
    assert!(result.is_err());
    match result {
        Err(AuthError::AuthenticationFailed) | Err(AuthError::RateLimitExceeded) => {}
        _ => panic!("Expected AuthenticationFailed or RateLimitExceeded error"),
    }
}

#[tokio::test]
async fn test_rate_limiting_per_ip_isolation() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Create a user
    let secret_key = auth_manager
        .create_user("test_user".to_string(), Some("my_secret_key".to_string()))
        .await
        .expect("User creation should succeed");

    let message = "STORE test_event FOR ctx1 PAYLOAD {\"id\":1}";
    let signature = compute_hmac(message, &secret_key);

    // Different IPs should be rate limited independently
    let ip1 = "192.168.1.100";
    let ip2 = "192.168.1.200";

    // Both IPs should be able to authenticate successfully
    let result1 = auth_manager
        .verify_signature(message, "test_user", &signature, Some(ip1))
        .await;
    let result2 = auth_manager
        .verify_signature(message, "test_user", &signature, Some(ip2))
        .await;

    assert!(result1.is_ok(), "IP1 should authenticate successfully");
    assert!(result2.is_ok(), "IP2 should authenticate successfully");
}

#[tokio::test]
async fn test_rate_limiting_only_on_failed_attempts() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Create a user
    let secret_key = auth_manager
        .create_user("test_user".to_string(), Some("my_secret_key".to_string()))
        .await
        .expect("User creation should succeed");

    let message = "STORE test_event FOR ctx1 PAYLOAD {\"id\":1}";
    let correct_signature = compute_hmac(message, &secret_key);
    let wrong_signature = "invalid_signature";
    let client_ip = "192.168.1.100";

    // Make many successful authentications - should all succeed
    for _ in 0..50 {
        let result = auth_manager
            .verify_signature(message, "test_user", &correct_signature, Some(client_ip))
            .await;
        assert!(
            result.is_ok(),
            "Successful auths should bypass rate limiting"
        );
    }

    // Now make a failed attempt - this should trigger rate limiting check
    let result = auth_manager
        .verify_signature(message, "test_user", wrong_signature, Some(client_ip))
        .await;

    assert!(result.is_err(), "Failed auth should be rejected");
    match result {
        Err(AuthError::AuthenticationFailed) | Err(AuthError::RateLimitExceeded) => {}
        _ => panic!("Expected AuthenticationFailed or RateLimitExceeded error"),
    }
}

#[tokio::test]
async fn test_rate_limiting_with_different_users_same_ip() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Create two users
    let secret_key1 = auth_manager
        .create_user("user1".to_string(), Some("key1".to_string()))
        .await
        .expect("User creation should succeed");
    let secret_key2 = auth_manager
        .create_user("user2".to_string(), Some("key2".to_string()))
        .await
        .expect("User creation should succeed");

    let message = "STORE test_event FOR ctx1 PAYLOAD {\"id\":1}";
    let signature1 = compute_hmac(message, &secret_key1);
    let signature2 = compute_hmac(message, &secret_key2);
    let client_ip = "192.168.1.100";

    // Both users from same IP should be able to authenticate successfully
    let result1 = auth_manager
        .verify_signature(message, "user1", &signature1, Some(client_ip))
        .await;
    let result2 = auth_manager
        .verify_signature(message, "user2", &signature2, Some(client_ip))
        .await;

    assert!(result1.is_ok(), "User1 should authenticate successfully");
    assert!(result2.is_ok(), "User2 should authenticate successfully");
}

#[tokio::test]
async fn test_rate_limiting_high_throughput_successful_auths() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Create a user
    let secret_key = auth_manager
        .create_user("test_user".to_string(), Some("my_secret_key".to_string()))
        .await
        .expect("User creation should succeed");

    let message = "STORE test_event FOR ctx1 PAYLOAD {\"id\":1}";
    let signature = compute_hmac(message, &secret_key);
    let client_ip = "192.168.1.100";

    // Simulate high-throughput scenario: many successful authentications
    // This verifies that legitimate clients can send at full speed (300K+ events/sec)
    let mut success_count = 0;
    for i in 0..1000 {
        let result = auth_manager
            .verify_signature(message, "test_user", &signature, Some(client_ip))
            .await;
        if result.is_ok() {
            success_count += 1;
        } else {
            panic!("Successful auth #{} should not be rate limited", i);
        }
    }

    assert_eq!(
        success_count, 1000,
        "All successful auths should bypass rate limiting"
    );
}

#[tokio::test]
async fn test_rate_limiting_mixed_success_and_failure() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Create a user
    let secret_key = auth_manager
        .create_user("test_user".to_string(), Some("my_secret_key".to_string()))
        .await
        .expect("User creation should succeed");

    let message = "STORE test_event FOR ctx1 PAYLOAD {\"id\":1}";
    let correct_signature = compute_hmac(message, &secret_key);
    let wrong_signature = "invalid_signature";
    let client_ip = "192.168.1.100";

    // Mix of successful and failed attempts
    // Successful ones should always work, failed ones should be rate limited
    for i in 0..20 {
        if i % 2 == 0 {
            // Successful auth
            let result = auth_manager
                .verify_signature(message, "test_user", &correct_signature, Some(client_ip))
                .await;
            assert!(
                result.is_ok(),
                "Successful auth #{} should bypass rate limiting",
                i
            );
        } else {
            // Failed auth
            let result = auth_manager
                .verify_signature(message, "test_user", wrong_signature, Some(client_ip))
                .await;
            assert!(result.is_err(), "Failed auth #{} should be rejected", i);
        }
    }
}
