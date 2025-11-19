use crate::command::handlers::auth::handle;
use crate::command::types::Command;
use crate::engine::auth::AuthManager;
use crate::engine::shard::manager::ShardManager;
use crate::logging::init_for_tests;
use crate::shared::response::unix::UnixRenderer;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::io::{AsyncReadExt, duplex};

/// Helper function to create a test AuthManager with temporary directories
async fn create_test_auth_manager() -> (Arc<AuthManager>, tempfile::TempDir) {
    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();
    let shard_manager = Arc::new(ShardManager::new(1, base_dir, wal_dir).await);
    let auth_manager = Arc::new(AuthManager::new(shard_manager));
    let temp_dir = tempdir().unwrap();
    (auth_manager, temp_dir)
}

/// Helper function to create an admin user for testing
async fn create_admin_user(auth_manager: &Arc<AuthManager>) -> String {
    let admin_id = "test_admin";
    auth_manager
        .create_user_with_roles(
            admin_id.to_string(),
            Some("admin_secret".to_string()),
            vec!["admin".to_string()],
        )
        .await
        .expect("Failed to create admin user");
    admin_id.to_string()
}

#[tokio::test]
async fn test_create_user_success_without_secret_key() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;
    let cmd = Command::CreateUser {
        user_id: "test_user".to_string(),
        secret_key: None,
        roles: None,
    };

    let (mut reader, mut writer) = duplex(1024);

    let admin_id = create_admin_user(&auth_manager).await;
    handle(
        &cmd,
        &auth_manager,
        Some(&admin_id),
        &mut writer,
        &UnixRenderer,
    )
    .await
    .expect("handler should not fail");

    // Read response
    let mut response = vec![0u8; 1024];
    let n = reader.read(&mut response).await.unwrap();
    let msg = String::from_utf8_lossy(&response[..n]);

    // Verify response contains success message and secret key
    assert!(msg.contains("200 OK"));
    assert!(msg.contains("User 'test_user' created"));
    assert!(msg.contains("Secret key: "));

    // Verify user was created in cache (admin + test_user = 2 users)
    let users = auth_manager.list_users().await;
    assert_eq!(users.len(), 2);
    let test_user = users
        .iter()
        .find(|u| u.user_id == "test_user")
        .expect("test_user should exist");
    assert!(test_user.active);
}

#[tokio::test]
async fn test_create_user_success_with_secret_key() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;
    let provided_key = "my_secret_key_12345";
    let cmd = Command::CreateUser {
        user_id: "test_user2".to_string(),
        secret_key: Some(provided_key.to_string()),
        roles: None,
    };

    let (mut reader, mut writer) = duplex(1024);

    let admin_id = create_admin_user(&auth_manager).await;
    handle(
        &cmd,
        &auth_manager,
        Some(&admin_id),
        &mut writer,
        &UnixRenderer,
    )
    .await
    .expect("handler should not fail");

    // Read response
    let mut response = vec![0u8; 1024];
    let n = reader.read(&mut response).await.unwrap();
    let msg = String::from_utf8_lossy(&response[..n]);

    // Verify response contains success message and provided secret key
    assert!(msg.contains("200 OK"));
    assert!(msg.contains("User 'test_user2' created"));
    assert!(msg.contains(&format!("Secret key: {}", provided_key)));

    // Verify user was created with provided key (admin + test_user2 = 2 users)
    let users = auth_manager.list_users().await;
    assert_eq!(users.len(), 2);
    let test_user2 = users
        .iter()
        .find(|u| u.user_id == "test_user2")
        .expect("test_user2 should exist");
    assert_eq!(test_user2.secret_key, provided_key);
    assert!(test_user2.active);
}

#[tokio::test]
async fn test_create_user_error_user_exists() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Create admin user first
    let admin_id = create_admin_user(&auth_manager).await;

    // Create first user
    let cmd1 = Command::CreateUser {
        user_id: "existing_user".to_string(),
        secret_key: None,
        roles: None,
    };
    let (_reader1, mut writer1) = duplex(1024);
    handle(
        &cmd1,
        &auth_manager,
        Some(&admin_id),
        &mut writer1,
        &UnixRenderer,
    )
    .await
    .expect("handler should not fail");

    // Try to create same user again
    let cmd2 = Command::CreateUser {
        user_id: "existing_user".to_string(),
        secret_key: None,
        roles: None,
    };
    let (mut reader2, mut writer2) = duplex(1024);

    handle(
        &cmd2,
        &auth_manager,
        Some(&admin_id),
        &mut writer2,
        &UnixRenderer,
    )
    .await
    .expect("handler should not fail");

    // Read response
    let mut response = vec![0u8; 1024];
    let n = reader2.read(&mut response).await.unwrap();
    let msg = String::from_utf8_lossy(&response[..n]);

    // Verify error response
    // Note: Error message doesn't include user_id to prevent user enumeration
    assert!(msg.contains("400"));
    assert!(msg.contains("User already exists"));

    // Verify only admin and existing_user exist (2 users)
    let users = auth_manager.list_users().await;
    assert_eq!(users.len(), 2);
}

#[tokio::test]
async fn test_create_user_error_invalid_user_id() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Try to create user with invalid ID (contains special characters)
    let cmd = Command::CreateUser {
        user_id: "invalid@user#123".to_string(),
        secret_key: None,
        roles: None,
    };
    let (mut reader, mut writer) = duplex(1024);

    let admin_id = create_admin_user(&auth_manager).await;
    handle(
        &cmd,
        &auth_manager,
        Some(&admin_id),
        &mut writer,
        &UnixRenderer,
    )
    .await
    .expect("handler should not fail");

    // Read response
    let mut response = vec![0u8; 1024];
    let n = reader.read(&mut response).await.unwrap();
    let msg = String::from_utf8_lossy(&response[..n]);

    // Verify error response
    assert!(msg.contains("400"));
    assert!(msg.contains("Invalid user ID format"));

    // Verify no users were created (only admin user exists)
    let users = auth_manager.list_users().await;
    assert_eq!(users.len(), 1); // Only admin user
}

#[tokio::test]
async fn test_create_user_valid_user_id_with_underscore_and_hyphen() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;
    let admin_id = create_admin_user(&auth_manager).await;

    // Test user IDs with underscore and hyphen (should be valid)
    let test_cases = vec!["user_123", "user-456", "user_123-456", "test-user_name"];

    for user_id in &test_cases {
        let cmd = Command::CreateUser {
            user_id: user_id.to_string(),
            secret_key: None,
            roles: None,
        };
        let (_reader, mut writer) = duplex(1024);

        handle(
            &cmd,
            &auth_manager,
            Some(&admin_id),
            &mut writer,
            &UnixRenderer,
        )
        .await
        .expect("handler should not fail");
    }

    // Verify all users were created (admin + test users)
    let users = auth_manager.list_users().await;
    assert_eq!(users.len(), test_cases.len() + 1); // Admin user + test users
}

#[tokio::test]
async fn test_revoke_key_success() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Create a user first
    let create_cmd = Command::CreateUser {
        user_id: "user_to_revoke".to_string(),
        secret_key: None,
        roles: None,
    };
    let (_reader1, mut writer1) = duplex(1024);
    let admin_id = create_admin_user(&auth_manager).await;
    handle(
        &create_cmd,
        &auth_manager,
        Some(&admin_id),
        &mut writer1,
        &UnixRenderer,
    )
    .await
    .expect("handler should not fail");

    // Verify user is active (admin + test user)
    let users_before = auth_manager.list_users().await;
    assert_eq!(users_before.len(), 2); // Admin + test user
    // Find the test user (not admin)
    let test_user = users_before
        .iter()
        .find(|u| u.user_id == "user_to_revoke")
        .unwrap();
    assert!(test_user.active);

    // Revoke the key
    let revoke_cmd = Command::RevokeKey {
        user_id: "user_to_revoke".to_string(),
    };
    let (mut reader2, mut writer2) = duplex(1024);

    handle(
        &revoke_cmd,
        &auth_manager,
        Some(&admin_id),
        &mut writer2,
        &UnixRenderer,
    )
    .await
    .expect("handler should not fail");

    // Read response
    let mut response = vec![0u8; 1024];
    let n = reader2.read(&mut response).await.unwrap();
    let msg = String::from_utf8_lossy(&response[..n]);

    // Verify success response
    assert!(msg.contains("200 OK"));
    assert!(msg.contains("Key revoked for user 'user_to_revoke'"));

    // Verify user is now inactive (admin + revoked user)
    let users_after = auth_manager.list_users().await;
    assert_eq!(users_after.len(), 2); // Admin + revoked user
    // Find the revoked user (not admin)
    let revoked_user = users_after
        .iter()
        .find(|u| u.user_id == "user_to_revoke")
        .unwrap();
    assert!(!revoked_user.active);
}

#[tokio::test]
async fn test_revoke_key_error_user_not_found() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Try to revoke key for non-existent user
    let cmd = Command::RevokeKey {
        user_id: "non_existent_user".to_string(),
    };
    let (mut reader, mut writer) = duplex(1024);

    let admin_id = create_admin_user(&auth_manager).await;
    handle(
        &cmd,
        &auth_manager,
        Some(&admin_id),
        &mut writer,
        &UnixRenderer,
    )
    .await
    .expect("handler should not fail");

    // Read response
    let mut response = vec![0u8; 1024];
    let n = reader.read(&mut response).await.unwrap();
    let msg = String::from_utf8_lossy(&response[..n]);

    // Verify error response
    assert!(msg.contains("400"));
    assert!(msg.contains("User not found: non_existent_user"));
}

#[tokio::test]
async fn test_list_users_empty() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    let cmd = Command::ListUsers;
    let (mut reader, mut writer) = duplex(1024);

    let admin_id = create_admin_user(&auth_manager).await;
    handle(
        &cmd,
        &auth_manager,
        Some(&admin_id),
        &mut writer,
        &UnixRenderer,
    )
    .await
    .expect("handler should not fail");

    // Read response
    let mut response = vec![0u8; 1024];
    let n = reader.read(&mut response).await.unwrap();
    let msg = String::from_utf8_lossy(&response[..n]);

    // Verify response contains admin user (since we created one)
    assert!(msg.contains("200 OK"));
    assert!(msg.contains("test_admin")); // Admin user should be listed
}

#[tokio::test]
async fn test_list_users_with_multiple_users() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;
    let admin_id = create_admin_user(&auth_manager).await;

    // Create multiple users
    let users = vec!["user1", "user2", "user3"];
    for user_id in &users {
        let create_cmd = Command::CreateUser {
            user_id: user_id.to_string(),
            secret_key: None,
            roles: None,
        };
        let (_reader, mut writer) = duplex(1024);
        handle(
            &create_cmd,
            &auth_manager,
            Some(&admin_id),
            &mut writer,
            &UnixRenderer,
        )
        .await
        .expect("handler should not fail");
    }

    // List users
    let list_cmd = Command::ListUsers;
    let (mut reader, mut writer) = duplex(1024);

    handle(
        &list_cmd,
        &auth_manager,
        Some(&admin_id),
        &mut writer,
        &UnixRenderer,
    )
    .await
    .expect("handler should not fail");

    // Read response
    let mut response = vec![0u8; 1024];
    let n = reader.read(&mut response).await.unwrap();
    let msg = String::from_utf8_lossy(&response[..n]);

    // Verify response contains all users
    assert!(msg.contains("200 OK"));
    for user_id in &users {
        assert!(msg.contains(user_id));
        assert!(msg.contains("active"));
    }
}

#[tokio::test]
async fn test_list_users_shows_active_and_inactive() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Create two users
    let create_cmd1 = Command::CreateUser {
        user_id: "active_user".to_string(),
        secret_key: None,
        roles: None,
    };
    let (_reader1, mut writer1) = duplex(1024);
    let admin_id = create_admin_user(&auth_manager).await;
    handle(
        &create_cmd1,
        &auth_manager,
        Some(&admin_id),
        &mut writer1,
        &UnixRenderer,
    )
    .await
    .expect("handler should not fail");

    let create_cmd2 = Command::CreateUser {
        user_id: "inactive_user".to_string(),
        secret_key: None,
        roles: None,
    };
    let (_reader2, mut writer2) = duplex(1024);
    handle(
        &create_cmd2,
        &auth_manager,
        Some(&admin_id),
        &mut writer2,
        &UnixRenderer,
    )
    .await
    .expect("handler should not fail");

    // Revoke one user's key
    let revoke_cmd = Command::RevokeKey {
        user_id: "inactive_user".to_string(),
    };
    let (_reader3, mut writer3) = duplex(1024);
    handle(
        &revoke_cmd,
        &auth_manager,
        Some(&admin_id),
        &mut writer3,
        &UnixRenderer,
    )
    .await
    .expect("handler should not fail");

    // List users
    let list_cmd = Command::ListUsers;
    let (mut reader, mut writer) = duplex(1024);

    // Use existing admin_id, don't create another one
    handle(
        &list_cmd,
        &auth_manager,
        Some(&admin_id),
        &mut writer,
        &UnixRenderer,
    )
    .await
    .expect("handler should not fail");

    // Read response
    let mut response = vec![0u8; 1024];
    let n = reader.read(&mut response).await.unwrap();
    let msg = String::from_utf8_lossy(&response[..n]);

    // Verify response shows both active and inactive users
    assert!(msg.contains("200 OK"));
    assert!(msg.contains("active_user"));
    assert!(msg.contains("inactive_user"));
    assert!(msg.contains("active"));
    assert!(msg.contains("inactive"));
}

#[tokio::test]
async fn test_invalid_command_variant() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Try to handle a non-auth command (should return error)
    let cmd = Command::Store {
        event_type: "test_event".to_string(),
        context_id: "ctx1".to_string(),
        payload: serde_json::json!({"id": 1}),
    };
    let (mut reader, mut writer) = duplex(1024);

    let admin_id = create_admin_user(&auth_manager).await;
    handle(
        &cmd,
        &auth_manager,
        Some(&admin_id),
        &mut writer,
        &UnixRenderer,
    )
    .await
    .expect("handler should not fail");

    // Read response
    let mut response = vec![0u8; 1024];
    let n = reader.read(&mut response).await.unwrap();
    let msg = String::from_utf8_lossy(&response[..n]);

    // Verify error response
    assert!(msg.contains("400"));
    assert!(msg.contains("Invalid command variant"));
}

#[tokio::test]
async fn test_response_formatting_unix_renderer() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Test CreateUser response format
    let cmd = Command::CreateUser {
        user_id: "format_test".to_string(),
        secret_key: Some("test_key".to_string()),
        roles: None,
    };
    let (mut reader, mut writer) = duplex(1024);

    let admin_id = create_admin_user(&auth_manager).await;
    handle(
        &cmd,
        &auth_manager,
        Some(&admin_id),
        &mut writer,
        &UnixRenderer,
    )
    .await
    .expect("handler should not fail");

    // Read response
    let mut response = vec![0u8; 1024];
    let n = reader.read(&mut response).await.unwrap();
    let msg = String::from_utf8_lossy(&response[..n]);

    // Verify Unix format: status line followed by lines
    let lines: Vec<&str> = msg.lines().collect();
    assert!(lines[0].starts_with("200"));
    assert!(lines.len() >= 2); // At least status + 2 content lines
}

#[tokio::test]
async fn test_create_user_then_revoke_then_create_again() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Create user
    let create_cmd1 = Command::CreateUser {
        user_id: "recreate_user".to_string(),
        secret_key: None,
        roles: None,
    };
    let (_reader1, mut writer1) = duplex(1024);
    let admin_id = create_admin_user(&auth_manager).await;
    handle(
        &create_cmd1,
        &auth_manager,
        Some(&admin_id),
        &mut writer1,
        &UnixRenderer,
    )
    .await
    .expect("handler should not fail");

    // Revoke key
    let revoke_cmd = Command::RevokeKey {
        user_id: "recreate_user".to_string(),
    };
    let (_reader2, mut writer2) = duplex(1024);
    handle(
        &revoke_cmd,
        &auth_manager,
        Some(&admin_id),
        &mut writer2,
        &UnixRenderer,
    )
    .await
    .expect("handler should not fail");

    // Try to create same user again (should fail - user still exists)
    let create_cmd2 = Command::CreateUser {
        user_id: "recreate_user".to_string(),
        secret_key: None,
        roles: None,
    };
    let (mut reader3, mut writer3) = duplex(1024);

    handle(
        &create_cmd2,
        &auth_manager,
        Some(&admin_id),
        &mut writer3,
        &UnixRenderer,
    )
    .await
    .expect("handler should not fail");

    // Read response
    let mut response = vec![0u8; 1024];
    let n = reader3.read(&mut response).await.unwrap();
    let msg = String::from_utf8_lossy(&response[..n]);

    // Verify error - user still exists (revoking doesn't delete)
    // Note: Error message doesn't include user_id to prevent user enumeration
    assert!(msg.contains("400"));
    assert!(msg.contains("User already exists"));
}

#[tokio::test]
async fn test_create_user_empty_user_id() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Try to create user with empty ID
    let cmd = Command::CreateUser {
        user_id: "".to_string(),
        secret_key: None,
        roles: None,
    };
    let (mut reader, mut writer) = duplex(1024);

    let admin_id = create_admin_user(&auth_manager).await;
    handle(
        &cmd,
        &auth_manager,
        Some(&admin_id),
        &mut writer,
        &UnixRenderer,
    )
    .await
    .expect("handler should not fail");

    // Read response
    let mut response = vec![0u8; 1024];
    let n = reader.read(&mut response).await.unwrap();
    let msg = String::from_utf8_lossy(&response[..n]);

    // Verify error response
    assert!(msg.contains("400"));
    assert!(msg.contains("Invalid user ID format"));
}

#[tokio::test]
async fn test_revoke_key_empty_user_id() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Try to revoke key with empty user ID
    let cmd = Command::RevokeKey {
        user_id: "".to_string(),
    };
    let (mut reader, mut writer) = duplex(1024);

    let admin_id = create_admin_user(&auth_manager).await;
    handle(
        &cmd,
        &auth_manager,
        Some(&admin_id),
        &mut writer,
        &UnixRenderer,
    )
    .await
    .expect("handler should not fail");

    // Read response
    let mut response = vec![0u8; 1024];
    let n = reader.read(&mut response).await.unwrap();
    let msg = String::from_utf8_lossy(&response[..n]);

    // Verify error response
    assert!(msg.contains("400"));
    assert!(msg.contains("User not found"));
}

#[tokio::test]
async fn test_multiple_operations_sequence() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Create user1
    let cmd1 = Command::CreateUser {
        user_id: "user1".to_string(),
        secret_key: Some("key1".to_string()),
        roles: None,
    };
    let (_reader1, mut writer1) = duplex(1024);
    let admin_id = create_admin_user(&auth_manager).await;
    handle(
        &cmd1,
        &auth_manager,
        Some(&admin_id),
        &mut writer1,
        &UnixRenderer,
    )
    .await
    .expect("handler should not fail");

    // Create user2
    let cmd2 = Command::CreateUser {
        user_id: "user2".to_string(),
        secret_key: Some("key2".to_string()),
        roles: None,
    };
    let (_reader2, mut writer2) = duplex(1024);
    handle(
        &cmd2,
        &auth_manager,
        Some(&admin_id),
        &mut writer2,
        &UnixRenderer,
    )
    .await
    .expect("handler should not fail");

    // List users (should show 2)
    let list_cmd = Command::ListUsers;
    let (mut reader3, mut writer3) = duplex(1024);
    handle(
        &list_cmd,
        &auth_manager,
        Some(&admin_id),
        &mut writer3,
        &UnixRenderer,
    )
    .await
    .expect("handler should not fail");

    let mut response = vec![0u8; 1024];
    let n = reader3.read(&mut response).await.unwrap();
    let msg = String::from_utf8_lossy(&response[..n]);
    assert!(msg.contains("user1"));
    assert!(msg.contains("user2"));

    // Revoke user1
    let revoke_cmd = Command::RevokeKey {
        user_id: "user1".to_string(),
    };
    let (_reader4, mut writer4) = duplex(1024);
    handle(
        &revoke_cmd,
        &auth_manager,
        Some(&admin_id),
        &mut writer4,
        &UnixRenderer,
    )
    .await
    .expect("handler should not fail");

    // List users again (should still show 2, but user1 inactive)
    let list_cmd2 = Command::ListUsers;
    let (mut reader5, mut writer5) = duplex(1024);
    handle(
        &list_cmd2,
        &auth_manager,
        Some(&admin_id),
        &mut writer5,
        &UnixRenderer,
    )
    .await
    .expect("handler should not fail");

    let mut response2 = vec![0u8; 1024];
    let n2 = reader5.read(&mut response2).await.unwrap();
    let msg2 = String::from_utf8_lossy(&response2[..n2]);
    assert!(msg2.contains("user1"));
    assert!(msg2.contains("user2"));
    assert!(msg2.contains("inactive"));
    assert!(msg2.contains("active"));
}

#[tokio::test]
async fn test_auth_handler_bypass_auth_allows_user_management() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Test that bypass user can create users (should succeed even without admin role)
    let cmd = Command::CreateUser {
        user_id: "bypass_created_user".to_string(),
        secret_key: None,
        roles: None,
    };

    let (mut reader, mut writer) = duplex(1024);

    handle(
        &cmd,
        &auth_manager,
        Some("bypass"),
        &mut writer,
        &UnixRenderer,
    )
    .await
    .expect("handler should not fail");

    // Read response
    let mut response = vec![0u8; 1024];
    let n = reader.read(&mut response).await.unwrap();
    let msg = String::from_utf8_lossy(&response[..n]);

    // Verify success response
    assert!(msg.contains("200 OK"));
    assert!(msg.contains("User 'bypass_created_user' created"));

    // Verify user was created
    let users = auth_manager.list_users().await;
    assert!(users.iter().any(|u| u.user_id == "bypass_created_user"));
}

#[tokio::test]
async fn test_auth_handler_bypass_auth_vs_regular_user() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;

    // Create a regular (non-admin) user
    auth_manager
        .create_user("regular_user".to_string(), Some("secret".to_string()))
        .await
        .unwrap();

    let cmd = Command::CreateUser {
        user_id: "test_user".to_string(),
        secret_key: None,
        roles: None,
    };

    // Test with regular user (should fail - not admin)
    let (mut reader1, mut writer1) = duplex(1024);
    handle(
        &cmd,
        &auth_manager,
        Some("regular_user"),
        &mut writer1,
        &UnixRenderer,
    )
    .await
    .expect("handler should not fail");

    let mut response = vec![0u8; 1024];
    let n = reader1.read(&mut response).await.unwrap();
    let msg = String::from_utf8_lossy(&response[..n]);
    assert!(msg.contains("403") || msg.contains("Forbidden"));
    assert!(msg.contains("Only admin users can manage users"));

    // Test with bypass user (should succeed)
    let (mut reader2, mut writer2) = duplex(1024);
    handle(
        &cmd,
        &auth_manager,
        Some("bypass"),
        &mut writer2,
        &UnixRenderer,
    )
    .await
    .expect("handler should not fail");

    let mut response2 = vec![0u8; 1024];
    let n2 = reader2.read(&mut response2).await.unwrap();
    let msg2 = String::from_utf8_lossy(&response2[..n2]);
    assert!(msg2.contains("200 OK"));
    assert!(msg2.contains("User 'test_user' created"));
}

#[tokio::test]
async fn test_create_user_with_roles() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;
    let cmd = Command::CreateUser {
        user_id: "admin_user".to_string(),
        secret_key: Some("secret".to_string()),
        roles: Some(vec!["admin".to_string(), "read-only".to_string()]),
    };

    let (mut reader, mut writer) = duplex(1024);

    let admin_id = create_admin_user(&auth_manager).await;
    handle(
        &cmd,
        &auth_manager,
        Some(&admin_id),
        &mut writer,
        &UnixRenderer,
    )
    .await
    .expect("handler should not fail");

    // Read response
    let mut response = vec![0u8; 1024];
    let n = reader.read(&mut response).await.unwrap();
    let msg = String::from_utf8_lossy(&response[..n]);

    // Verify success response
    assert!(msg.contains("200 OK"));
    assert!(msg.contains("User 'admin_user' created"));

    // Verify user was created with roles
    let users = auth_manager.list_users().await;
    let created_user = users
        .iter()
        .find(|u| u.user_id == "admin_user")
        .expect("admin_user should exist");
    assert_eq!(
        created_user.roles,
        vec!["admin".to_string(), "read-only".to_string()]
    );
    assert!(auth_manager.is_admin("admin_user").await);
}

#[tokio::test]
async fn test_create_user_without_roles_defaults_to_empty() {
    init_for_tests();

    let (auth_manager, _temp_dir) = create_test_auth_manager().await;
    let cmd = Command::CreateUser {
        user_id: "regular_user".to_string(),
        secret_key: Some("secret".to_string()),
        roles: None,
    };

    let (_reader, mut writer) = duplex(1024);

    let admin_id = create_admin_user(&auth_manager).await;
    handle(
        &cmd,
        &auth_manager,
        Some(&admin_id),
        &mut writer,
        &UnixRenderer,
    )
    .await
    .expect("handler should not fail");

    // Verify user was created with empty roles
    let users = auth_manager.list_users().await;
    let created_user = users
        .iter()
        .find(|u| u.user_id == "regular_user")
        .expect("regular_user should exist");
    assert_eq!(created_user.roles, Vec::<String>::new());
    assert!(!auth_manager.is_admin("regular_user").await);
}
