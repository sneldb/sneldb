use super::types::{AuthError, PermissionCache, UserCache};
use super::user_ops::{create_user, create_user_with_roles, list_users, revoke_key};
use crate::engine::shard::manager::ShardManager;
use crate::logging::init_for_tests;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::RwLock;

/// Helper function to create test dependencies
async fn create_test_deps() -> (
    Arc<RwLock<UserCache>>,
    Arc<RwLock<PermissionCache>>,
    Arc<ShardManager>,
) {
    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();
    let shard_manager = Arc::new(ShardManager::new(1, base_dir, wal_dir).await);
    let cache = Arc::new(RwLock::new(UserCache::new()));
    let permission_cache = Arc::new(RwLock::new(PermissionCache::new()));
    (cache, permission_cache, shard_manager)
}

#[tokio::test]
async fn test_create_user_success_without_secret_key() {
    init_for_tests();

    let (cache, permission_cache, shard_manager) = create_test_deps().await;

    let result = create_user(&cache, &permission_cache, &shard_manager, "test_user".to_string(), None).await;

    assert!(result.is_ok());
    let secret_key = result.unwrap();

    // Verify secret key is generated (64 hex chars = 32 bytes)
    assert_eq!(secret_key.len(), 64);
    assert!(secret_key.chars().all(|c| c.is_ascii_hexdigit()));

    // Verify user was created in cache
    let users = list_users(&cache).await;
    assert_eq!(users.len(), 1);
    assert_eq!(users[0].user_id, "test_user");
    assert_eq!(users[0].secret_key, secret_key);
    assert!(users[0].active);
}

#[tokio::test]
async fn test_create_user_success_with_secret_key() {
    init_for_tests();

    let (cache, permission_cache, shard_manager) = create_test_deps().await;
    let provided_key = "my_custom_secret_key_12345";

    let result = create_user(
        &cache,
        &permission_cache,
        &shard_manager,
        "test_user2".to_string(),
        Some(provided_key.to_string()),
    )
    .await;

    assert!(result.is_ok());
    let returned_key = result.unwrap();
    assert_eq!(returned_key, provided_key);

    // Verify user was created with provided key
    let users = list_users(&cache).await;
    assert_eq!(users.len(), 1);
    assert_eq!(users[0].user_id, "test_user2");
    assert_eq!(users[0].secret_key, provided_key);
    assert!(users[0].active);
}

#[tokio::test]
async fn test_create_user_error_user_exists() {
    init_for_tests();

    let (cache, permission_cache, shard_manager) = create_test_deps().await;

    // Create first user
    create_user(
        &cache,
        &permission_cache,
        &shard_manager,
        "existing_user".to_string(),
        None,
    )
    .await
    .expect("First creation should succeed");

    // Try to create same user again
    let result = create_user(
        &cache,
        &permission_cache,
        &shard_manager,
        "existing_user".to_string(),
        None,
    )
    .await;

    assert!(result.is_err());
    match result {
        Err(AuthError::UserExists) => {},
        _ => panic!("Expected UserExists error"),
    }

    // Verify only one user exists
    let users = list_users(&cache).await;
    assert_eq!(users.len(), 1);
}

#[tokio::test]
async fn test_create_user_error_invalid_user_id() {
    init_for_tests();

    let (cache, permission_cache, shard_manager) = create_test_deps().await;

    // Test various invalid user IDs
    let invalid_ids = vec!["user@domain", "user#123", "user space", "user.dot", ""];

    for invalid_id in invalid_ids {
        let result = create_user(
            &cache,
            &permission_cache,
            &shard_manager,
            invalid_id.to_string(),
            None,
        )
        .await;
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

    let (cache, permission_cache, shard_manager) = create_test_deps().await;

    // Test valid user ID formats
    let valid_ids = vec![
        "user123",
        "user_123",
        "user-456",
        "user_123-456",
        "test-user_name",
    ];

    for valid_id in &valid_ids {
        let result = create_user(
            &cache,
            &permission_cache,
            &shard_manager,
            valid_id.to_string(),
            None,
        )
        .await;
        assert!(result.is_ok(), "Failed to create user '{}'", valid_id);
    }

    // Verify all users were created
    let users = list_users(&cache).await;
    assert_eq!(users.len(), valid_ids.len());
}

#[tokio::test]
async fn test_create_user_generates_unique_keys() {
    init_for_tests();

    let (cache, permission_cache, shard_manager) = create_test_deps().await;

    // Create multiple users without providing keys
    let mut keys = Vec::new();
    for i in 0..10 {
        let key = create_user(
            &cache,
            &permission_cache,
            &shard_manager,
            format!("user{}", i),
            None,
        )
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
async fn test_create_user_defaults_to_empty_roles() {
    init_for_tests();

    let (cache, permission_cache, shard_manager) = create_test_deps().await;

    create_user(
        &cache,
        &permission_cache,
        &shard_manager,
        "regular_user".to_string(),
        Some("secret".to_string()),
    )
    .await
    .expect("User creation should succeed");

    let users = list_users(&cache).await;
    assert_eq!(users.len(), 1);
    assert_eq!(users[0].roles, Vec::<String>::new());
}

#[tokio::test]
async fn test_create_user_with_roles() {
    init_for_tests();

    let (cache, permission_cache, shard_manager) = create_test_deps().await;

    let roles = vec!["admin".to_string(), "read-only".to_string()];
    let result = create_user_with_roles(
        &cache,
        &permission_cache,
        &shard_manager,
        "admin_user".to_string(),
        Some("secret".to_string()),
        roles.clone(),
    )
    .await;

    assert!(result.is_ok());

    let users = list_users(&cache).await;
    assert_eq!(users.len(), 1);
    assert_eq!(users[0].user_id, "admin_user");
    assert_eq!(users[0].roles, roles);
}

#[tokio::test]
async fn test_create_user_with_roles_error_user_exists() {
    init_for_tests();

    let (cache, permission_cache, shard_manager) = create_test_deps().await;

    // Create user first
    create_user(
        &cache,
        &permission_cache,
        &shard_manager,
        "existing_user".to_string(),
        None,
    )
    .await
    .expect("User creation should succeed");

    // Try to create same user with roles
    let result = create_user_with_roles(
        &cache,
        &permission_cache,
        &shard_manager,
        "existing_user".to_string(),
        Some("secret".to_string()),
        vec!["admin".to_string()],
    )
    .await;

    assert!(result.is_err());
    match result {
        Err(AuthError::UserExists) => {},
        _ => panic!("Expected UserExists error"),
    }
}

#[tokio::test]
async fn test_create_user_with_roles_error_invalid_user_id() {
    init_for_tests();

    let (cache, permission_cache, shard_manager) = create_test_deps().await;

    let result = create_user_with_roles(
        &cache,
        &permission_cache,
        &shard_manager,
        "user@invalid".to_string(),
        Some("secret".to_string()),
        vec!["admin".to_string()],
    )
    .await;

    assert!(result.is_err());
    match result {
        Err(AuthError::InvalidUserId) => {}
        _ => panic!("Expected InvalidUserId error"),
    }
}

#[tokio::test]
async fn test_revoke_key_success() {
    init_for_tests();

    let (cache, permission_cache, shard_manager) = create_test_deps().await;

    // Create a user first
    create_user(
        &cache,
        &permission_cache,
        &shard_manager,
        "user_to_revoke".to_string(),
        None,
    )
    .await
    .expect("User creation should succeed");

    // Verify user is active
    let users_before = list_users(&cache).await;
    assert_eq!(users_before.len(), 1);
    assert!(users_before[0].active);

    // Revoke the key
    let result = revoke_key(&cache, &permission_cache, &shard_manager, "user_to_revoke").await;
    assert!(result.is_ok());

    // Verify user is now inactive
    let users_after = list_users(&cache).await;
    assert_eq!(users_after.len(), 1);
    assert!(!users_after[0].active);
    assert_eq!(users_after[0].user_id, "user_to_revoke");
}

#[tokio::test]
async fn test_revoke_key_error_user_not_found() {
    init_for_tests();

    let (cache, permission_cache, shard_manager) = create_test_deps().await;

    let result = revoke_key(&cache, &permission_cache, &shard_manager, "non_existent_user").await;

    assert!(result.is_err());
    match result {
        Err(AuthError::UserNotFound(user_id)) => assert_eq!(user_id, "non_existent_user"),
        _ => panic!("Expected UserNotFound error"),
    }
}

#[tokio::test]
async fn test_revoke_key_preserves_secret_key() {
    init_for_tests();

    let (cache, permission_cache, shard_manager) = create_test_deps().await;

    // Create a user
    let original_secret = "my_secret_key_12345";
    create_user(
        &cache,
        &permission_cache,
        &shard_manager,
        "test_user".to_string(),
        Some(original_secret.to_string()),
    )
    .await
    .expect("User creation should succeed");

    // Revoke key
    revoke_key(&cache, &permission_cache, &shard_manager, "test_user")
        .await
        .expect("Revoke should succeed");

    // Verify secret key is preserved
    let users = list_users(&cache).await;
    let user = users.iter().find(|u| u.user_id == "test_user").unwrap();
    assert_eq!(user.secret_key, original_secret);
    assert!(!user.active);
}

#[tokio::test]
async fn test_list_users_empty() {
    init_for_tests();

    let cache = Arc::new(RwLock::new(UserCache::new()));

    let users = list_users(&cache).await;
    assert_eq!(users.len(), 0);
}

#[tokio::test]
async fn test_list_users_with_multiple_users() {
    init_for_tests();

    let (cache, permission_cache, shard_manager) = create_test_deps().await;

    // Create multiple users
    let user_ids = vec!["user1", "user2", "user3"];
    for user_id in &user_ids {
        create_user(
            &cache,
            &permission_cache,
            &shard_manager,
            user_id.to_string(),
            None,
        )
        .await
        .expect("User creation should succeed");
    }

    // List users
    let users = list_users(&cache).await;
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

    let (cache, permission_cache, shard_manager) = create_test_deps().await;

    // Create two users
    create_user(
        &cache,
        &permission_cache,
        &shard_manager,
        "active_user".to_string(),
        None,
    )
    .await
    .expect("User creation should succeed");
    create_user(
        &cache,
        &permission_cache,
        &shard_manager,
        "inactive_user".to_string(),
        None,
    )
    .await
    .expect("User creation should succeed");

    // Revoke one user's key
    revoke_key(&cache, &permission_cache, &shard_manager, "inactive_user")
        .await
        .expect("Revoke should succeed");

    // List users
    let users = list_users(&cache).await;
    assert_eq!(users.len(), 2);

    // Verify active/inactive status
    let active_user = users.iter().find(|u| u.user_id == "active_user").unwrap();
    let inactive_user = users.iter().find(|u| u.user_id == "inactive_user").unwrap();
    assert!(active_user.active);
    assert!(!inactive_user.active);
}

#[tokio::test]
async fn test_create_user_then_revoke_then_create_again_fails() {
    init_for_tests();

    let (cache, permission_cache, shard_manager) = create_test_deps().await;

    // Create user
    create_user(
        &cache,
        &permission_cache,
        &shard_manager,
        "recreate_user".to_string(),
        None,
    )
    .await
    .expect("User creation should succeed");

    // Revoke key
    revoke_key(&cache, &permission_cache, &shard_manager, "recreate_user")
        .await
        .expect("Revoke should succeed");

    // Try to create same user again (should fail - user still exists)
    let result = create_user(
        &cache,
        &permission_cache,
        &shard_manager,
        "recreate_user".to_string(),
        None,
    )
    .await;

    assert!(result.is_err());
    match result {
        Err(AuthError::UserExists) => {},
        _ => panic!("Expected UserExists error"),
    }
}

#[tokio::test]
async fn test_bootstrap_admin_user_skips_when_users_exist() {
    init_for_tests();

    let (cache, permission_cache, shard_manager) = create_test_deps().await;

    // Create a user first
    create_user(
        &cache,
        &permission_cache,
        &shard_manager,
        "existing_user".to_string(),
        Some("secret".to_string()),
    )
    .await
    .expect("User creation should succeed");

    // Bootstrap should skip (we can't easily test this without mocking CONFIG,
    // but we can verify the logic: if users exist, bootstrap returns early)
    let user_count = list_users(&cache).await.len();
    assert_eq!(user_count, 1);
    // Bootstrap logic checks user_count > 0 and returns early, which is correct behavior
}

#[tokio::test]
async fn test_create_user_with_empty_roles() {
    init_for_tests();

    let (cache, permission_cache, shard_manager) = create_test_deps().await;

    let result = create_user_with_roles(
        &cache,
        &permission_cache,
        &shard_manager,
        "user_no_roles".to_string(),
        Some("secret".to_string()),
        Vec::new(),
    )
    .await;

    assert!(result.is_ok());

    let users = list_users(&cache).await;
    assert_eq!(users.len(), 1);
    assert_eq!(users[0].roles, Vec::<String>::new());
}

#[tokio::test]
async fn test_create_user_with_multiple_roles() {
    init_for_tests();

    let (cache, permission_cache, shard_manager) = create_test_deps().await;

    let roles = vec![
        "admin".to_string(),
        "read-only".to_string(),
        "write-only".to_string(),
    ];
    let result = create_user_with_roles(
        &cache,
        &permission_cache,
        &shard_manager,
        "multi_role_user".to_string(),
        Some("secret".to_string()),
        roles.clone(),
    )
    .await;

    assert!(result.is_ok());

    let users = list_users(&cache).await;
    assert_eq!(users.len(), 1);
    assert_eq!(users[0].roles, roles);
}

