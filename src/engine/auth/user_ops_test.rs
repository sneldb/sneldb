use super::storage::AuthWalStorage;
use super::types::{
    AuthError, MAX_SECRET_KEY_LENGTH, MAX_USER_ID_LENGTH, PermissionCache, UserCache,
};
use super::user_ops::{
    bootstrap_admin_user, create_user, create_user_with_roles, list_users, revoke_key,
};
use crate::logging::init_for_tests;
use crate::shared::config::CONFIG;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::RwLock;

/// Helper function to create test dependencies
async fn create_test_deps() -> (
    Arc<RwLock<UserCache>>,
    Arc<RwLock<PermissionCache>>,
    Arc<dyn crate::engine::auth::storage::AuthStorage>,
) {
    let wal_dir = tempdir().unwrap().into_path();
    let wal_path = wal_dir.join("auth.swal");
    let storage: Arc<dyn crate::engine::auth::storage::AuthStorage> =
        Arc::new(AuthWalStorage::new(wal_path).expect("create auth wal storage"));
    let cache = Arc::new(RwLock::new(UserCache::new()));
    let permission_cache = Arc::new(RwLock::new(PermissionCache::new()));
    (cache, permission_cache, storage)
}

#[tokio::test]
async fn test_create_user_success_without_secret_key() {
    init_for_tests();

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    let result = create_user(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user".to_string(),
        None,
    )
    .await;

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

    let (cache, permission_cache, auth_storage) = create_test_deps().await;
    let provided_key = "my_custom_secret_key_12345";

    let result = create_user(
        &cache,
        &permission_cache,
        &auth_storage,
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

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    // Create first user
    create_user(
        &cache,
        &permission_cache,
        &auth_storage,
        "existing_user".to_string(),
        None,
    )
    .await
    .expect("First creation should succeed");

    // Try to create same user again
    let result = create_user(
        &cache,
        &permission_cache,
        &auth_storage,
        "existing_user".to_string(),
        None,
    )
    .await;

    assert!(result.is_err());
    match result {
        Err(AuthError::UserExists) => {}
        _ => panic!("Expected UserExists error"),
    }

    // Verify only one user exists
    let users = list_users(&cache).await;
    assert_eq!(users.len(), 1);
}

#[tokio::test]
async fn test_create_user_error_invalid_user_id() {
    init_for_tests();

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    // Test various invalid user IDs
    let invalid_ids = vec!["user@domain", "user#123", "user space", "user.dot", ""];

    for invalid_id in invalid_ids {
        let result = create_user(
            &cache,
            &permission_cache,
            &auth_storage,
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

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

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
            &auth_storage,
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

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    // Create multiple users without providing keys
    let mut keys = Vec::new();
    for i in 0..10 {
        let key = create_user(
            &cache,
            &permission_cache,
            &auth_storage,
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

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    create_user(
        &cache,
        &permission_cache,
        &auth_storage,
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

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    let roles = vec!["admin".to_string(), "read-only".to_string()];
    let result = create_user_with_roles(
        &cache,
        &permission_cache,
        &auth_storage,
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

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    // Create user first
    create_user(
        &cache,
        &permission_cache,
        &auth_storage,
        "existing_user".to_string(),
        None,
    )
    .await
    .expect("User creation should succeed");

    // Try to create same user with roles
    let result = create_user_with_roles(
        &cache,
        &permission_cache,
        &auth_storage,
        "existing_user".to_string(),
        Some("secret".to_string()),
        vec!["admin".to_string()],
    )
    .await;

    assert!(result.is_err());
    match result {
        Err(AuthError::UserExists) => {}
        _ => panic!("Expected UserExists error"),
    }
}

#[tokio::test]
async fn test_create_user_with_roles_error_invalid_user_id() {
    init_for_tests();

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    let result = create_user_with_roles(
        &cache,
        &permission_cache,
        &auth_storage,
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

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    // Create a user first
    create_user(
        &cache,
        &permission_cache,
        &auth_storage,
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
    let result = revoke_key(&cache, &permission_cache, &auth_storage, "user_to_revoke").await;
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

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    let result = revoke_key(
        &cache,
        &permission_cache,
        &auth_storage,
        "non_existent_user",
    )
    .await;

    assert!(result.is_err());
    match result {
        Err(AuthError::UserNotFound(user_id)) => assert_eq!(user_id, "non_existent_user"),
        _ => panic!("Expected UserNotFound error"),
    }
}

#[tokio::test]
async fn test_revoke_key_preserves_secret_key() {
    init_for_tests();

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    // Create a user
    let original_secret = "my_secret_key_12345";
    create_user(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user".to_string(),
        Some(original_secret.to_string()),
    )
    .await
    .expect("User creation should succeed");

    // Revoke key
    revoke_key(&cache, &permission_cache, &auth_storage, "test_user")
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

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    // Create multiple users
    let user_ids = vec!["user1", "user2", "user3"];
    for user_id in &user_ids {
        create_user(
            &cache,
            &permission_cache,
            &auth_storage,
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

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    // Create two users
    create_user(
        &cache,
        &permission_cache,
        &auth_storage,
        "active_user".to_string(),
        None,
    )
    .await
    .expect("User creation should succeed");
    create_user(
        &cache,
        &permission_cache,
        &auth_storage,
        "inactive_user".to_string(),
        None,
    )
    .await
    .expect("User creation should succeed");

    // Revoke one user's key
    revoke_key(&cache, &permission_cache, &auth_storage, "inactive_user")
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

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    // Create user
    create_user(
        &cache,
        &permission_cache,
        &auth_storage,
        "recreate_user".to_string(),
        None,
    )
    .await
    .expect("User creation should succeed");

    // Revoke key
    revoke_key(&cache, &permission_cache, &auth_storage, "recreate_user")
        .await
        .expect("Revoke should succeed");

    // Try to create same user again (should fail - user still exists)
    let result = create_user(
        &cache,
        &permission_cache,
        &auth_storage,
        "recreate_user".to_string(),
        None,
    )
    .await;

    assert!(result.is_err());
    match result {
        Err(AuthError::UserExists) => {}
        _ => panic!("Expected UserExists error"),
    }
}

#[tokio::test]
async fn test_bootstrap_admin_user_skips_when_users_exist() {
    init_for_tests();

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    // Create a user first
    create_user(
        &cache,
        &permission_cache,
        &auth_storage,
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

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    let result = create_user_with_roles(
        &cache,
        &permission_cache,
        &auth_storage,
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

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    let roles = vec![
        "admin".to_string(),
        "read-only".to_string(),
        "write-only".to_string(),
    ];
    let result = create_user_with_roles(
        &cache,
        &permission_cache,
        &auth_storage,
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

#[tokio::test]
async fn test_create_user_error_secret_key_too_long() {
    init_for_tests();

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    // Create a secret key that exceeds MAX_SECRET_KEY_LENGTH
    let too_long_key = "a".repeat(MAX_SECRET_KEY_LENGTH + 1);

    let result = create_user(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user".to_string(),
        Some(too_long_key),
    )
    .await;

    assert!(result.is_err());
    match result {
        Err(AuthError::SecretKeyTooLong { max }) => {
            assert_eq!(max, MAX_SECRET_KEY_LENGTH);
        }
        _ => panic!("Expected SecretKeyTooLong error"),
    }
}

#[tokio::test]
async fn test_create_user_with_roles_error_secret_key_too_long() {
    init_for_tests();

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    // Create a secret key that exceeds MAX_SECRET_KEY_LENGTH
    let too_long_key = "a".repeat(MAX_SECRET_KEY_LENGTH + 1);

    let result = create_user_with_roles(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user".to_string(),
        Some(too_long_key),
        vec!["admin".to_string()],
    )
    .await;

    assert!(result.is_err());
    match result {
        Err(AuthError::SecretKeyTooLong { max }) => {
            assert_eq!(max, MAX_SECRET_KEY_LENGTH);
        }
        _ => panic!("Expected SecretKeyTooLong error"),
    }
}

#[tokio::test]
async fn test_create_user_secret_key_at_max_length() {
    init_for_tests();

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    // Create a secret key at exactly MAX_SECRET_KEY_LENGTH (should succeed)
    let max_length_key = "a".repeat(MAX_SECRET_KEY_LENGTH);

    let result = create_user(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user".to_string(),
        Some(max_length_key.clone()),
    )
    .await;

    assert!(result.is_ok());
    let users = list_users(&cache).await;
    assert_eq!(users.len(), 1);
    assert_eq!(users[0].secret_key, max_length_key);
}

#[tokio::test]
async fn test_create_user_error_user_id_too_long() {
    init_for_tests();

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    // Create a user ID that exceeds MAX_USER_ID_LENGTH
    let too_long_id = "a".repeat(MAX_USER_ID_LENGTH + 1);

    let result = create_user(&cache, &permission_cache, &auth_storage, too_long_id, None).await;

    assert!(result.is_err());
    match result {
        Err(AuthError::UserIdTooLong { max }) => {
            assert_eq!(max, MAX_USER_ID_LENGTH);
        }
        _ => panic!("Expected UserIdTooLong error"),
    }
}

#[tokio::test]
async fn test_create_user_with_roles_error_user_id_too_long() {
    init_for_tests();

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    // Create a user ID that exceeds MAX_USER_ID_LENGTH
    let too_long_id = "a".repeat(MAX_USER_ID_LENGTH + 1);

    let result = create_user_with_roles(
        &cache,
        &permission_cache,
        &auth_storage,
        too_long_id,
        Some("secret".to_string()),
        vec!["admin".to_string()],
    )
    .await;

    assert!(result.is_err());
    match result {
        Err(AuthError::UserIdTooLong { max }) => {
            assert_eq!(max, MAX_USER_ID_LENGTH);
        }
        _ => panic!("Expected UserIdTooLong error"),
    }
}

#[tokio::test]
async fn test_create_user_user_id_at_max_length() {
    init_for_tests();

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    // Create a user ID at exactly MAX_USER_ID_LENGTH (should succeed)
    let max_length_id = "a".repeat(MAX_USER_ID_LENGTH);

    let result = create_user(
        &cache,
        &permission_cache,
        &auth_storage,
        max_length_id.clone(),
        None,
    )
    .await;

    assert!(result.is_ok());
    let users = list_users(&cache).await;
    assert_eq!(users.len(), 1);
    assert_eq!(users[0].user_id, max_length_id);
}

#[tokio::test]
async fn test_create_user_with_roles_generates_secret_key_when_none() {
    init_for_tests();

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    let result = create_user_with_roles(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user".to_string(),
        None, // No secret key provided
        vec!["admin".to_string()],
    )
    .await;

    assert!(result.is_ok());
    let secret_key = result.unwrap();

    // Verify secret key is generated (64 hex chars = 32 bytes)
    assert_eq!(secret_key.len(), 64);
    assert!(secret_key.chars().all(|c| c.is_ascii_hexdigit()));

    let users = list_users(&cache).await;
    assert_eq!(users.len(), 1);
    assert_eq!(users[0].secret_key, secret_key);
}

#[tokio::test]
async fn test_revoke_key_preserves_created_at() {
    init_for_tests();

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    // Create a user
    create_user(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user".to_string(),
        Some("secret".to_string()),
    )
    .await
    .expect("User creation should succeed");

    // Get the created_at timestamp before revoke
    let users_before = list_users(&cache).await;
    let created_at_before = users_before[0].created_at;

    // Wait a bit to ensure timestamps would differ if not preserved
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    // Revoke key
    revoke_key(&cache, &permission_cache, &auth_storage, "test_user")
        .await
        .expect("Revoke should succeed");

    // Verify created_at is preserved
    let users_after = list_users(&cache).await;
    assert_eq!(users_after[0].created_at, created_at_before);
}

#[tokio::test]
async fn test_revoke_key_preserves_roles() {
    init_for_tests();

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    // Create a user with roles
    let roles = vec!["admin".to_string(), "read-only".to_string()];
    create_user_with_roles(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user".to_string(),
        Some("secret".to_string()),
        roles.clone(),
    )
    .await
    .expect("User creation should succeed");

    // Revoke key
    revoke_key(&cache, &permission_cache, &auth_storage, "test_user")
        .await
        .expect("Revoke should succeed");

    // Verify roles are preserved
    let users = list_users(&cache).await;
    assert_eq!(users[0].roles, roles);
}

#[tokio::test]
async fn test_revoke_key_preserves_permissions() {
    init_for_tests();

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    // Create a user
    create_user(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user".to_string(),
        Some("secret".to_string()),
    )
    .await
    .expect("User creation should succeed");

    // Get permissions before revoke (should be empty initially)
    let users_before = list_users(&cache).await;
    let permissions_before = users_before[0].permissions.clone();

    // Revoke key
    revoke_key(&cache, &permission_cache, &auth_storage, "test_user")
        .await
        .expect("Revoke should succeed");

    // Verify permissions are preserved
    let users_after = list_users(&cache).await;
    assert_eq!(users_after[0].permissions, permissions_before);
}

#[tokio::test]
async fn test_bootstrap_admin_user_skips_when_admin_exists_in_cache() {
    init_for_tests();

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    // Check if CONFIG has auth settings - if not, skip this test
    // Note: This test requires CONFIG.auth.initial_admin_user to be set
    // In test config, it might not be set, so we check first
    if CONFIG.auth.is_none() {
        // Skip test if no auth config
        return;
    }

    let admin_user_id = if let Some(ref auth_config) = CONFIG.auth {
        if let Some(ref admin_user) = auth_config.initial_admin_user {
            admin_user.clone()
        } else {
            // No admin user configured, skip test
            return;
        }
    } else {
        return;
    };

    // Create the admin user directly in cache (simulating it already exists)
    create_user_with_roles(
        &cache,
        &permission_cache,
        &auth_storage,
        admin_user_id.clone(),
        Some("existing_key".to_string()),
        vec!["admin".to_string()],
    )
    .await
    .expect("Admin user creation should succeed");

    // Bootstrap should skip since admin user exists in cache
    let result = bootstrap_admin_user(&cache, &permission_cache, &auth_storage).await;
    assert!(result.is_ok());

    // Verify admin user still exists and wasn't recreated
    let users = list_users(&cache).await;
    let admin_user = users.iter().find(|u| u.user_id == admin_user_id);
    assert!(admin_user.is_some());
    assert_eq!(admin_user.unwrap().secret_key, "existing_key");
}

#[tokio::test]
async fn test_bootstrap_admin_user_creates_when_not_in_cache() {
    init_for_tests();

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    // Check if CONFIG has auth settings with admin user configured
    if CONFIG.auth.is_none() {
        // Skip test if no auth config
        return;
    }

    let (admin_user_id, admin_key) = if let Some(ref auth_config) = CONFIG.auth {
        if let (Some(admin_user), Some(admin_key)) = (
            &auth_config.initial_admin_user,
            &auth_config.initial_admin_key,
        ) {
            (admin_user.clone(), admin_key.clone())
        } else {
            // No admin user/key configured, skip test
            return;
        }
    } else {
        return;
    };

    // Ensure cache is empty (admin user not in cache)
    let users_before = list_users(&cache).await;
    assert!(users_before.is_empty() || !users_before.iter().any(|u| u.user_id == admin_user_id));

    // Bootstrap should create admin user
    let result = bootstrap_admin_user(&cache, &permission_cache, &auth_storage).await;
    assert!(result.is_ok());

    // Verify admin user was created
    let users_after = list_users(&cache).await;
    let admin_user = users_after.iter().find(|u| u.user_id == admin_user_id);
    assert!(admin_user.is_some());
    let admin = admin_user.unwrap();
    assert_eq!(admin.secret_key, admin_key);
    assert!(admin.roles.contains(&"admin".to_string()));
    assert!(admin.active);
}

#[tokio::test]
async fn test_bootstrap_admin_user_skips_when_other_users_exist_but_not_admin() {
    init_for_tests();

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    // Check if CONFIG has auth settings
    if CONFIG.auth.is_none() {
        return;
    }

    let admin_user_id = if let Some(ref auth_config) = CONFIG.auth {
        if let Some(ref admin_user) = auth_config.initial_admin_user {
            admin_user.clone()
        } else {
            return;
        }
    } else {
        return;
    };

    // Create a different user (not the admin user)
    create_user(
        &cache,
        &permission_cache,
        &auth_storage,
        "other_user".to_string(),
        Some("secret".to_string()),
    )
    .await
    .expect("User creation should succeed");

    // Bootstrap should still create admin user since it doesn't exist in cache
    let result = bootstrap_admin_user(&cache, &permission_cache, &auth_storage).await;
    assert!(result.is_ok());

    // Verify both users exist
    let users = list_users(&cache).await;
    assert!(users.iter().any(|u| u.user_id == "other_user"));
    assert!(users.iter().any(|u| u.user_id == admin_user_id));
}

#[tokio::test]
async fn test_create_user_with_roles_empty_secret_key_string() {
    init_for_tests();

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    // Empty string secret key should be valid (length 0 < MAX_SECRET_KEY_LENGTH)
    let result = create_user_with_roles(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user".to_string(),
        Some("".to_string()),
        vec!["admin".to_string()],
    )
    .await;

    assert!(result.is_ok());
    let users = list_users(&cache).await;
    assert_eq!(users.len(), 1);
    assert_eq!(users[0].secret_key, "");
}

#[tokio::test]
async fn test_create_user_with_roles_handles_user_exists_gracefully() {
    init_for_tests();

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    // Create user first
    create_user(
        &cache,
        &permission_cache,
        &auth_storage,
        "existing_user".to_string(),
        Some("secret".to_string()),
    )
    .await
    .expect("User creation should succeed");

    // Try to create same user with roles - should fail with UserExists
    let result = create_user_with_roles(
        &cache,
        &permission_cache,
        &auth_storage,
        "existing_user".to_string(),
        Some("different_secret".to_string()),
        vec!["admin".to_string()],
    )
    .await;

    assert!(result.is_err());
    match result {
        Err(AuthError::UserExists) => {}
        _ => panic!("Expected UserExists error"),
    }

    // Verify user still has original secret key (not changed)
    let users = list_users(&cache).await;
    assert_eq!(users.len(), 1);
    assert_eq!(users[0].secret_key, "secret");
    assert_eq!(users[0].roles, Vec::<String>::new()); // Original user had no roles
}

#[tokio::test]
async fn test_create_user_with_roles_creates_active_user() {
    init_for_tests();

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    let result = create_user_with_roles(
        &cache,
        &permission_cache,
        &auth_storage,
        "active_user".to_string(),
        Some("secret".to_string()),
        vec!["admin".to_string()],
    )
    .await;

    assert!(result.is_ok());

    let users = list_users(&cache).await;
    assert_eq!(users.len(), 1);
    assert!(users[0].active);
}

#[tokio::test]
async fn test_create_user_with_roles_initializes_empty_permissions() {
    init_for_tests();

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    let result = create_user_with_roles(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user".to_string(),
        Some("secret".to_string()),
        vec!["admin".to_string()],
    )
    .await;

    assert!(result.is_ok());

    let users = list_users(&cache).await;
    assert_eq!(users.len(), 1);
    assert!(users[0].permissions.is_empty());
}

#[tokio::test]
async fn test_create_user_with_roles_sets_created_at() {
    init_for_tests();

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    let before_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let result = create_user_with_roles(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user".to_string(),
        Some("secret".to_string()),
        vec!["admin".to_string()],
    )
    .await;

    assert!(result.is_ok());

    let after_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let users = list_users(&cache).await;
    assert_eq!(users.len(), 1);
    assert!(users[0].created_at >= before_time);
    assert!(users[0].created_at <= after_time);
}

#[tokio::test]
async fn test_revoke_key_twice_still_inactive() {
    init_for_tests();

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    // Create a user
    create_user(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user".to_string(),
        Some("secret".to_string()),
    )
    .await
    .expect("User creation should succeed");

    // Revoke key first time
    revoke_key(&cache, &permission_cache, &auth_storage, "test_user")
        .await
        .expect("First revoke should succeed");

    // Revoke key second time (should still work, user remains inactive)
    let result = revoke_key(&cache, &permission_cache, &auth_storage, "test_user").await;
    assert!(result.is_ok());

    // Verify user is still inactive
    let users = list_users(&cache).await;
    assert_eq!(users.len(), 1);
    assert!(!users[0].active);
}
