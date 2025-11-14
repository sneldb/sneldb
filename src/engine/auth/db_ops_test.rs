use super::db_ops::{load_from_db, store_user_in_db};
use super::types::{PermissionSet, User};
use crate::engine::shard::manager::ShardManager;
use crate::logging::init_for_tests;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;

/// Helper function to create a test ShardManager
async fn create_test_shard_manager() -> Arc<ShardManager> {
    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();
    Arc::new(ShardManager::new(1, base_dir, wal_dir).await)
}

#[tokio::test]
async fn test_store_user_in_db_success() {
    init_for_tests();

    let shard_manager = create_test_shard_manager().await;

    let user = User {
        user_id: "test_user".to_string(),
        secret_key: "my_secret_key".to_string(),
        active: true,
        created_at: 1000,
        roles: vec!["admin".to_string()],
        permissions: HashMap::new(),
    };

    let result = store_user_in_db(&shard_manager, &user).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_store_user_in_db_with_permissions() {
    init_for_tests();

    let shard_manager = create_test_shard_manager().await;

    let mut permissions = HashMap::new();
    permissions.insert("event_type1".to_string(), PermissionSet::read_write());
    permissions.insert("event_type2".to_string(), PermissionSet::read_only());

    let user = User {
        user_id: "test_user".to_string(),
        secret_key: "my_secret_key".to_string(),
        active: true,
        created_at: 1000,
        roles: vec!["admin".to_string(), "read-only".to_string()],
        permissions,
    };

    let result = store_user_in_db(&shard_manager, &user).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_store_user_in_db_inactive_user() {
    init_for_tests();

    let shard_manager = create_test_shard_manager().await;

    let user = User {
        user_id: "inactive_user".to_string(),
        secret_key: "my_secret_key".to_string(),
        active: false,
        created_at: 1000,
        roles: Vec::new(),
        permissions: HashMap::new(),
    };

    let result = store_user_in_db(&shard_manager, &user).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_store_user_in_db_empty_roles() {
    init_for_tests();

    let shard_manager = create_test_shard_manager().await;

    let user = User {
        user_id: "test_user".to_string(),
        secret_key: "my_secret_key".to_string(),
        active: true,
        created_at: 1000,
        roles: Vec::new(),
        permissions: HashMap::new(),
    };

    let result = store_user_in_db(&shard_manager, &user).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_store_user_in_db_empty_permissions() {
    init_for_tests();

    let shard_manager = create_test_shard_manager().await;

    let user = User {
        user_id: "test_user".to_string(),
        secret_key: "my_secret_key".to_string(),
        active: true,
        created_at: 1000,
        roles: vec!["admin".to_string()],
        permissions: HashMap::new(),
    };

    let result = store_user_in_db(&shard_manager, &user).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_store_user_in_db_multiple_users() {
    init_for_tests();

    let shard_manager = create_test_shard_manager().await;

    // Store multiple users
    for i in 1..=5 {
        let user = User {
            user_id: format!("user{}", i),
            secret_key: format!("secret{}", i),
            active: i % 2 == 0, // Alternate active/inactive
            created_at: 1000 + i as u64,
            roles: if i == 1 {
                vec!["admin".to_string()]
            } else {
                Vec::new()
            },
            permissions: HashMap::new(),
        };

        let result = store_user_in_db(&shard_manager, &user).await;
        assert!(result.is_ok(), "Failed to store user{}", i);
    }
}

#[tokio::test]
async fn test_store_user_in_db_with_special_characters() {
    init_for_tests();

    let shard_manager = create_test_shard_manager().await;

    let user = User {
        user_id: "user_123-test".to_string(),
        secret_key: "secret_key_with_special_chars!@#$%".to_string(),
        active: true,
        created_at: 1000,
        roles: vec!["role-with-dash".to_string(), "role_with_underscore".to_string()],
        permissions: HashMap::new(),
    };

    let result = store_user_in_db(&shard_manager, &user).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_store_user_in_db_large_roles_list() {
    init_for_tests();

    let shard_manager = create_test_shard_manager().await;

    let roles: Vec<String> = (1..=100).map(|i| format!("role{}", i)).collect();

    let user = User {
        user_id: "test_user".to_string(),
        secret_key: "my_secret_key".to_string(),
        active: true,
        created_at: 1000,
        roles,
        permissions: HashMap::new(),
    };

    let result = store_user_in_db(&shard_manager, &user).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_store_user_in_db_large_permissions_map() {
    init_for_tests();

    let shard_manager = create_test_shard_manager().await;

    let mut permissions = HashMap::new();
    for i in 1..=100 {
        permissions.insert(
            format!("event_type{}", i),
            PermissionSet::read_write(),
        );
    }

    let user = User {
        user_id: "test_user".to_string(),
        secret_key: "my_secret_key".to_string(),
        active: true,
        created_at: 1000,
        roles: Vec::new(),
        permissions,
    };

    let result = store_user_in_db(&shard_manager, &user).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_store_user_in_db_zero_created_at() {
    init_for_tests();

    let shard_manager = create_test_shard_manager().await;

    let user = User {
        user_id: "test_user".to_string(),
        secret_key: "my_secret_key".to_string(),
        active: true,
        created_at: 0,
        roles: Vec::new(),
        permissions: HashMap::new(),
    };

    let result = store_user_in_db(&shard_manager, &user).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_store_user_in_db_large_created_at() {
    init_for_tests();

    let shard_manager = create_test_shard_manager().await;

    let user = User {
        user_id: "test_user".to_string(),
        secret_key: "my_secret_key".to_string(),
        active: true,
        created_at: u64::MAX,
        roles: Vec::new(),
        permissions: HashMap::new(),
    };

    let result = store_user_in_db(&shard_manager, &user).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_load_from_db_success() {
    init_for_tests();

    let shard_manager = create_test_shard_manager().await;

    // load_from_db is currently a placeholder that always succeeds
    let result = load_from_db(&shard_manager).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_load_from_db_multiple_calls() {
    init_for_tests();

    let shard_manager = create_test_shard_manager().await;

    // Call multiple times (should all succeed)
    for _ in 0..5 {
        let result = load_from_db(&shard_manager).await;
        assert!(result.is_ok());
    }
}

#[tokio::test]
async fn test_store_user_in_db_updates_existing_user() {
    init_for_tests();

    let shard_manager = create_test_shard_manager().await;

    // Store user first time
    let user1 = User {
        user_id: "test_user".to_string(),
        secret_key: "secret1".to_string(),
        active: true,
        created_at: 1000,
        roles: vec!["role1".to_string()],
        permissions: HashMap::new(),
    };

    let result = store_user_in_db(&shard_manager, &user1).await;
    assert!(result.is_ok());

    // Store same user with different data (should overwrite)
    let user2 = User {
        user_id: "test_user".to_string(),
        secret_key: "secret2".to_string(),
        active: false,
        created_at: 2000,
        roles: vec!["role2".to_string()],
        permissions: HashMap::new(),
    };

    let result = store_user_in_db(&shard_manager, &user2).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_store_user_in_db_all_permission_types() {
    init_for_tests();

    let shard_manager = create_test_shard_manager().await;

    let mut permissions = HashMap::new();
    permissions.insert("read_write".to_string(), PermissionSet::read_write());
    permissions.insert("read_only".to_string(), PermissionSet::read_only());
    permissions.insert("write_only".to_string(), PermissionSet::write_only());
    permissions.insert("none".to_string(), PermissionSet::none());

    let user = User {
        user_id: "test_user".to_string(),
        secret_key: "my_secret_key".to_string(),
        active: true,
        created_at: 1000,
        roles: Vec::new(),
        permissions,
    };

    let result = store_user_in_db(&shard_manager, &user).await;
    assert!(result.is_ok());
}

