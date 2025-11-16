use super::db_ops::{load_from_db, store_user_in_db};
use super::storage::AuthWalStorage;
use super::types::{PermissionCache, PermissionSet, User, UserCache, UserKey};
use crate::logging::init_for_tests;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::RwLock;
use tokio::time::Duration;

/// Helper function to create a test auth storage
fn create_test_storage() -> (
    Arc<dyn crate::engine::auth::storage::AuthStorage>,
    std::path::PathBuf,
) {
    let wal_dir = tempdir().unwrap().into_path();
    let path = wal_dir.join("auth.swal");
    let storage: Arc<dyn crate::engine::auth::storage::AuthStorage> =
        Arc::new(AuthWalStorage::new(path.clone()).expect("create auth wal storage"));
    (storage, path)
}

async fn settle_shard() {
    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(5)).await;
}

#[tokio::test]
async fn test_store_user_in_db_success() {
    init_for_tests();

    let (auth_storage, _path) = create_test_storage();

    let user = User {
        user_id: "test_user".to_string(),
        secret_key: "my_secret_key".to_string(),
        active: true,
        created_at: 1000,
        roles: vec!["admin".to_string()],
        permissions: HashMap::new(),
    };

    let result = store_user_in_db(&auth_storage, &user).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_store_user_in_db_with_permissions() {
    init_for_tests();

    let (auth_storage, _path) = create_test_storage();

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

    let result = store_user_in_db(&auth_storage, &user).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_store_user_in_db_inactive_user() {
    init_for_tests();

    let (auth_storage, _path) = create_test_storage();

    let user = User {
        user_id: "inactive_user".to_string(),
        secret_key: "my_secret_key".to_string(),
        active: false,
        created_at: 1000,
        roles: Vec::new(),
        permissions: HashMap::new(),
    };

    let result = store_user_in_db(&auth_storage, &user).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_store_user_in_db_empty_roles() {
    init_for_tests();

    let (auth_storage, _path) = create_test_storage();

    let user = User {
        user_id: "test_user".to_string(),
        secret_key: "my_secret_key".to_string(),
        active: true,
        created_at: 1000,
        roles: Vec::new(),
        permissions: HashMap::new(),
    };

    let result = store_user_in_db(&auth_storage, &user).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_store_user_in_db_empty_permissions() {
    init_for_tests();

    let (auth_storage, _path) = create_test_storage();

    let user = User {
        user_id: "test_user".to_string(),
        secret_key: "my_secret_key".to_string(),
        active: true,
        created_at: 1000,
        roles: vec!["admin".to_string()],
        permissions: HashMap::new(),
    };

    let result = store_user_in_db(&auth_storage, &user).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_store_user_in_db_multiple_users() {
    init_for_tests();

    let (auth_storage, _path) = create_test_storage();

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

        let result = store_user_in_db(&auth_storage, &user).await;
        assert!(result.is_ok(), "Failed to store user{}", i);
    }
}

#[tokio::test]
async fn test_store_user_in_db_with_special_characters() {
    init_for_tests();

    let (auth_storage, _path) = create_test_storage();

    let user = User {
        user_id: "user_123-test".to_string(),
        secret_key: "secret_key_with_special_chars!@#$%".to_string(),
        active: true,
        created_at: 1000,
        roles: vec![
            "role-with-dash".to_string(),
            "role_with_underscore".to_string(),
        ],
        permissions: HashMap::new(),
    };

    let result = store_user_in_db(&auth_storage, &user).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_store_user_in_db_large_roles_list() {
    init_for_tests();

    let (auth_storage, _path) = create_test_storage();

    let roles: Vec<String> = (1..=100).map(|i| format!("role{}", i)).collect();

    let user = User {
        user_id: "test_user".to_string(),
        secret_key: "my_secret_key".to_string(),
        active: true,
        created_at: 1000,
        roles,
        permissions: HashMap::new(),
    };

    let result = store_user_in_db(&auth_storage, &user).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_store_user_in_db_large_permissions_map() {
    init_for_tests();

    let (auth_storage, _path) = create_test_storage();

    let mut permissions = HashMap::new();
    for i in 1..=100 {
        permissions.insert(format!("event_type{}", i), PermissionSet::read_write());
    }

    let user = User {
        user_id: "test_user".to_string(),
        secret_key: "my_secret_key".to_string(),
        active: true,
        created_at: 1000,
        roles: Vec::new(),
        permissions,
    };

    let result = store_user_in_db(&auth_storage, &user).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_store_user_in_db_zero_created_at() {
    init_for_tests();

    let (auth_storage, _path) = create_test_storage();

    let user = User {
        user_id: "test_user".to_string(),
        secret_key: "my_secret_key".to_string(),
        active: true,
        created_at: 0,
        roles: Vec::new(),
        permissions: HashMap::new(),
    };

    let result = store_user_in_db(&auth_storage, &user).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_store_user_in_db_large_created_at() {
    init_for_tests();

    let (auth_storage, _path) = create_test_storage();

    let user = User {
        user_id: "test_user".to_string(),
        secret_key: "my_secret_key".to_string(),
        active: true,
        created_at: u64::MAX,
        roles: Vec::new(),
        permissions: HashMap::new(),
    };

    let result = store_user_in_db(&auth_storage, &user).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_load_from_db_success() {
    init_for_tests();

    let (auth_storage, _path) = create_test_storage();
    let cache = Arc::new(RwLock::new(UserCache::new()));
    let permission_cache = Arc::new(RwLock::new(PermissionCache::new()));

    // No users stored yet; should still succeed and leave caches empty
    let result = load_from_db(&cache, &permission_cache, &auth_storage).await;
    assert!(result.is_ok());

    let cache_guard = cache.read().await;
    assert!(cache_guard.all_users().is_empty());
}

#[tokio::test]
async fn test_load_from_db_multiple_calls() {
    init_for_tests();

    let (auth_storage, _path) = create_test_storage();
    let cache = Arc::new(RwLock::new(UserCache::new()));
    let permission_cache = Arc::new(RwLock::new(PermissionCache::new()));

    // Call multiple times (should all succeed)
    for _ in 0..5 {
        let result = load_from_db(&cache, &permission_cache, &auth_storage).await;
        assert!(result.is_ok());
    }
}

#[tokio::test]
async fn test_store_user_in_db_updates_existing_user() {
    init_for_tests();

    let (auth_storage, _path) = create_test_storage();

    // Store user first time
    let user1 = User {
        user_id: "test_user".to_string(),
        secret_key: "secret1".to_string(),
        active: true,
        created_at: 1000,
        roles: vec!["role1".to_string()],
        permissions: HashMap::new(),
    };

    let result = store_user_in_db(&auth_storage, &user1).await;
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

    let result = store_user_in_db(&auth_storage, &user2).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_store_user_in_db_all_permission_types() {
    init_for_tests();

    let (auth_storage, _path) = create_test_storage();

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

    let result = store_user_in_db(&auth_storage, &user).await;
    assert!(result.is_ok());
}

fn append_corrupted_frame(path: &std::path::Path) {
    use std::fs::OpenOptions;
    use std::io::Write;

    let mut file = OpenOptions::new()
        .write(true)
        .append(true)
        .open(path)
        .expect("open auth wal for corruption");

    let len = 4u32.to_le_bytes();
    let crc = 0xDEADBEEFu32.to_le_bytes();
    let payload = [1u8, 2, 3, 4];

    file.write_all(&len).expect("write corrupted len");
    file.write_all(&crc).expect("write corrupted crc");
    file.write_all(&payload).expect("write corrupted payload");
}

#[tokio::test]
async fn test_load_from_db_populates_caches_with_permissions() {
    init_for_tests();

    let (auth_storage, _path) = create_test_storage();
    let cache = Arc::new(RwLock::new(UserCache::new()));
    let permission_cache = Arc::new(RwLock::new(PermissionCache::new()));

    let mut permissions = HashMap::new();
    permissions.insert("orders".to_string(), PermissionSet::read_write());
    let user = User {
        user_id: "cache_user".to_string(),
        secret_key: "secret".to_string(),
        active: true,
        created_at: 10,
        roles: vec!["admin".to_string()],
        permissions,
    };
    store_user_in_db(&auth_storage, &user)
        .await
        .expect("store user");

    settle_shard().await;
    load_from_db(&cache, &permission_cache, &auth_storage)
        .await
        .expect("load users");

    let cache_guard = cache.read().await;
    let users: Vec<_> = cache_guard.all_users().into_iter().cloned().collect();
    assert_eq!(users.len(), 1);
    let loaded = &users[0];
    assert_eq!(loaded.user_id, "cache_user");
    assert!(loaded.active);
    assert!(loaded.permissions.get("orders").unwrap().read);

    let perm_guard = permission_cache.read().await;
    assert!(perm_guard.is_admin("cache_user"));
    assert!(perm_guard.can_write("cache_user", "orders"));
}

#[tokio::test]
async fn test_load_from_db_latest_event_wins() {
    init_for_tests();

    let (auth_storage, _path) = create_test_storage();
    let cache = Arc::new(RwLock::new(UserCache::new()));
    let permission_cache = Arc::new(RwLock::new(PermissionCache::new()));

    let first = User {
        user_id: "flappy".to_string(),
        secret_key: "secret1".to_string(),
        active: true,
        created_at: 1,
        roles: vec![],
        permissions: HashMap::new(),
    };
    store_user_in_db(&auth_storage, &first)
        .await
        .expect("store first user");

    tokio::time::sleep(Duration::from_millis(10)).await;

    let mut updated_perms = HashMap::new();
    updated_perms.insert("queue".to_string(), PermissionSet::write_only());
    let second = User {
        user_id: "flappy".to_string(),
        secret_key: "secret2".to_string(),
        active: false,
        created_at: 2,
        roles: vec!["auditor".to_string()],
        permissions: updated_perms.clone(),
    };
    store_user_in_db(&auth_storage, &second)
        .await
        .expect("store second user");

    settle_shard().await;
    load_from_db(&cache, &permission_cache, &auth_storage)
        .await
        .expect("load users");

    let cache_guard = cache.read().await;
    let users: Vec<_> = cache_guard.all_users().into_iter().cloned().collect();
    assert_eq!(users.len(), 1);
    let loaded = &users[0];
    assert_eq!(loaded.secret_key, "secret2");
    assert!(!loaded.active);
    assert_eq!(loaded.permissions, updated_perms);
}

#[tokio::test]
async fn test_load_from_db_skips_corrupted_events() {
    init_for_tests();

    let (auth_storage, path) = create_test_storage();
    let cache = Arc::new(RwLock::new(UserCache::new()));
    let permission_cache = Arc::new(RwLock::new(PermissionCache::new()));

    append_corrupted_frame(&path);

    let user = User {
        user_id: "valid".to_string(),
        secret_key: "secret".to_string(),
        active: true,
        created_at: 3,
        roles: vec![],
        permissions: HashMap::new(),
    };
    store_user_in_db(&auth_storage, &user)
        .await
        .expect("store valid user");

    settle_shard().await;
    load_from_db(&cache, &permission_cache, &auth_storage)
        .await
        .expect("load users");

    let cache_guard = cache.read().await;
    let users: Vec<_> = cache_guard.all_users().into_iter().cloned().collect();
    assert_eq!(users.len(), 1);
    assert_eq!(users[0].user_id, "valid");
}

#[tokio::test]
async fn test_load_from_db_resets_old_cache_entries() {
    init_for_tests();

    let (auth_storage, _path) = create_test_storage();
    let cache = Arc::new(RwLock::new(UserCache::new()));
    let permission_cache = Arc::new(RwLock::new(PermissionCache::new()));

    {
        let mut cache_guard = cache.write().await;
        cache_guard.insert(
            UserKey::from(User {
                user_id: "stale".to_string(),
                secret_key: "old".to_string(),
                active: true,
                created_at: 0,
                roles: vec![],
                permissions: HashMap::new(),
            })
            .into(),
        );
    }
    {
        let mut perm_guard = permission_cache.write().await;
        perm_guard.update_user(
            cache
                .read()
                .await
                .get("stale")
                .expect("stale present in cache"),
        );
    }

    let fresh = User {
        user_id: "fresh".to_string(),
        secret_key: "new".to_string(),
        active: true,
        created_at: 4,
        roles: vec![],
        permissions: HashMap::new(),
    };
    store_user_in_db(&auth_storage, &fresh)
        .await
        .expect("store fresh user");

    settle_shard().await;
    load_from_db(&cache, &permission_cache, &auth_storage)
        .await
        .expect("load users");

    let cache_guard = cache.read().await;
    let users: Vec<_> = cache_guard.all_users().into_iter().cloned().collect();
    assert_eq!(users.len(), 1);
    assert_eq!(users[0].user_id, "fresh");
    let perm_guard = permission_cache.read().await;
    assert!(!perm_guard.is_admin("stale"));
}
