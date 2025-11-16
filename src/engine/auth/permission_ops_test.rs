use super::permission_ops::{get_permissions, grant_permission, revoke_permission};
use super::storage::AuthWalStorage;
use super::types::{AuthError, PermissionCache, PermissionSet, UserCache};
use super::user_ops::create_user;
use crate::logging::init_for_tests;
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
async fn test_grant_permission_success() {
    init_for_tests();

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    // Create a user first
    create_user(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user".to_string(),
        None,
    )
    .await
    .expect("User creation should succeed");

    // Grant permission
    let permission_set = PermissionSet::read_write();
    let result = grant_permission(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user",
        "event_type1",
        permission_set.clone(),
    )
    .await;

    assert!(result.is_ok());

    // Verify permission was granted
    let permissions = get_permissions(&cache, "test_user").await.unwrap();
    assert!(permissions.contains_key("event_type1"));
    assert_eq!(permissions.get("event_type1").unwrap(), &permission_set);
}

#[tokio::test]
async fn test_grant_permission_error_user_not_found() {
    init_for_tests();

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    let permission_set = PermissionSet::read_write();
    let result = grant_permission(
        &cache,
        &permission_cache,
        &auth_storage,
        "non_existent_user",
        "event_type1",
        permission_set,
    )
    .await;

    assert!(result.is_err());
    match result {
        Err(AuthError::UserNotFound(user_id)) => assert_eq!(user_id, "non_existent_user"),
        _ => panic!("Expected UserNotFound error"),
    }
}

#[tokio::test]
async fn test_grant_permission_multiple_event_types() {
    init_for_tests();

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    // Create a user first
    create_user(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user".to_string(),
        None,
    )
    .await
    .expect("User creation should succeed");

    // Grant permissions for multiple event types
    grant_permission(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user",
        "event_type1",
        PermissionSet::read_write(),
    )
    .await
    .expect("Should grant permission");

    grant_permission(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user",
        "event_type2",
        PermissionSet::read_only(),
    )
    .await
    .expect("Should grant permission");

    grant_permission(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user",
        "event_type3",
        PermissionSet::write_only(),
    )
    .await
    .expect("Should grant permission");

    // Verify all permissions were granted
    let permissions = get_permissions(&cache, "test_user").await.unwrap();
    assert_eq!(permissions.len(), 3);
    assert_eq!(
        permissions.get("event_type1").unwrap(),
        &PermissionSet::read_write()
    );
    assert_eq!(
        permissions.get("event_type2").unwrap(),
        &PermissionSet::read_only()
    );
    assert_eq!(
        permissions.get("event_type3").unwrap(),
        &PermissionSet::write_only()
    );
}

#[tokio::test]
async fn test_grant_permission_overwrites_existing() {
    init_for_tests();

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    // Create a user first
    create_user(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user".to_string(),
        None,
    )
    .await
    .expect("User creation should succeed");

    // Grant read-only permission
    grant_permission(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user",
        "event_type1",
        PermissionSet::read_only(),
    )
    .await
    .expect("Should grant permission");

    // Grant read-write permission (should overwrite)
    grant_permission(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user",
        "event_type1",
        PermissionSet::read_write(),
    )
    .await
    .expect("Should grant permission");

    // Verify permission was overwritten
    let permissions = get_permissions(&cache, "test_user").await.unwrap();
    assert_eq!(
        permissions.get("event_type1").unwrap(),
        &PermissionSet::read_write()
    );
}

#[tokio::test]
async fn test_revoke_permission_success() {
    init_for_tests();

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    // Create a user first
    create_user(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user".to_string(),
        None,
    )
    .await
    .expect("User creation should succeed");

    // Grant permission
    grant_permission(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user",
        "event_type1",
        PermissionSet::read_write(),
    )
    .await
    .expect("Should grant permission");

    // Revoke permission
    let result = revoke_permission(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user",
        "event_type1",
    )
    .await;

    assert!(result.is_ok());

    // Verify permission was revoked
    let permissions = get_permissions(&cache, "test_user").await.unwrap();
    assert!(!permissions.contains_key("event_type1"));
}

#[tokio::test]
async fn test_revoke_permission_error_user_not_found() {
    init_for_tests();

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    let result = revoke_permission(
        &cache,
        &permission_cache,
        &auth_storage,
        "non_existent_user",
        "event_type1",
    )
    .await;

    assert!(result.is_err());
    match result {
        Err(AuthError::UserNotFound(user_id)) => assert_eq!(user_id, "non_existent_user"),
        _ => panic!("Expected UserNotFound error"),
    }
}

#[tokio::test]
async fn test_revoke_permission_nonexistent_event_type() {
    init_for_tests();

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    // Create a user first
    create_user(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user".to_string(),
        None,
    )
    .await
    .expect("User creation should succeed");

    // Revoke permission for event type that doesn't exist (should succeed)
    let result = revoke_permission(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user",
        "nonexistent_event",
    )
    .await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_revoke_permission_multiple_event_types() {
    init_for_tests();

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    // Create a user first
    create_user(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user".to_string(),
        None,
    )
    .await
    .expect("User creation should succeed");

    // Grant permissions for multiple event types
    grant_permission(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user",
        "event_type1",
        PermissionSet::read_write(),
    )
    .await
    .expect("Should grant permission");

    grant_permission(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user",
        "event_type2",
        PermissionSet::read_only(),
    )
    .await
    .expect("Should grant permission");

    grant_permission(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user",
        "event_type3",
        PermissionSet::write_only(),
    )
    .await
    .expect("Should grant permission");

    // Revoke one permission
    revoke_permission(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user",
        "event_type2",
    )
    .await
    .expect("Should revoke permission");

    // Verify only the revoked permission was removed
    let permissions = get_permissions(&cache, "test_user").await.unwrap();
    assert_eq!(permissions.len(), 2);
    assert!(permissions.contains_key("event_type1"));
    assert!(!permissions.contains_key("event_type2"));
    assert!(permissions.contains_key("event_type3"));
}

#[tokio::test]
async fn test_get_permissions_success() {
    init_for_tests();

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    // Create a user first
    create_user(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user".to_string(),
        None,
    )
    .await
    .expect("User creation should succeed");

    // Grant permissions
    grant_permission(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user",
        "event_type1",
        PermissionSet::read_write(),
    )
    .await
    .expect("Should grant permission");

    grant_permission(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user",
        "event_type2",
        PermissionSet::read_only(),
    )
    .await
    .expect("Should grant permission");

    // Get permissions
    let result = get_permissions(&cache, "test_user").await;
    assert!(result.is_ok());

    let permissions = result.unwrap();
    assert_eq!(permissions.len(), 2);
    assert_eq!(
        permissions.get("event_type1").unwrap(),
        &PermissionSet::read_write()
    );
    assert_eq!(
        permissions.get("event_type2").unwrap(),
        &PermissionSet::read_only()
    );
}

#[tokio::test]
async fn test_get_permissions_error_user_not_found() {
    init_for_tests();

    let cache = Arc::new(RwLock::new(UserCache::new()));

    let result = get_permissions(&cache, "non_existent_user").await;

    assert!(result.is_err());
    match result {
        Err(AuthError::UserNotFound(user_id)) => assert_eq!(user_id, "non_existent_user"),
        _ => panic!("Expected UserNotFound error"),
    }
}

#[tokio::test]
async fn test_get_permissions_empty_when_no_permissions() {
    init_for_tests();

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    // Create a user first
    create_user(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user".to_string(),
        None,
    )
    .await
    .expect("User creation should succeed");

    // Get permissions (should be empty)
    let permissions = get_permissions(&cache, "test_user").await.unwrap();
    assert!(permissions.is_empty());
}

#[tokio::test]
async fn test_grant_permission_all_permission_types() {
    init_for_tests();

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    // Create a user first
    create_user(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user".to_string(),
        None,
    )
    .await
    .expect("User creation should succeed");

    // Grant all permission types
    grant_permission(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user",
        "read_write",
        PermissionSet::read_write(),
    )
    .await
    .expect("Should grant permission");

    grant_permission(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user",
        "read_only",
        PermissionSet::read_only(),
    )
    .await
    .expect("Should grant permission");

    grant_permission(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user",
        "write_only",
        PermissionSet::write_only(),
    )
    .await
    .expect("Should grant permission");

    grant_permission(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user",
        "none",
        PermissionSet::none(),
    )
    .await
    .expect("Should grant permission");

    // Verify all permissions
    let permissions = get_permissions(&cache, "test_user").await.unwrap();
    assert_eq!(permissions.len(), 4);
    assert_eq!(
        permissions.get("read_write").unwrap(),
        &PermissionSet::read_write()
    );
    assert_eq!(
        permissions.get("read_only").unwrap(),
        &PermissionSet::read_only()
    );
    assert_eq!(
        permissions.get("write_only").unwrap(),
        &PermissionSet::write_only()
    );
    assert_eq!(permissions.get("none").unwrap(), &PermissionSet::none());
}

#[tokio::test]
async fn test_permission_operations_preserve_other_user_data() {
    init_for_tests();

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    // Create two users
    create_user(
        &cache,
        &permission_cache,
        &auth_storage,
        "user1".to_string(),
        None,
    )
    .await
    .expect("User creation should succeed");

    create_user(
        &cache,
        &permission_cache,
        &auth_storage,
        "user2".to_string(),
        None,
    )
    .await
    .expect("User creation should succeed");

    // Grant permission to user1
    grant_permission(
        &cache,
        &permission_cache,
        &auth_storage,
        "user1",
        "event_type1",
        PermissionSet::read_write(),
    )
    .await
    .expect("Should grant permission");

    // Verify user2 has no permissions
    let user2_permissions = get_permissions(&cache, "user2").await.unwrap();
    assert!(user2_permissions.is_empty());

    // Verify user1 still has permission
    let user1_permissions = get_permissions(&cache, "user1").await.unwrap();
    assert_eq!(user1_permissions.len(), 1);
}

#[tokio::test]
async fn test_revoke_all_permissions() {
    init_for_tests();

    let (cache, permission_cache, auth_storage) = create_test_deps().await;

    // Create a user first
    create_user(
        &cache,
        &permission_cache,
        &auth_storage,
        "test_user".to_string(),
        None,
    )
    .await
    .expect("User creation should succeed");

    // Grant multiple permissions
    for i in 1..=5 {
        grant_permission(
            &cache,
            &permission_cache,
            &auth_storage,
            "test_user",
            &format!("event_type{}", i),
            PermissionSet::read_write(),
        )
        .await
        .expect("Should grant permission");
    }

    // Revoke all permissions
    for i in 1..=5 {
        revoke_permission(
            &cache,
            &permission_cache,
            &auth_storage,
            "test_user",
            &format!("event_type{}", i),
        )
        .await
        .expect("Should revoke permission");
    }

    // Verify all permissions were revoked
    let permissions = get_permissions(&cache, "test_user").await.unwrap();
    assert!(permissions.is_empty());
}
