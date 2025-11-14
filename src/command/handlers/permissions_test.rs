use crate::command::handlers::permissions::handle;
use crate::command::types::Command;
use crate::engine::auth::{AuthManager, PermissionSet};
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use crate::logging::init_for_tests;
use crate::shared::response::JsonRenderer;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::io::{AsyncReadExt, duplex};
use tokio::sync::RwLock;

/// Helper function to create test dependencies
async fn create_test_deps() -> (
    Arc<AuthManager>,
    Arc<RwLock<SchemaRegistry>>,
    tempfile::TempDir,
) {
    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();
    let shard_manager = Arc::new(ShardManager::new(1, base_dir, wal_dir).await);
    let auth_manager = Arc::new(AuthManager::new(shard_manager));
    let temp_dir = tempdir().unwrap();
    let schema_path = temp_dir.path().join("schemas.bin");
    let registry = Arc::new(RwLock::new(
        SchemaRegistry::new_with_path(schema_path).unwrap(),
    ));
    (auth_manager, registry, temp_dir)
}

/// Helper function to create an admin user
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

/// Helper function to create a regular user
async fn create_regular_user(auth_manager: &Arc<AuthManager>, user_id: &str) {
    auth_manager
        .create_user(user_id.to_string(), Some("user_secret".to_string()))
        .await
        .expect("Failed to create user");
}

/// Helper function to read response from reader
async fn read_response<R: tokio::io::AsyncRead + Unpin>(reader: &mut R) -> String {
    let mut buffer = vec![0u8; 4096];
    let n = reader.read(&mut buffer).await.unwrap();
    String::from_utf8_lossy(&buffer[..n]).to_string()
}

/// Helper function to convert CommandMiniSchema to MiniSchema
fn create_mini_schema() -> crate::engine::schema::registry::MiniSchema {
    use crate::engine::schema::types::FieldType;
    crate::engine::schema::registry::MiniSchema {
        fields: HashMap::from([("id".to_string(), FieldType::I64)]),
    }
}

#[tokio::test]
async fn test_grant_permission_success() {
    init_for_tests();

    let (auth_manager, registry, _temp_dir) = create_test_deps().await;
    let admin_id = create_admin_user(&auth_manager).await;
    create_regular_user(&auth_manager, "test_user").await;

    // Define schema for event type
    {
        let mut reg = registry.write().await;
        reg.define("test_event", create_mini_schema())
            .expect("Failed to define schema");
    }

    let cmd = Command::GrantPermission {
        permissions: vec!["read".to_string(), "write".to_string()],
        event_types: vec!["test_event".to_string()],
        user_id: "test_user".to_string(),
    };

    let (mut reader, mut writer) = duplex(4096);

    handle(
        &cmd,
        &auth_manager,
        &registry,
        Some(&admin_id),
        &mut writer,
        &JsonRenderer,
    )
    .await
    .expect("Handler should not fail");

    let response = read_response(&mut reader).await;
    assert!(response.contains("200"));
    assert!(response.contains("Permissions granted"));

    // Verify permission was granted
    let permissions = auth_manager.get_permissions("test_user").await.unwrap();
    assert!(permissions.contains_key("test_event"));
    let perm_set = permissions.get("test_event").unwrap();
    assert!(perm_set.read);
    assert!(perm_set.write);
}

#[tokio::test]
async fn test_grant_permission_error_no_auth() {
    init_for_tests();

    let (auth_manager, registry, _temp_dir) = create_test_deps().await;

    let cmd = Command::GrantPermission {
        permissions: vec!["read".to_string()],
        event_types: vec!["test_event".to_string()],
        user_id: "test_user".to_string(),
    };

    let (mut reader, mut writer) = duplex(4096);

    handle(
        &cmd,
        &auth_manager,
        &registry,
        None,
        &mut writer,
        &JsonRenderer,
    )
    .await
    .expect("Handler should not fail");

    let response = read_response(&mut reader).await;
    assert!(response.contains("401") || response.contains("Unauthorized"));
    assert!(response.contains("Authentication required"));
}

#[tokio::test]
async fn test_grant_permission_error_non_admin() {
    init_for_tests();

    let (auth_manager, registry, _temp_dir) = create_test_deps().await;
    create_regular_user(&auth_manager, "regular_user").await;
    create_regular_user(&auth_manager, "test_user").await;

    let cmd = Command::GrantPermission {
        permissions: vec!["read".to_string()],
        event_types: vec!["test_event".to_string()],
        user_id: "test_user".to_string(),
    };

    let (mut reader, mut writer) = duplex(4096);

    handle(
        &cmd,
        &auth_manager,
        &registry,
        Some("regular_user"),
        &mut writer,
        &JsonRenderer,
    )
    .await
    .expect("Handler should not fail");

    let response = read_response(&mut reader).await;
    assert!(response.contains("403") || response.contains("Forbidden"));
    assert!(response.contains("Only admin users can manage permissions"));
}

#[tokio::test]
async fn test_grant_permission_error_invalid_permission() {
    init_for_tests();

    let (auth_manager, registry, _temp_dir) = create_test_deps().await;
    let admin_id = create_admin_user(&auth_manager).await;

    let cmd = Command::GrantPermission {
        permissions: vec!["invalid_perm".to_string()],
        event_types: vec!["test_event".to_string()],
        user_id: "test_user".to_string(),
    };

    let (mut reader, mut writer) = duplex(4096);

    handle(
        &cmd,
        &auth_manager,
        &registry,
        Some(&admin_id),
        &mut writer,
        &JsonRenderer,
    )
    .await
    .expect("Handler should not fail");

    let response = read_response(&mut reader).await;
    assert!(response.contains("400") || response.contains("BadRequest"));
    assert!(response.contains("Invalid permission"));
    assert!(response.contains("invalid_perm"));
}

#[tokio::test]
async fn test_grant_permission_error_schema_not_found() {
    init_for_tests();

    let (auth_manager, registry, _temp_dir) = create_test_deps().await;
    let admin_id = create_admin_user(&auth_manager).await;
    create_regular_user(&auth_manager, "test_user").await;

    let cmd = Command::GrantPermission {
        permissions: vec!["read".to_string()],
        event_types: vec!["nonexistent_event".to_string()],
        user_id: "test_user".to_string(),
    };

    let (mut reader, mut writer) = duplex(4096);

    handle(
        &cmd,
        &auth_manager,
        &registry,
        Some(&admin_id),
        &mut writer,
        &JsonRenderer,
    )
    .await
    .expect("Handler should not fail");

    let response = read_response(&mut reader).await;
    assert!(response.contains("400") || response.contains("BadRequest"));
    assert!(response.contains("No schema defined for event type"));
    assert!(response.contains("nonexistent_event"));
}

#[tokio::test]
async fn test_grant_permission_merges_with_existing() {
    init_for_tests();

    let (auth_manager, registry, _temp_dir) = create_test_deps().await;
    let admin_id = create_admin_user(&auth_manager).await;
    create_regular_user(&auth_manager, "test_user").await;

    // Define schema
    {
        let mut reg = registry.write().await;
        reg.define("test_event", create_mini_schema())
            .expect("Failed to define schema");
    }

    // Grant read permission first
    auth_manager
        .grant_permission("test_user", "test_event", PermissionSet::read_only())
        .await
        .expect("Failed to grant permission");

    // Grant write permission (should merge with existing read)
    let cmd = Command::GrantPermission {
        permissions: vec!["write".to_string()],
        event_types: vec!["test_event".to_string()],
        user_id: "test_user".to_string(),
    };

    let (mut reader, mut writer) = duplex(4096);

    handle(
        &cmd,
        &auth_manager,
        &registry,
        Some(&admin_id),
        &mut writer,
        &JsonRenderer,
    )
    .await
    .expect("Handler should not fail");

    let response = read_response(&mut reader).await;
    assert!(response.contains("200"));

    // Verify both read and write are present
    let permissions = auth_manager.get_permissions("test_user").await.unwrap();
    let perm_set = permissions.get("test_event").unwrap();
    assert!(perm_set.read);
    assert!(perm_set.write);
}

#[tokio::test]
async fn test_grant_permission_multiple_event_types() {
    init_for_tests();

    let (auth_manager, registry, _temp_dir) = create_test_deps().await;
    let admin_id = create_admin_user(&auth_manager).await;
    create_regular_user(&auth_manager, "test_user").await;

    // Define schemas
    {
        let mut reg = registry.write().await;
        reg.define("event1", create_mini_schema())
            .expect("Failed to define schema");
        reg.define("event2", create_mini_schema())
            .expect("Failed to define schema");
    }

    let cmd = Command::GrantPermission {
        permissions: vec!["read".to_string()],
        event_types: vec!["event1".to_string(), "event2".to_string()],
        user_id: "test_user".to_string(),
    };

    let (mut reader, mut writer) = duplex(4096);

    handle(
        &cmd,
        &auth_manager,
        &registry,
        Some(&admin_id),
        &mut writer,
        &JsonRenderer,
    )
    .await
    .expect("Handler should not fail");

    let response = read_response(&mut reader).await;
    assert!(response.contains("200"));

    // Verify both event types have permissions
    let permissions = auth_manager.get_permissions("test_user").await.unwrap();
    assert!(permissions.contains_key("event1"));
    assert!(permissions.contains_key("event2"));
}

#[tokio::test]
async fn test_grant_permission_error_user_not_found() {
    init_for_tests();

    let (auth_manager, registry, _temp_dir) = create_test_deps().await;
    let admin_id = create_admin_user(&auth_manager).await;

    // Define schema
    {
        let mut reg = registry.write().await;
        reg.define("test_event", create_mini_schema())
            .expect("Failed to define schema");
    }

    let cmd = Command::GrantPermission {
        permissions: vec!["read".to_string()],
        event_types: vec!["test_event".to_string()],
        user_id: "nonexistent_user".to_string(),
    };

    let (mut reader, mut writer) = duplex(4096);

    handle(
        &cmd,
        &auth_manager,
        &registry,
        Some(&admin_id),
        &mut writer,
        &JsonRenderer,
    )
    .await
    .expect("Handler should not fail");

    let response = read_response(&mut reader).await;
    assert!(response.contains("400") || response.contains("BadRequest"));
    assert!(response.contains("Failed to grant permission"));
}

#[tokio::test]
async fn test_revoke_permission_success_full_revocation() {
    init_for_tests();

    let (auth_manager, registry, _temp_dir) = create_test_deps().await;
    let admin_id = create_admin_user(&auth_manager).await;
    create_regular_user(&auth_manager, "test_user").await;

    // Define schema
    {
        let mut reg = registry.write().await;
        reg.define("test_event", create_mini_schema())
            .expect("Failed to define schema");
    }

    // Grant permission first
    auth_manager
        .grant_permission("test_user", "test_event", PermissionSet::read_write())
        .await
        .expect("Failed to grant permission");

    // Revoke all permissions (empty permissions list means revoke all)
    let cmd = Command::RevokePermission {
        permissions: vec![],
        event_types: vec!["test_event".to_string()],
        user_id: "test_user".to_string(),
    };

    let (mut reader, mut writer) = duplex(4096);

    handle(
        &cmd,
        &auth_manager,
        &registry,
        Some(&admin_id),
        &mut writer,
        &JsonRenderer,
    )
    .await
    .expect("Handler should not fail");

    let response = read_response(&mut reader).await;
    assert!(response.contains("200"));
    assert!(response.contains("Permissions revoked"));

    // Verify permission was removed
    let permissions = auth_manager.get_permissions("test_user").await.unwrap();
    assert!(!permissions.contains_key("test_event"));
}

#[tokio::test]
async fn test_revoke_permission_partial_revocation() {
    init_for_tests();

    let (auth_manager, registry, _temp_dir) = create_test_deps().await;
    let admin_id = create_admin_user(&auth_manager).await;
    create_regular_user(&auth_manager, "test_user").await;

    // Define schema
    {
        let mut reg = registry.write().await;
        reg.define("test_event", create_mini_schema())
            .expect("Failed to define schema");
    }

    // Grant read-write permission first
    auth_manager
        .grant_permission("test_user", "test_event", PermissionSet::read_write())
        .await
        .expect("Failed to grant permission");

    // Revoke only write permission
    let cmd = Command::RevokePermission {
        permissions: vec!["write".to_string()],
        event_types: vec!["test_event".to_string()],
        user_id: "test_user".to_string(),
    };

    let (mut reader, mut writer) = duplex(4096);

    handle(
        &cmd,
        &auth_manager,
        &registry,
        Some(&admin_id),
        &mut writer,
        &JsonRenderer,
    )
    .await
    .expect("Handler should not fail");

    let response = read_response(&mut reader).await;
    assert!(response.contains("200"));

    // Verify only read permission remains
    let permissions = auth_manager.get_permissions("test_user").await.unwrap();
    let perm_set = permissions.get("test_event").unwrap();
    assert!(perm_set.read);
    assert!(!perm_set.write);
}

#[tokio::test]
async fn test_revoke_permission_multiple_event_types() {
    init_for_tests();

    let (auth_manager, registry, _temp_dir) = create_test_deps().await;
    let admin_id = create_admin_user(&auth_manager).await;
    create_regular_user(&auth_manager, "test_user").await;

    // Define schemas
    {
        let mut reg = registry.write().await;
        reg.define("event1", create_mini_schema())
            .expect("Failed to define schema");
        reg.define("event2", create_mini_schema())
            .expect("Failed to define schema");
    }

    // Grant permissions first
    auth_manager
        .grant_permission("test_user", "event1", PermissionSet::read_write())
        .await
        .expect("Failed to grant permission");
    auth_manager
        .grant_permission("test_user", "event2", PermissionSet::read_write())
        .await
        .expect("Failed to grant permission");

    // Revoke from both
    let cmd = Command::RevokePermission {
        permissions: vec!["write".to_string()],
        event_types: vec!["event1".to_string(), "event2".to_string()],
        user_id: "test_user".to_string(),
    };

    let (mut reader, mut writer) = duplex(4096);

    handle(
        &cmd,
        &auth_manager,
        &registry,
        Some(&admin_id),
        &mut writer,
        &JsonRenderer,
    )
    .await
    .expect("Handler should not fail");

    let response = read_response(&mut reader).await;
    assert!(response.contains("200"));

    // Verify both event types have only read permission
    let permissions = auth_manager.get_permissions("test_user").await.unwrap();
    assert!(permissions.get("event1").unwrap().read);
    assert!(!permissions.get("event1").unwrap().write);
    assert!(permissions.get("event2").unwrap().read);
    assert!(!permissions.get("event2").unwrap().write);
}

#[tokio::test]
async fn test_revoke_permission_error_user_not_found() {
    init_for_tests();

    let (auth_manager, registry, _temp_dir) = create_test_deps().await;
    let admin_id = create_admin_user(&auth_manager).await;

    // Define schema
    {
        let mut reg = registry.write().await;
        reg.define("test_event", create_mini_schema())
            .expect("Failed to define schema");
    }

    let cmd = Command::RevokePermission {
        permissions: vec!["read".to_string()],
        event_types: vec!["test_event".to_string()],
        user_id: "nonexistent_user".to_string(),
    };

    let (mut reader, mut writer) = duplex(4096);

    handle(
        &cmd,
        &auth_manager,
        &registry,
        Some(&admin_id),
        &mut writer,
        &JsonRenderer,
    )
    .await
    .expect("Handler should not fail");

    let response = read_response(&mut reader).await;
    assert!(response.contains("400") || response.contains("BadRequest"));
    assert!(response.contains("Failed to revoke permission"));
}

#[tokio::test]
async fn test_show_permissions_success_with_permissions() {
    init_for_tests();

    let (auth_manager, registry, _temp_dir) = create_test_deps().await;
    let admin_id = create_admin_user(&auth_manager).await;
    create_regular_user(&auth_manager, "test_user").await;

    // Grant some permissions
    auth_manager
        .grant_permission("test_user", "event1", PermissionSet::read_write())
        .await
        .expect("Failed to grant permission");
    auth_manager
        .grant_permission("test_user", "event2", PermissionSet::read_only())
        .await
        .expect("Failed to grant permission");

    let cmd = Command::ShowPermissions {
        user_id: "test_user".to_string(),
    };

    let (mut reader, mut writer) = duplex(4096);

    handle(
        &cmd,
        &auth_manager,
        &registry,
        Some(&admin_id),
        &mut writer,
        &JsonRenderer,
    )
    .await
    .expect("Handler should not fail");

    let response = read_response(&mut reader).await;
    assert!(response.contains("200"));
    assert!(response.contains("Permissions for user 'test_user'"));
    assert!(response.contains("event1"));
    assert!(response.contains("event2"));
}

#[tokio::test]
async fn test_show_permissions_success_no_permissions() {
    init_for_tests();

    let (auth_manager, registry, _temp_dir) = create_test_deps().await;
    let admin_id = create_admin_user(&auth_manager).await;
    create_regular_user(&auth_manager, "test_user").await;

    let cmd = Command::ShowPermissions {
        user_id: "test_user".to_string(),
    };

    let (mut reader, mut writer) = duplex(4096);

    handle(
        &cmd,
        &auth_manager,
        &registry,
        Some(&admin_id),
        &mut writer,
        &JsonRenderer,
    )
    .await
    .expect("Handler should not fail");

    let response = read_response(&mut reader).await;
    assert!(response.contains("200"));
    assert!(response.contains("User 'test_user' has no permissions"));
}

#[tokio::test]
async fn test_show_permissions_error_user_not_found() {
    init_for_tests();

    let (auth_manager, registry, _temp_dir) = create_test_deps().await;
    let admin_id = create_admin_user(&auth_manager).await;

    let cmd = Command::ShowPermissions {
        user_id: "nonexistent_user".to_string(),
    };

    let (mut reader, mut writer) = duplex(4096);

    handle(
        &cmd,
        &auth_manager,
        &registry,
        Some(&admin_id),
        &mut writer,
        &JsonRenderer,
    )
    .await
    .expect("Handler should not fail");

    let response = read_response(&mut reader).await;
    assert!(response.contains("400") || response.contains("BadRequest"));
    assert!(response.contains("Failed to get permissions"));
}

#[tokio::test]
async fn test_show_permissions_error_no_auth() {
    init_for_tests();

    let (auth_manager, registry, _temp_dir) = create_test_deps().await;

    let cmd = Command::ShowPermissions {
        user_id: "test_user".to_string(),
    };

    let (mut reader, mut writer) = duplex(4096);

    handle(
        &cmd,
        &auth_manager,
        &registry,
        None,
        &mut writer,
        &JsonRenderer,
    )
    .await
    .expect("Handler should not fail");

    let response = read_response(&mut reader).await;
    assert!(response.contains("401") || response.contains("Unauthorized"));
}

#[tokio::test]
async fn test_show_permissions_error_non_admin() {
    init_for_tests();

    let (auth_manager, registry, _temp_dir) = create_test_deps().await;
    create_regular_user(&auth_manager, "regular_user").await;

    let cmd = Command::ShowPermissions {
        user_id: "test_user".to_string(),
    };

    let (mut reader, mut writer) = duplex(4096);

    handle(
        &cmd,
        &auth_manager,
        &registry,
        Some("regular_user"),
        &mut writer,
        &JsonRenderer,
    )
    .await
    .expect("Handler should not fail");

    let response = read_response(&mut reader).await;
    assert!(response.contains("403") || response.contains("Forbidden"));
}

#[tokio::test]
async fn test_show_permissions_displays_permission_types() {
    init_for_tests();

    let (auth_manager, registry, _temp_dir) = create_test_deps().await;
    let admin_id = create_admin_user(&auth_manager).await;
    create_regular_user(&auth_manager, "test_user").await;

    // Grant different permission types
    auth_manager
        .grant_permission("test_user", "read_write", PermissionSet::read_write())
        .await
        .expect("Failed to grant permission");
    auth_manager
        .grant_permission("test_user", "read_only", PermissionSet::read_only())
        .await
        .expect("Failed to grant permission");
    auth_manager
        .grant_permission("test_user", "write_only", PermissionSet::write_only())
        .await
        .expect("Failed to grant permission");

    let cmd = Command::ShowPermissions {
        user_id: "test_user".to_string(),
    };

    let (mut reader, mut writer) = duplex(4096);

    handle(
        &cmd,
        &auth_manager,
        &registry,
        Some(&admin_id),
        &mut writer,
        &JsonRenderer,
    )
    .await
    .expect("Handler should not fail");

    let response = read_response(&mut reader).await;
    assert!(response.contains("200"));
    assert!(response.contains("read_write"));
    assert!(response.contains("read_only"));
    assert!(response.contains("write_only"));
    // Verify format includes both read and write for read_write
    assert!(response.contains("read") && response.contains("write"));
}

#[tokio::test]
async fn test_invalid_command_variant() {
    init_for_tests();

    let (auth_manager, registry, _temp_dir) = create_test_deps().await;
    let admin_id = create_admin_user(&auth_manager).await;

    // Use a command that's not handled by permissions handler
    let cmd = Command::Ping;

    let (mut reader, mut writer) = duplex(4096);

    handle(
        &cmd,
        &auth_manager,
        &registry,
        Some(&admin_id),
        &mut writer,
        &JsonRenderer,
    )
    .await
    .expect("Handler should not fail");

    let response = read_response(&mut reader).await;
    assert!(response.contains("400") || response.contains("BadRequest"));
    assert!(response.contains("Invalid command variant"));
}

#[tokio::test]
async fn test_grant_permission_read_only() {
    init_for_tests();

    let (auth_manager, registry, _temp_dir) = create_test_deps().await;
    let admin_id = create_admin_user(&auth_manager).await;
    create_regular_user(&auth_manager, "test_user").await;

    // Define schema
    {
        let mut reg = registry.write().await;
        reg.define("test_event", create_mini_schema())
            .expect("Failed to define schema");
    }

    let cmd = Command::GrantPermission {
        permissions: vec!["read".to_string()],
        event_types: vec!["test_event".to_string()],
        user_id: "test_user".to_string(),
    };

    let (mut reader, mut writer) = duplex(4096);

    handle(
        &cmd,
        &auth_manager,
        &registry,
        Some(&admin_id),
        &mut writer,
        &JsonRenderer,
    )
    .await
    .expect("Handler should not fail");

    let response = read_response(&mut reader).await;
    assert!(response.contains("200"));

    // Verify only read permission
    let permissions = auth_manager.get_permissions("test_user").await.unwrap();
    let perm_set = permissions.get("test_event").unwrap();
    assert!(perm_set.read);
    assert!(!perm_set.write);
}

#[tokio::test]
async fn test_grant_permission_write_only() {
    init_for_tests();

    let (auth_manager, registry, _temp_dir) = create_test_deps().await;
    let admin_id = create_admin_user(&auth_manager).await;
    create_regular_user(&auth_manager, "test_user").await;

    // Define schema
    {
        let mut reg = registry.write().await;
        reg.define("test_event", create_mini_schema())
            .expect("Failed to define schema");
    }

    let cmd = Command::GrantPermission {
        permissions: vec!["write".to_string()],
        event_types: vec!["test_event".to_string()],
        user_id: "test_user".to_string(),
    };

    let (mut reader, mut writer) = duplex(4096);

    handle(
        &cmd,
        &auth_manager,
        &registry,
        Some(&admin_id),
        &mut writer,
        &JsonRenderer,
    )
    .await
    .expect("Handler should not fail");

    let response = read_response(&mut reader).await;
    assert!(response.contains("200"));

    // Verify only write permission
    let permissions = auth_manager.get_permissions("test_user").await.unwrap();
    let perm_set = permissions.get("test_event").unwrap();
    assert!(!perm_set.read);
    assert!(perm_set.write);
}

#[tokio::test]
async fn test_revoke_permission_nonexistent_permission() {
    init_for_tests();

    let (auth_manager, registry, _temp_dir) = create_test_deps().await;
    let admin_id = create_admin_user(&auth_manager).await;
    create_regular_user(&auth_manager, "test_user").await;

    // Define schema
    {
        let mut reg = registry.write().await;
        reg.define("test_event", create_mini_schema())
            .expect("Failed to define schema");
    }

    // Try to revoke permission that doesn't exist (should succeed, just no-op)
    let cmd = Command::RevokePermission {
        permissions: vec!["read".to_string()],
        event_types: vec!["test_event".to_string()],
        user_id: "test_user".to_string(),
    };

    let (mut reader, mut writer) = duplex(4096);

    handle(
        &cmd,
        &auth_manager,
        &registry,
        Some(&admin_id),
        &mut writer,
        &JsonRenderer,
    )
    .await
    .expect("Handler should not fail");

    let response = read_response(&mut reader).await;
    assert!(response.contains("200"));
    assert!(response.contains("Permissions revoked"));
}

#[tokio::test]
async fn test_permissions_handler_bypass_auth_allows_permission_management() {
    init_for_tests();

    let (auth_manager, registry, _temp_dir) = create_test_deps().await;

    // Define a schema first
    let mut reg = registry.write().await;
    reg.define("test_event", create_mini_schema())
        .expect("Failed to define schema");
    drop(reg);

    // Create a user to grant permissions to
    create_regular_user(&auth_manager, "test_user").await;

    // Test that bypass user can grant permissions (should succeed even without admin role)
    let cmd = Command::GrantPermission {
        user_id: "test_user".to_string(),
        event_types: vec!["test_event".to_string()],
        permissions: vec!["read".to_string(), "write".to_string()],
    };

    let (mut reader, mut writer) = duplex(1024);

    handle(
        &cmd,
        &auth_manager,
        &registry,
        Some("bypass"),
        &mut writer,
        &JsonRenderer,
    )
    .await
    .expect("handler should not fail");

    let response = read_response(&mut reader).await;
    assert!(response.contains("200"));
    assert!(response.contains("Permissions granted"));

    // Verify permissions were granted
    let perms = auth_manager.get_permissions("test_user").await.unwrap();
    assert!(perms.get("test_event").unwrap().read);
    assert!(perms.get("test_event").unwrap().write);
}

#[tokio::test]
async fn test_permissions_handler_bypass_auth_vs_regular_user() {
    init_for_tests();

    let (auth_manager, registry, _temp_dir) = create_test_deps().await;

    // Define a schema first
    let mut reg = registry.write().await;
    reg.define("test_event", create_mini_schema())
        .expect("Failed to define schema");
    drop(reg);

    // Create a regular (non-admin) user
    create_regular_user(&auth_manager, "regular_user").await;
    create_regular_user(&auth_manager, "target_user").await;

    let cmd = Command::GrantPermission {
        user_id: "target_user".to_string(),
        event_types: vec!["test_event".to_string()],
        permissions: vec!["read".to_string()],
    };

    // Test with regular user (should fail - not admin)
    let (mut reader1, mut writer1) = duplex(1024);
    handle(
        &cmd,
        &auth_manager,
        &registry,
        Some("regular_user"),
        &mut writer1,
        &JsonRenderer,
    )
    .await
    .expect("handler should not fail");

    let response1 = read_response(&mut reader1).await;
    assert!(response1.contains("403") || response1.contains("Forbidden"));
    assert!(response1.contains("Only admin users can manage permissions"));

    // Test with bypass user (should succeed)
    let (mut reader2, mut writer2) = duplex(1024);
    handle(
        &cmd,
        &auth_manager,
        &registry,
        Some("bypass"),
        &mut writer2,
        &JsonRenderer,
    )
    .await
    .expect("handler should not fail");

    let response2 = read_response(&mut reader2).await;
    assert!(response2.contains("200"));
    assert!(response2.contains("Permissions granted"));
}
