use crate::command::handlers::define;
use crate::command::types::{Command, FieldSpec, MiniSchema};
use crate::engine::auth::AuthManager;
use crate::engine::schema::registry::SchemaRegistry;
use crate::engine::schema::{EnumType, FieldType};
use crate::engine::shard::manager::ShardManager;
use crate::shared::response::JsonRenderer;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::RwLock;

#[tokio::test]
async fn test_define_handler_success() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let schema_dir = tempdir().unwrap();
    let schema_path = schema_dir.path().join("schemas.bin");
    let registry = Arc::new(RwLock::new(
        SchemaRegistry::new_with_path(schema_path).unwrap(),
    ));
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    let cmd = Command::Define {
        event_type: "signup".to_string(),
        version: Some(1),
        schema: MiniSchema {
            fields: HashMap::from([
                (
                    "field1".to_string(),
                    FieldSpec::Primitive("string".to_string()),
                ),
                (
                    "field2".to_string(),
                    FieldSpec::Primitive("u64".to_string()),
                ),
            ]),
        },
    };

    let (_reader, mut writer) = tokio::io::duplex(1024);

    define::handle(
        &cmd,
        &shard_manager,
        &registry,
        None,
        None,
        &mut writer,
        &JsonRenderer,
    )
    .await
    .unwrap();

    // Confirm it was added to the registry
    let r = registry.read().await;

    assert_eq!(r.get("signup").unwrap().fields.len(), 2);
    assert_eq!(r.get("signup").unwrap().fields["field1"], FieldType::String);
    assert_eq!(r.get("signup").unwrap().fields["field2"], FieldType::U64);
}

#[tokio::test]
async fn test_define_handler_with_enum_field() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let schema_dir = tempdir().unwrap();
    let schema_path = schema_dir.path().join("schemas.bin");
    let registry = Arc::new(RwLock::new(
        SchemaRegistry::new_with_path(schema_path).unwrap(),
    ));
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    let cmd = Command::Define {
        event_type: "subscription".to_string(),
        version: None,
        schema: MiniSchema {
            fields: HashMap::from([(
                "plan".to_string(),
                FieldSpec::Enum(vec!["pro".to_string(), "basic".to_string()]),
            )]),
        },
    };

    let (_reader, mut writer) = tokio::io::duplex(1024);

    define::handle(
        &cmd,
        &shard_manager,
        &registry,
        None,
        None,
        &mut writer,
        &JsonRenderer,
    )
    .await
    .unwrap();

    let r = registry.read().await;
    let schema = r.get("subscription").unwrap();
    assert_eq!(schema.fields.len(), 1);
    assert_eq!(
        schema.fields["plan"],
        FieldType::Enum(EnumType {
            variants: vec!["pro".to_string(), "basic".to_string()],
        })
    );
}

#[tokio::test]
async fn test_define_handler_with_optional_field() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let schema_dir = tempdir().unwrap();
    let schema_path = schema_dir.path().join("schemas.bin");
    let registry = Arc::new(RwLock::new(
        SchemaRegistry::new_with_path(schema_path).unwrap(),
    ));
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    let cmd = Command::Define {
        event_type: "user_profile".to_string(),
        version: None,
        schema: MiniSchema {
            fields: HashMap::from([(
                "nickname".to_string(),
                FieldSpec::Primitive("string | null".to_string()),
            )]),
        },
    };

    let (_reader, mut writer) = tokio::io::duplex(1024);

    define::handle(
        &cmd,
        &shard_manager,
        &registry,
        None,
        None,
        &mut writer,
        &JsonRenderer,
    )
    .await
    .unwrap();

    let r = registry.read().await;
    let ft = &r.get("user_profile").unwrap().fields["nickname"];
    assert_eq!(ft, &FieldType::Optional(Box::new(FieldType::String)));
}

#[tokio::test]
async fn test_define_handler_engine_error() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let schema_dir = tempdir().unwrap();
    let schema_path = schema_dir.path().join("schemas.bin");
    let registry = Arc::new(RwLock::new(
        SchemaRegistry::new_with_path(schema_path).unwrap(),
    ));
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    let cmd = Command::Define {
        event_type: "invalid".to_string(),
        version: Some(1),
        schema: MiniSchema {
            fields: HashMap::new(),
        },
    };

    let (_reader, mut writer) = tokio::io::duplex(1024);

    define::handle(
        &cmd,
        &shard_manager,
        &registry,
        None,
        None,
        &mut writer,
        &JsonRenderer,
    )
    .await
    .unwrap();

    let r = registry.read().await;
    assert!(r.get("invalid").is_none());
}

#[tokio::test]
async fn test_define_handler_bypass_auth_allows_definition() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let schema_dir = tempdir().unwrap();
    let schema_path = schema_dir.path().join("schemas.bin");
    let registry = Arc::new(RwLock::new(
        SchemaRegistry::new_with_path(schema_path).unwrap(),
    ));
    let shard_manager = Arc::new(ShardManager::new(1, base_dir, wal_dir).await);
    let auth_manager = Arc::new(AuthManager::new(Arc::clone(&shard_manager)));

    let cmd = Command::Define {
        event_type: "bypass_event".to_string(),
        version: None,
        schema: MiniSchema {
            fields: HashMap::from([(
                "field1".to_string(),
                FieldSpec::Primitive("string".to_string()),
            )]),
        },
    };

    let (_reader, mut writer) = tokio::io::duplex(1024);

    // Test with bypass user (should succeed even without admin role)
    define::handle(
        &cmd,
        shard_manager.as_ref(),
        &registry,
        Some(&auth_manager),
        Some("bypass"),
        &mut writer,
        &JsonRenderer,
    )
    .await
    .unwrap();

    // Confirm it was added to the registry
    let r = registry.read().await;
    assert!(r.get("bypass_event").is_some());
    assert_eq!(r.get("bypass_event").unwrap().fields.len(), 1);
}

#[tokio::test]
async fn test_define_handler_bypass_auth_vs_regular_user() {
    use crate::logging::init_for_tests;
    use tokio::io::AsyncReadExt;
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let schema_dir = tempdir().unwrap();
    let schema_path = schema_dir.path().join("schemas.bin");
    let registry = Arc::new(RwLock::new(
        SchemaRegistry::new_with_path(schema_path).unwrap(),
    ));
    let shard_manager = Arc::new(ShardManager::new(1, base_dir, wal_dir).await);
    let auth_manager = Arc::new(AuthManager::new(Arc::clone(&shard_manager)));

    // Create a regular (non-admin) user
    auth_manager
        .create_user("regular_user".to_string(), Some("secret".to_string()))
        .await
        .unwrap();

    let cmd = Command::Define {
        event_type: "test_event".to_string(),
        version: None,
        schema: MiniSchema {
            fields: HashMap::from([(
                "field1".to_string(),
                FieldSpec::Primitive("string".to_string()),
            )]),
        },
    };

    // Test with regular user (should fail - not admin)
    let (mut reader1, mut writer1) = tokio::io::duplex(1024);
    define::handle(
        &cmd,
        shard_manager.as_ref(),
        &registry,
        Some(&auth_manager),
        Some("regular_user"),
        &mut writer1,
        &JsonRenderer,
    )
    .await
    .unwrap();

    let mut response = vec![0u8; 1024];
    let n = reader1.read(&mut response).await.unwrap();
    let msg = String::from_utf8_lossy(&response[..n]);
    assert!(msg.contains("403") || msg.contains("Forbidden"));
    assert!(msg.contains("Only admin users can define schemas"));

    // Test with bypass user (should succeed)
    let (_reader2, mut writer2) = tokio::io::duplex(1024);
    define::handle(
        &cmd,
        shard_manager.as_ref(),
        &registry,
        Some(&auth_manager),
        Some("bypass"),
        &mut writer2,
        &JsonRenderer,
    )
    .await
    .unwrap();

    // Confirm it was added to the registry
    let r = registry.read().await;
    assert!(r.get("test_event").is_some());
}
