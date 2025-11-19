use crate::command::dispatcher::dispatch_command;
use crate::command::parser::command::parse_command;
use crate::command::parser::error::ParseError;
use crate::command::types::Command;
use crate::command::types::FieldSpec;
use crate::engine::auth::AuthManager;
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use crate::logging::init_for_tests;
use crate::shared::response::unix::UnixRenderer;
use serde_json::json;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::io::{AsyncReadExt, duplex};
use tokio::sync::RwLock;

#[test]
fn test_parse_command_store_basic() {
    let input = r#"STORE order_created FOR user-9 PAYLOAD { "id": 9 }"#;
    let command = parse_command(input).expect("Failed to parse STORE command");

    assert_eq!(
        command,
        Command::Store {
            event_type: "order_created".to_string(),
            context_id: "user-9".to_string(),
            payload: json!({ "id": 9 }),
        }
    );
}

#[test]
fn test_parse_command_store_with_complex_payload_should_fail() {
    // PEG parser now allows nested JSON (unlike the old token-based parser)
    // This test should verify that nested JSON is accepted
    let input =
        r#"STORE order_created FOR user-9 PAYLOAD { "id": 9, "details": { "product": "book" } }"#;
    let result = parse_command(input);

    assert!(
        result.is_ok(),
        "PEG parser should accept nested JSON payloads"
    );

    // Verify the nested structure is preserved
    if let Ok(Command::Store { payload, .. }) = result {
        assert_eq!(payload["id"], 9);
        assert_eq!(payload["details"]["product"], "book");
    } else {
        panic!("Expected Command::Store");
    }
}

#[test]
fn test_parse_command_query_basic() {
    let input = r#"QUERY orders WHERE status = "pending""#;
    let command = parse_command(input).expect("Failed to parse QUERY command");

    if let Command::Query { event_type, .. } = command {
        assert_eq!(event_type, "orders");
    } else {
        panic!("Expected Command::Query");
    }
}

#[test]
fn test_parse_command_query_with_limit() {
    let input = r#"QUERY login_events WHERE success = true LIMIT 100"#;
    let command = parse_command(input).expect("Failed to parse QUERY with LIMIT");

    if let Command::Query {
        event_type, limit, ..
    } = command
    {
        assert_eq!(event_type, "login_events");
        assert_eq!(limit, Some(100));
    } else {
        panic!("Expected Command::Query");
    }
}

#[test]
fn test_parse_command_query_with_return_clause() {
    let input = r#"QUERY orders RETURN [id, "status"] WHERE status = "pending" LIMIT 10"#;
    let command = parse_command(input).expect("Failed to parse QUERY with RETURN");

    if let Command::Query {
        event_type,
        where_clause,
        limit,
        ..
    } = command
    {
        assert_eq!(event_type, "orders");
        assert_eq!(limit, Some(10));
        assert!(where_clause.is_some());
    } else {
        panic!("Expected Command::Query");
    }
}

#[test]
fn test_parse_command_replay_with_full_arguments() {
    let input = r#"REPLAY orders FOR user-123 SINCE "2024-01-01T00:00:00Z""#;
    let command = parse_command(input).expect("Failed to parse REPLAY command");

    if let Command::Replay {
        event_type,
        context_id,
        since,
        ..
    } = command
    {
        assert_eq!(event_type.unwrap(), "orders");
        assert_eq!(context_id, "user-123");
        assert_eq!(since.unwrap(), "2024-01-01T00:00:00Z");
    } else {
        panic!("Expected Command::Replay");
    }
}

#[test]
fn test_parse_command_replay_with_return_clause() {
    let input = r#"REPLAY orders FOR user-123 RETURN [id, "status"]"#;
    let command = parse_command(input).expect("Failed to parse REPLAY with RETURN");

    if let Command::Replay {
        event_type,
        context_id,
        since,
        ..
    } = command
    {
        assert_eq!(event_type.unwrap(), "orders");
        assert_eq!(context_id, "user-123");
        assert_eq!(since, None);
    } else {
        panic!("Expected Command::Replay");
    }
}

#[test]
fn test_parse_command_replay_without_event_type() {
    let input = r#"REPLAY FOR user-123"#;
    let command = parse_command(input).expect("Failed to parse REPLAY without event_type");

    if let Command::Replay {
        event_type,
        context_id,
        since,
        ..
    } = command
    {
        assert_eq!(event_type, None);
        assert_eq!(context_id, "user-123");
        assert_eq!(since, None);
    } else {
        panic!("Expected Command::Replay");
    }
}

#[test]
fn test_parse_command_define_basic() {
    let input = r#"DEFINE order_created FIELDS { "id": "number", "status": "string" }"#;
    let command = parse_command(input).expect("Failed to parse DEFINE command");

    if let Command::Define {
        event_type,
        version: _,
        schema,
    } = command
    {
        assert_eq!(event_type, "order_created");
        assert_eq!(
            schema.fields["id"],
            FieldSpec::Primitive("number".to_string())
        );
        assert_eq!(
            schema.fields["status"],
            FieldSpec::Primitive("string".to_string())
        );
    } else {
        panic!("Expected Command::Define");
    }
}

#[test]
fn test_parse_command_define_with_more_fields() {
    let input = r#"DEFINE user_signed_up FIELDS { "user_id": "string", "email": "string", "age": "number" }"#;
    let command = parse_command(input).expect("Failed to parse DEFINE with extra fields");

    if let Command::Define {
        event_type,
        version: _,
        schema,
    } = command
    {
        assert_eq!(event_type, "user_signed_up");
        assert_eq!(
            schema.fields["user_id"],
            FieldSpec::Primitive("string".to_string())
        );
        assert_eq!(
            schema.fields["email"],
            FieldSpec::Primitive("string".to_string())
        );
        assert_eq!(
            schema.fields["age"],
            FieldSpec::Primitive("number".to_string())
        );
    } else {
        panic!("Expected Command::Define");
    }
}
#[test]
fn test_parse_command_ping() {
    let input = "PING";
    let command = parse_command(input).expect("Failed to parse PING command");

    assert_eq!(command, Command::Ping);
}

#[tokio::test]
async fn test_dispatch_ping() {
    init_for_tests();

    let (_shard_manager, registry, _auth_manager, _temp_dir) = create_test_components().await;
    let cmd = Command::Ping;

    let (mut reader, mut writer) = duplex(1024);

    dispatch_command(
        &cmd,
        &mut writer,
        &_shard_manager,
        &registry,
        None, // Ping doesn't require auth manager
        None, // Ping doesn't require user_id
        &UnixRenderer,
    )
    .await
    .expect("Ping dispatch should succeed");

    // Close write end
    drop(writer);

    // Read response
    let mut response = Vec::new();
    reader.read_to_end(&mut response).await.expect("Should read response");

    let response_str = String::from_utf8(response).expect("Response should be valid UTF-8");
    assert!(
        response_str.contains("PONG") || response_str.contains("pong"),
        "Response should contain PONG, got: {}",
        response_str
    );
}

#[test]
fn test_parse_command_flush() {
    let input = "FLUSH";
    let command = parse_command(input).expect("Failed to parse FLUSH command");

    assert_eq!(command, Command::Flush);
}

#[test]
fn test_parse_command_invalid_token_should_fail() {
    let input = r#"STORE order_created FOR user-9 PAYLOAD { [ "bad" ] }"#;
    let result = parse_command(input);

    // PEG parser extracts the JSON block successfully, but JSON parsing fails
    assert!(
        matches!(result, Err(ParseError::InvalidJson(_))),
        "Expected InvalidJson error for invalid JSON syntax"
    );
}

#[test]
fn test_parse_command_define_with_enum_field() {
    let input = r#"DEFINE subscription FIELDS { "plan": ["pro", "basic"] }"#;
    let command = parse_command(input).expect("Failed to parse DEFINE with enum field");

    if let Command::Define {
        event_type, schema, ..
    } = command
    {
        assert_eq!(event_type, "subscription");
        assert_eq!(
            schema.fields["plan"],
            FieldSpec::Enum(vec!["pro".to_string(), "basic".to_string()])
        );
    } else {
        panic!("Expected Command::Define");
    }
}

#[test]
fn test_parse_command_define_with_invalid_enum_should_fail() {
    let input = r#"DEFINE subscription FIELDS { "plan": ["pro", 1] }"#;
    let result = parse_command(input);

    assert!(
        matches!(result, Err(ParseError::InvalidJson(_))),
        "Expected InvalidJson for non-string enum variant"
    );
}

#[test]
fn test_parse_command_define_with_nullable_field() {
    let input = r#"DEFINE user_created FIELDS { "email": "string | null" }"#;
    let command = parse_command(input).expect("Failed to parse DEFINE with nullable field");

    if let Command::Define {
        event_type, schema, ..
    } = command
    {
        assert_eq!(event_type, "user_created");
        assert_eq!(
            schema.fields["email"],
            FieldSpec::Primitive("string | null".to_string())
        );
    } else {
        panic!("Expected Command::Define");
    }
}

#[test]
fn test_parse_command_unknown_command_should_fail() {
    let input = "INVALIDCOMMAND something";
    let result = parse_command(input);

    assert!(
        matches!(result, Err(ParseError::UnknownCommand(_))),
        "Expected UnknownCommand error"
    );
}

#[test]
fn test_parse_command_query_invalid_syntax_should_fail() {
    let input = r#"QUERY orders WHERE"#;
    let result = parse_command(input);

    let err = result.expect_err("Expected error for incomplete WHERE clause");
    match err {
        ParseError::UnexpectedToken(msg) => {
            assert!(msg.contains("PEG parse error:"), "{}", msg);
            assert!(msg.contains("expected one of"), "{}", msg);
        }
        other => panic!("Expected UnexpectedToken, got {:?}", other),
    }
}

#[test]
fn test_parse_command_store_missing_payload_should_fail() {
    let input = r#"STORE order_created FOR user-9"#;
    let result = parse_command(input);

    // PEG parser fails when PAYLOAD keyword is missing, returns UnexpectedToken
    assert!(
        matches!(result, Err(ParseError::UnexpectedToken(_))),
        "Expected UnexpectedToken error for missing PAYLOAD keyword"
    );
}

/// Helper function to create test components
async fn create_test_components() -> (
    Arc<ShardManager>,
    Arc<RwLock<SchemaRegistry>>,
    Arc<AuthManager>,
    tempfile::TempDir,
) {
    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();
    let schema_dir = tempdir().unwrap().into_path();
    let schema_path = schema_dir.join("schemas.bin");
    let shard_manager = Arc::new(ShardManager::new(1, base_dir, wal_dir).await);
    let registry = Arc::new(RwLock::new(
        SchemaRegistry::new_with_path(schema_path).expect("Failed to initialize SchemaRegistry"),
    ));
    let auth_manager = Arc::new(AuthManager::new(Arc::clone(&shard_manager)));
    let temp_dir = tempdir().unwrap();
    (shard_manager, registry, auth_manager, temp_dir)
}

/// Helper to create an admin user for testing user management commands
async fn create_test_admin(auth_manager: &Arc<AuthManager>) -> String {
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
async fn test_dispatch_create_user_with_auth_manager() {
    init_for_tests();

    let (_shard_manager, registry, auth_manager, _temp_dir) = create_test_components().await;
    let cmd = Command::CreateUser {
        user_id: "test_user".to_string(),
        secret_key: None,
        roles: None,
    };

    // Create admin user for user management commands
    let _ = auth_manager
        .create_user_with_roles(
            "test_admin".to_string(),
            Some("admin_secret".to_string()),
            vec!["admin".to_string()],
        )
        .await
        .expect("Failed to create admin user");

    let (mut reader, mut writer) = duplex(1024);

    dispatch_command(
        &cmd,
        &mut writer,
        &_shard_manager,
        &registry,
        Some(&auth_manager),
        Some("test_admin"),
        &UnixRenderer,
    )
    .await
    .expect("dispatch should succeed");

    // Read response
    let mut response = vec![0u8; 1024];
    let n = reader.read(&mut response).await.unwrap();
    let msg = String::from_utf8_lossy(&response[..n]);

    // Verify response contains success message
    assert!(msg.contains("200 OK"));
    assert!(msg.contains("User 'test_user' created"));
    assert!(msg.contains("Secret key: "));
}

#[tokio::test]
async fn test_dispatch_create_user_without_auth_manager() {
    init_for_tests();

    let (_shard_manager, registry, _auth_manager, _temp_dir) = create_test_components().await;
    let cmd = Command::CreateUser {
        user_id: "test_user".to_string(),
        secret_key: None,
        roles: None,
    };

    let (mut reader, mut writer) = duplex(1024);

    dispatch_command(
        &cmd,
        &mut writer,
        &_shard_manager,
        &registry,
        None, // No auth manager
        None, // No user_id
        &UnixRenderer,
    )
    .await
    .expect("dispatch should succeed");

    // Read response
    let mut response = vec![0u8; 1024];
    let n = reader.read(&mut response).await.unwrap();
    let msg = String::from_utf8_lossy(&response[..n]);

    // Verify error response
    assert!(msg.contains("500"));
    assert!(msg.contains("Authentication not configured"));
}

#[tokio::test]
async fn test_dispatch_revoke_key_with_auth_manager() {
    init_for_tests();

    let (_shard_manager, registry, auth_manager, _temp_dir) = create_test_components().await;

    // Create admin user first
    let _ = auth_manager
        .create_user_with_roles(
            "test_admin".to_string(),
            Some("admin_secret".to_string()),
            vec!["admin".to_string()],
        )
        .await
        .expect("Failed to create admin user");

    // First create a user
    let create_cmd = Command::CreateUser {
        user_id: "user_to_revoke".to_string(),
        secret_key: None,
        roles: None,
    };
    let (_reader1, mut writer1) = duplex(1024);
    dispatch_command(
        &create_cmd,
        &mut writer1,
        &_shard_manager,
        &registry,
        Some(&auth_manager),
        Some("test_admin"),
        &UnixRenderer,
    )
    .await
    .expect("dispatch should succeed");

    // Then revoke the key
    let revoke_cmd = Command::RevokeKey {
        user_id: "user_to_revoke".to_string(),
    };
    let (mut reader2, mut writer2) = duplex(1024);

    dispatch_command(
        &revoke_cmd,
        &mut writer2,
        &_shard_manager,
        &registry,
        Some(&auth_manager),
        Some("test_admin"),
        &UnixRenderer,
    )
    .await
    .expect("dispatch should succeed");

    // Read response
    let mut response = vec![0u8; 1024];
    let n = reader2.read(&mut response).await.unwrap();
    let msg = String::from_utf8_lossy(&response[..n]);

    // Verify success response
    assert!(msg.contains("200 OK"));
    assert!(msg.contains("Key revoked for user 'user_to_revoke'"));
}

#[tokio::test]
async fn test_dispatch_revoke_key_without_auth_manager() {
    init_for_tests();

    let (_shard_manager, registry, _auth_manager, _temp_dir) = create_test_components().await;
    let cmd = Command::RevokeKey {
        user_id: "test_user".to_string(),
    };

    let (mut reader, mut writer) = duplex(1024);

    dispatch_command(
        &cmd,
        &mut writer,
        &_shard_manager,
        &registry,
        None, // No auth manager
        None, // No user_id
        &UnixRenderer,
    )
    .await
    .expect("dispatch should succeed");

    // Read response
    let mut response = vec![0u8; 1024];
    let n = reader.read(&mut response).await.unwrap();
    let msg = String::from_utf8_lossy(&response[..n]);

    // Verify error response
    assert!(msg.contains("500"));
    assert!(msg.contains("Authentication not configured"));
}

#[tokio::test]
async fn test_dispatch_list_users_with_auth_manager() {
    init_for_tests();

    let (_shard_manager, registry, auth_manager, _temp_dir) = create_test_components().await;

    // Create admin user first
    let _ = auth_manager
        .create_user_with_roles(
            "test_admin".to_string(),
            Some("admin_secret".to_string()),
            vec!["admin".to_string()],
        )
        .await
        .expect("Failed to create admin user");

    // Create a user first
    let create_cmd = Command::CreateUser {
        user_id: "list_test_user".to_string(),
        secret_key: None,
        roles: None,
    };
    let (_reader1, mut writer1) = duplex(1024);
    dispatch_command(
        &create_cmd,
        &mut writer1,
        &_shard_manager,
        &registry,
        Some(&auth_manager),
        Some("test_admin"),
        &UnixRenderer,
    )
    .await
    .expect("dispatch should succeed");

    // List users
    let list_cmd = Command::ListUsers;
    let (mut reader2, mut writer2) = duplex(1024);

    dispatch_command(
        &list_cmd,
        &mut writer2,
        &_shard_manager,
        &registry,
        Some(&auth_manager),
        Some("test_admin"),
        &UnixRenderer,
    )
    .await
    .expect("dispatch should succeed");

    // Read response
    let mut response = vec![0u8; 1024];
    let n = reader2.read(&mut response).await.unwrap();
    let msg = String::from_utf8_lossy(&response[..n]);

    // Verify response contains user
    assert!(msg.contains("200 OK"));
    assert!(msg.contains("list_test_user"));
}

#[tokio::test]
async fn test_dispatch_list_users_without_auth_manager() {
    init_for_tests();

    let (_shard_manager, registry, _auth_manager, _temp_dir) = create_test_components().await;
    let cmd = Command::ListUsers;

    let (mut reader, mut writer) = duplex(1024);

    dispatch_command(
        &cmd,
        &mut writer,
        &_shard_manager,
        &registry,
        None, // No auth manager
        None, // No user_id
        &UnixRenderer,
    )
    .await
    .expect("dispatch should succeed");

    // Read response
    let mut response = vec![0u8; 1024];
    let n = reader.read(&mut response).await.unwrap();
    let msg = String::from_utf8_lossy(&response[..n]);

    // Verify error response
    assert!(msg.contains("500"));
    assert!(msg.contains("Authentication not configured"));
}

#[tokio::test]
async fn test_dispatch_list_users_empty_with_auth_manager() {
    init_for_tests();

    let (_shard_manager, registry, auth_manager, _temp_dir) = create_test_components().await;
    let _ = create_test_admin(&auth_manager).await;

    let cmd = Command::ListUsers;

    let (mut reader, mut writer) = duplex(1024);

    dispatch_command(
        &cmd,
        &mut writer,
        &_shard_manager,
        &registry,
        Some(&auth_manager),
        Some("test_admin"),
        &UnixRenderer,
    )
    .await
    .expect("dispatch should succeed");

    // Read response
    let mut response = vec![0u8; 1024];
    let n = reader.read(&mut response).await.unwrap();
    let msg = String::from_utf8_lossy(&response[..n]);

    // Verify response shows admin user (since we created one)
    assert!(msg.contains("200 OK"));
    assert!(msg.contains("test_admin")); // Admin user should be listed
}

#[tokio::test]
async fn test_dispatch_create_user_error_user_exists() {
    init_for_tests();

    let (_shard_manager, registry, auth_manager, _temp_dir) = create_test_components().await;
    let _ = create_test_admin(&auth_manager).await;

    // Create user first
    let create_cmd1 = Command::CreateUser {
        user_id: "existing_user".to_string(),
        secret_key: None,
        roles: None,
    };
    let (_reader1, mut writer1) = duplex(1024);
    dispatch_command(
        &create_cmd1,
        &mut writer1,
        &_shard_manager,
        &registry,
        Some(&auth_manager),
        Some("test_admin"),
        &UnixRenderer,
    )
    .await
    .expect("dispatch should succeed");

    // Try to create same user again
    let create_cmd2 = Command::CreateUser {
        user_id: "existing_user".to_string(),
        secret_key: None,
        roles: None,
    };
    let (mut reader2, mut writer2) = duplex(1024);

    dispatch_command(
        &create_cmd2,
        &mut writer2,
        &_shard_manager,
        &registry,
        Some(&auth_manager),
        Some("test_admin"),
        &UnixRenderer,
    )
    .await
    .expect("dispatch should succeed");

    // Read response
    let mut response = vec![0u8; 1024];
    let n = reader2.read(&mut response).await.unwrap();
    let msg = String::from_utf8_lossy(&response[..n]);

    // Verify error response
    assert!(msg.contains("400"));
    // Note: Error message doesn't include user_id to prevent user enumeration
    assert!(msg.contains("User already exists"));
}

#[tokio::test]
async fn test_dispatch_revoke_key_error_user_not_found() {
    init_for_tests();

    let (_shard_manager, registry, auth_manager, _temp_dir) = create_test_components().await;
    let _ = create_test_admin(&auth_manager).await; // Create admin user first

    let cmd = Command::RevokeKey {
        user_id: "non_existent_user".to_string(),
    };

    let (mut reader, mut writer) = duplex(1024);

    dispatch_command(
        &cmd,
        &mut writer,
        &_shard_manager,
        &registry,
        Some(&auth_manager),
        Some("test_admin"),
        &UnixRenderer,
    )
    .await
    .expect("dispatch should succeed");

    // Read response
    let mut response = vec![0u8; 1024];
    let n = reader.read(&mut response).await.unwrap();
    let msg = String::from_utf8_lossy(&response[..n]);

    // Verify error response
    assert!(msg.contains("400"));
    assert!(msg.contains("User not found: non_existent_user"));
}

#[tokio::test]
async fn test_dispatch_create_user_with_secret_key() {
    init_for_tests();

    let (_shard_manager, registry, auth_manager, _temp_dir) = create_test_components().await;
    let _ = create_test_admin(&auth_manager).await;

    let cmd = Command::CreateUser {
        user_id: "user_with_key".to_string(),
        secret_key: Some("my_custom_secret_key".to_string()),
        roles: None,
    };

    let (mut reader, mut writer) = duplex(1024);

    dispatch_command(
        &cmd,
        &mut writer,
        &_shard_manager,
        &registry,
        Some(&auth_manager),
        Some("test_admin"),
        &UnixRenderer,
    )
    .await
    .expect("dispatch should succeed");

    // Read response
    let mut response = vec![0u8; 1024];
    let n = reader.read(&mut response).await.unwrap();
    let msg = String::from_utf8_lossy(&response[..n]);

    // Verify response contains provided secret key
    assert!(msg.contains("200 OK"));
    assert!(msg.contains("User 'user_with_key' created"));
    assert!(msg.contains("Secret key: my_custom_secret_key"));
}

#[tokio::test]
async fn test_dispatch_multiple_auth_commands_sequence() {
    init_for_tests();

    let (_shard_manager, registry, auth_manager, _temp_dir) = create_test_components().await;
    let _ = create_test_admin(&auth_manager).await;

    // Create user1
    let create_cmd1 = Command::CreateUser {
        user_id: "user1".to_string(),
        secret_key: None,
        roles: None,
    };
    let (_reader1, mut writer1) = duplex(1024);
    dispatch_command(
        &create_cmd1,
        &mut writer1,
        &_shard_manager,
        &registry,
        Some(&auth_manager),
        Some("test_admin"),
        &UnixRenderer,
    )
    .await
    .expect("dispatch should succeed");

    // Create user2
    let create_cmd2 = Command::CreateUser {
        user_id: "user2".to_string(),
        secret_key: None,
        roles: None,
    };
    let (_reader2, mut writer2) = duplex(1024);
    dispatch_command(
        &create_cmd2,
        &mut writer2,
        &_shard_manager,
        &registry,
        Some(&auth_manager),
        Some("test_admin"),
        &UnixRenderer,
    )
    .await
    .expect("dispatch should succeed");

    // List users
    let list_cmd = Command::ListUsers;
    let (mut reader3, mut writer3) = duplex(1024);
    dispatch_command(
        &list_cmd,
        &mut writer3,
        &_shard_manager,
        &registry,
        Some(&auth_manager),
        Some("test_admin"),
        &UnixRenderer,
    )
    .await
    .expect("dispatch should succeed");

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
    dispatch_command(
        &revoke_cmd,
        &mut writer4,
        &_shard_manager,
        &registry,
        Some(&auth_manager),
        Some("test_admin"),
        &UnixRenderer,
    )
    .await
    .expect("dispatch should succeed");

    // List users again
    let list_cmd2 = Command::ListUsers;
    let (mut reader5, mut writer5) = duplex(1024);
    dispatch_command(
        &list_cmd2,
        &mut writer5,
        &_shard_manager,
        &registry,
        Some(&auth_manager),
        Some("test_admin"),
        &UnixRenderer,
    )
    .await
    .expect("dispatch should succeed");

    let mut response2 = vec![0u8; 1024];
    let n2 = reader5.read(&mut response2).await.unwrap();
    let msg2 = String::from_utf8_lossy(&response2[..n2]);
    assert!(msg2.contains("user1"));
    assert!(msg2.contains("user2"));
    assert!(msg2.contains("inactive"));
    assert!(msg2.contains("active"));
}
