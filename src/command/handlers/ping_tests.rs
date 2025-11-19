use crate::command::handlers::ping::handle;
use crate::command::types::Command;
use crate::shared::response::{JsonRenderer, UnixRenderer};
use std::io::Cursor;
use tokio::io::AsyncReadExt;

#[tokio::test]
async fn test_ping_returns_pong() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let cmd = Command::Ping;
    let mut writer = Vec::new();

    handle(&cmd, &mut writer, &JsonRenderer)
        .await
        .expect("ping handler should not fail");

    let response = String::from_utf8(writer).expect("response should be valid UTF-8");
    assert!(
        response.contains("PONG") || response.contains("pong"),
        "Response should contain PONG, got: {}",
        response
    );
}

#[tokio::test]
async fn test_ping_with_json_renderer() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let cmd = Command::Ping;
    let mut writer = Vec::new();

    handle(&cmd, &mut writer, &JsonRenderer)
        .await
        .expect("ping handler should not fail");

    let response = String::from_utf8(writer).expect("response should be valid UTF-8");

    // JSON renderer should produce JSON output
    assert!(
        response.contains("PONG") || response.contains("pong"),
        "Response should contain PONG, got: {}",
        response
    );
}

#[tokio::test]
async fn test_ping_with_unix_renderer() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let cmd = Command::Ping;
    let mut writer = Vec::new();

    handle(&cmd, &mut writer, &UnixRenderer)
        .await
        .expect("ping handler should not fail");

    let response = String::from_utf8(writer).expect("response should be valid UTF-8");

    // Unix renderer should produce plain text output
    assert!(
        response.contains("PONG") || response.contains("pong"),
        "Response should contain PONG, got: {}",
        response
    );
}

#[tokio::test]
async fn test_ping_multiple_times() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let cmd = Command::Ping;
    let mut writer1 = Vec::new();
    let mut writer2 = Vec::new();
    let mut writer3 = Vec::new();

    // First ping
    handle(&cmd, &mut writer1, &JsonRenderer)
        .await
        .expect("first ping should succeed");

    // Second ping
    handle(&cmd, &mut writer2, &JsonRenderer)
        .await
        .expect("second ping should succeed");

    // Third ping
    handle(&cmd, &mut writer3, &JsonRenderer)
        .await
        .expect("third ping should succeed");

    let response1 = String::from_utf8(writer1).expect("response should be valid UTF-8");
    let response2 = String::from_utf8(writer2).expect("response should be valid UTF-8");
    let response3 = String::from_utf8(writer3).expect("response should be valid UTF-8");

    assert!(
        response1.contains("PONG") || response1.contains("pong"),
        "First response should contain PONG"
    );
    assert!(
        response2.contains("PONG") || response2.contains("pong"),
        "Second response should contain PONG"
    );
    assert!(
        response3.contains("PONG") || response3.contains("pong"),
        "Third response should contain PONG"
    );
}

#[tokio::test]
async fn test_ping_with_cursor_writer() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let cmd = Command::Ping;
    let mut writer = Cursor::new(Vec::new());

    handle(&cmd, &mut writer, &JsonRenderer)
        .await
        .expect("ping handler should not fail");

    let mut buffer = Vec::new();
    writer.set_position(0);
    writer
        .read_to_end(&mut buffer)
        .await
        .expect("should read response");

    let response = String::from_utf8(buffer).expect("response should be valid UTF-8");
    assert!(
        response.contains("PONG") || response.contains("pong"),
        "Response should contain PONG, got: {}",
        response
    );
}

#[tokio::test]
async fn test_ping_ignores_command_content() {
    use crate::logging::init_for_tests;
    init_for_tests();

    // Even though we pass a Ping command, the handler doesn't use its content
    // This test verifies that the handler works regardless of command details
    let cmd = Command::Ping;
    let mut writer = Vec::new();

    handle(&cmd, &mut writer, &JsonRenderer)
        .await
        .expect("ping handler should not fail");

    let response = String::from_utf8(writer).expect("response should be valid UTF-8");
    assert!(
        response.contains("PONG") || response.contains("pong"),
        "Response should contain PONG"
    );
}

#[tokio::test]
async fn test_ping_flushes_writer() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let cmd = Command::Ping;
    let mut writer = Vec::new();

    handle(&cmd, &mut writer, &JsonRenderer)
        .await
        .expect("ping handler should not fail");

    // After flush, writer should have content
    assert!(!writer.is_empty(), "Writer should have content after flush");
}

#[tokio::test]
async fn test_ping_returns_ok_status() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let cmd = Command::Ping;
    let mut writer = Vec::new();

    let result = handle(&cmd, &mut writer, &JsonRenderer).await;

    assert!(result.is_ok(), "Ping handler should return Ok(())");
}

#[tokio::test]
async fn test_ping_with_different_renderers_produce_different_formats() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let cmd = Command::Ping;

    // Test JSON renderer
    let mut json_writer = Vec::new();
    handle(&cmd, &mut json_writer, &JsonRenderer)
        .await
        .expect("ping with JSON renderer should succeed");
    let json_response = String::from_utf8(json_writer).expect("response should be valid UTF-8");

    // Test Unix renderer
    let mut unix_writer = Vec::new();
    handle(&cmd, &mut unix_writer, &UnixRenderer)
        .await
        .expect("ping with Unix renderer should succeed");
    let unix_response = String::from_utf8(unix_writer).expect("response should be valid UTF-8");

    // Both should contain PONG but in different formats
    assert!(
        json_response.contains("PONG") || json_response.contains("pong"),
        "JSON response should contain PONG"
    );
    assert!(
        unix_response.contains("PONG") || unix_response.contains("pong"),
        "Unix response should contain PONG"
    );

    // Formats should be different (JSON vs plain text)
    assert_ne!(
        json_response, unix_response,
        "JSON and Unix renderers should produce different output formats"
    );
}

#[tokio::test]
async fn test_ping_is_idempotent() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let cmd = Command::Ping;
    let mut writer1 = Vec::new();
    let mut writer2 = Vec::new();

    // First call
    handle(&cmd, &mut writer1, &JsonRenderer)
        .await
        .expect("first ping should succeed");

    // Second call with same command
    handle(&cmd, &mut writer2, &JsonRenderer)
        .await
        .expect("second ping should succeed");

    let response1 = String::from_utf8(writer1).expect("response should be valid UTF-8");
    let response2 = String::from_utf8(writer2).expect("response should be valid UTF-8");

    // Both responses should be identical (idempotent)
    assert_eq!(
        response1, response2,
        "Ping should be idempotent and produce same output"
    );
}

#[tokio::test]
async fn test_ping_handles_empty_writer() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let cmd = Command::Ping;
    let mut writer = Vec::new();

    handle(&cmd, &mut writer, &JsonRenderer)
        .await
        .expect("ping handler should handle empty writer");

    // Writer should now have content
    assert!(!writer.is_empty(), "Writer should have content after ping");
}

#[tokio::test]
async fn test_ping_with_large_buffer() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let cmd = Command::Ping;
    // Pre-allocate a large buffer
    let mut writer = Vec::with_capacity(1024 * 1024);

    handle(&cmd, &mut writer, &JsonRenderer)
        .await
        .expect("ping handler should handle large buffer");

    let response = String::from_utf8(writer).expect("response should be valid UTF-8");
    assert!(
        response.contains("PONG") || response.contains("pong"),
        "Response should contain PONG even with large buffer"
    );
}
