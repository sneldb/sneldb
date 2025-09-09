use crate::command::parser::command::parse_command;
use crate::command::types::Command;
use serde_json::json;

#[test]
fn test_parse_batch_with_two_store_commands() {
    let input = r#"
        BATCH [
            STORE order_created FOR user-1 PAYLOAD { "id": 1, "status": "pending" };
            STORE order_created FOR user-2 PAYLOAD { "id": 2, "status": "shipped" };
        ]
    "#;

    let command = parse_command(input).expect("Failed to parse BATCH command");

    if let Command::Batch(commands) = command {
        assert_eq!(commands.len(), 2);

        assert_eq!(
            commands[0],
            Command::Store {
                event_type: "order_created".to_string(),
                context_id: "user-1".to_string(),
                payload: json!({ "id": 1, "status": "pending" }),
            }
        );

        assert_eq!(
            commands[1],
            Command::Store {
                event_type: "order_created".to_string(),
                context_id: "user-2".to_string(),
                payload: json!({ "id": 2, "status": "shipped" }),
            }
        );
    } else {
        panic!("Expected Command::Batch");
    }
}

#[test]
fn test_parse_batch_with_mixed_commands() {
    let input = r#"
        BATCH [
            PING;
            FLUSH;
        ]
    "#;

    let command = parse_command(input).expect("Failed to parse BATCH with mixed commands");

    if let Command::Batch(commands) = command {
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0], Command::Ping);
        assert_eq!(commands[1], Command::Flush);
    } else {
        panic!("Expected Command::Batch");
    }
}

#[test]
fn test_parse_empty_batch_should_fail() {
    let input = r#"
        BATCH [ ]
    "#;

    let result = parse_command(input);

    assert!(result.is_err(), "Expected failure for empty BATCH");
}

#[test]
fn test_parse_batch_missing_closing_bracket_should_fail() {
    let input = r#"
        BATCH [
            STORE order_created FOR user-1 PAYLOAD { "id": 1, "status": "pending" };
    "#;

    let result = parse_command(input);

    assert!(
        result.is_err(),
        "Expected failure for BATCH missing closing bracket"
    );
}

#[test]
fn test_parse_batch_with_invalid_command_inside() {
    let input = r#"
        BATCH [
            INVALID_COMMAND something;
        ]
    "#;

    let result = parse_command(input);

    assert!(
        result.is_err(),
        "Expected failure due to invalid command inside BATCH"
    );
}

#[test]
fn test_parse_batch_with_extra_semicolons() {
    let input = r#"
        BATCH [
            PING;;
            FLUSH;;
        ]
    "#;

    let command = parse_command(input).expect("Failed to parse BATCH with extra semicolons");

    if let Command::Batch(commands) = command {
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0], Command::Ping);
        assert_eq!(commands[1], Command::Flush);
    } else {
        panic!("Expected Command::Batch");
    }
}

#[test]
fn test_parse_batch_with_missing_semicolon_should_fail() {
    let input = r#"
        BATCH [
            STORE order_created FOR user-1 PAYLOAD { "id": 1, "status": "pending" }
            STORE order_created FOR user-1 PAYLOAD { "id": 1, "status": "pending" };
        ]
    "#;

    let result = parse_command(input);

    assert!(
        result.is_err(),
        "Expected failure due to missing semicolon between PING and FLUSH"
    );
}

#[test]
fn test_parse_batch_single_command_only() {
    let input = r#"
        BATCH [
            PING;
        ]
    "#;

    let command = parse_command(input).expect("Failed to parse BATCH with single command");

    if let Command::Batch(commands) = command {
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0], Command::Ping);
    } else {
        panic!("Expected Command::Batch");
    }
}
