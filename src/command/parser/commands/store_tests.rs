use crate::command::parser::commands::store;
use crate::command::parser::error::ParseError;
use crate::command::types::Command;
use serde_json::json;

#[cfg(test)]
mod store_tests {
    use super::*;

    #[test]
    fn test_parse_store_simple() {
        let input = r#"STORE order_created FOR user-9 PAYLOAD { "id": 9, "status": "pending" }"#;

        let command = store::parse_peg(input).expect("Failed to parse STORE command");

        assert_eq!(
            command,
            Command::Store {
                event_type: "order_created".to_string(),
                context_id: "user-9".to_string(),
                payload: json!({
                    "id": 9,
                    "status": "pending"
                }),
            }
        );
    }

    #[test]
    fn test_parse_store_two_glued_should_fail() {
        let input = r#"STORE order_created FOR user-9 PAYLOAD { "id": 9, "status": "pending" } STORE order_created FOR user-10 PAYLOAD { "id": 10, "status": "shipped" }"#;

        let result = store::parse_peg(input);

        assert!(
            result.is_err(),
            "Expected failure when two STORE commands are glued without separator"
        );
    }

    #[test]
    fn test_parse_store_with_quoted_context_id() {
        let input = r#"STORE order_created FOR "user 9" PAYLOAD { "id": 9, "status": "pending" }"#;

        let command = store::parse_peg(input).expect("Failed to parse STORE command");

        assert_eq!(
            command,
            Command::Store {
                event_type: "order_created".to_string(),
                context_id: "user 9".to_string(),
                payload: json!({
                    "id": 9,
                    "status": "pending"
                }),
            }
        );
    }

    #[test]
    fn test_parse_store_nested_payload() {
        // PEG parser allows nested JSON (unlike token-based parser)
        let input =
            r#"STORE order_created FOR user-9 PAYLOAD { "details": { "product": "book" } }"#;

        let command =
            store::parse_peg(input).expect("Failed to parse STORE command with nested JSON");

        assert_eq!(
            command,
            Command::Store {
                event_type: "order_created".to_string(),
                context_id: "user-9".to_string(),
                payload: json!({
                    "details": {
                        "product": "book"
                    }
                }),
            }
        );
    }

    #[test]
    fn test_parse_store_missing_payload_keyword() {
        let input = r#"STORE order_created FOR user-9 { "id": 9, "status": "pending" }"#;

        let result = store::parse_peg(input);

        assert!(result.is_err());
    }

    #[test]
    fn test_parse_store_missing_json_brace() {
        let input = r#"STORE order_created FOR user-9 PAYLOAD "id": 9, "status": "pending""#;

        let result = store::parse_peg(input);

        assert!(result.is_err());
    }

    #[test]
    fn test_parse_store_with_extra_tokens_after_payload() {
        let input = r#"STORE order_created FOR user-9 PAYLOAD { "id": 9 } extra_garbage"#;
        let result = store::parse_peg(input);
        assert!(
            result.is_err(),
            "Expected failure when extra tokens are present after payload"
        );
    }

    #[test]
    fn test_parse_store_empty_payload() {
        let input = r#"STORE empty_event FOR user-1 PAYLOAD { }"#;

        let command = store::parse_peg(input).expect("Failed to parse STORE command");

        assert_eq!(
            command,
            Command::Store {
                event_type: "empty_event".to_string(),
                context_id: "user-1".to_string(),
                payload: json!({}),
            }
        );
    }

    #[test]
    fn test_parse_store_invalid_json_payload_should_fail() {
        let input = r#"STORE broken_event FOR user-9 PAYLOAD { "id": 9 "status": "pending" }"#; // missing comma

        let result = store::parse_peg(input);

        assert!(matches!(result, Err(ParseError::InvalidJson(_))));
    }
}
