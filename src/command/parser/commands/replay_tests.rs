use crate::command::parser::commands::replay;
use crate::command::parser::tokenizer::tokenize;
use crate::command::types::Command;

#[cfg(test)]
mod replay_tests {
    use super::*;

    #[test]
    fn test_parse_replay_minimal() {
        let input = r#"REPLAY FOR user-123"#;
        let tokens = tokenize(input);

        let command = replay::parse(&tokens).expect("Failed to parse REPLAY command");

        assert_eq!(
            command,
            Command::Replay {
                event_type: None,
                context_id: "user-123".to_string(),
                since: None,
                return_fields: None,
            }
        );
    }

    #[test]
    fn test_parse_replay_with_event_type() {
        let input = r#"REPLAY order_created FOR user-123"#;
        let tokens = tokenize(input);

        let command = replay::parse(&tokens).expect("Failed to parse REPLAY with event_type");

        assert_eq!(
            command,
            Command::Replay {
                event_type: Some("order_created".to_string()),
                context_id: "user-123".to_string(),
                since: None,
                return_fields: None,
            }
        );
    }

    #[test]
    fn test_parse_replay_with_since() {
        let input = r#"REPLAY FOR user-123 SINCE "2024-01-01T00:00:00Z""#;
        let tokens = tokenize(input);

        let command = replay::parse(&tokens).expect("Failed to parse REPLAY with SINCE");

        assert_eq!(
            command,
            Command::Replay {
                event_type: None,
                context_id: "user-123".to_string(),
                since: Some("2024-01-01T00:00:00Z".to_string()),
                return_fields: None,
            }
        );
    }

    #[test]
    fn test_parse_replay_with_event_type_and_since() {
        let input = r#"REPLAY order_created FOR user-123 SINCE "2024-01-01T00:00:00Z""#;
        let tokens = tokenize(input);

        let command =
            replay::parse(&tokens).expect("Failed to parse REPLAY with event_type and SINCE");

        assert_eq!(
            command,
            Command::Replay {
                event_type: Some("order_created".to_string()),
                context_id: "user-123".to_string(),
                since: Some("2024-01-01T00:00:00Z".to_string()),
                return_fields: None,
            }
        );
    }

    #[test]
    fn test_parse_replay_missing_for_should_fail() {
        let input = r#"REPLAY order_created"#; // Missing "FOR context_id"
        let tokens = tokenize(input);

        let result = replay::parse(&tokens);

        assert!(result.is_err());
    }

    #[test]
    fn test_parse_replay_missing_context_id_should_fail() {
        let input = r#"REPLAY order_created FOR"#; // Missing actual context_id
        let tokens = tokenize(input);

        let result = replay::parse(&tokens);

        assert!(result.is_err());
    }

    #[test]
    fn test_parse_replay_with_trailing_garbage_should_fail() {
        let input = r#"REPLAY order_created FOR user-123 garbage"#;
        let tokens = tokenize(input);
        let result = replay::parse(&tokens);
        assert!(result.is_err(), "Expected failure due to trailing garbage");
    }

    #[test]
    fn test_parse_replay_with_forgarbled_keyword_should_fail() {
        let input = r#"REPLAY order_created FORGARBAGE"#;
        let tokens = tokenize(input);
        let result = replay::parse(&tokens);
        assert!(
            result.is_err(),
            "Expected failure due to missing space after FOR"
        );
    }

    #[test]
    fn test_parse_replay_with_return_ignored() {
        let input = r#"REPLAY order_created FOR user-123 RETURN [context_id, event_type, "timestamp", payload]"#;
        let tokens = tokenize(input);

        let command = replay::parse(&tokens).expect("Failed to parse REPLAY with RETURN");

        assert_eq!(
            command,
            Command::Replay {
                event_type: Some("order_created".to_string()),
                context_id: "user-123".to_string(),
                since: None,
                return_fields: None,
            }
        );
    }

    #[test]
    fn test_parse_replay_with_return_and_since() {
        let input = r#"REPLAY FOR user-123 SINCE "2024-01-01T00:00:00Z" RETURN ["plan", country]"#;
        let tokens = tokenize(input);

        let command = replay::parse(&tokens).expect("Failed to parse REPLAY with SINCE and RETURN");

        assert_eq!(
            command,
            Command::Replay {
                event_type: None,
                context_id: "user-123".to_string(),
                since: Some("2024-01-01T00:00:00Z".to_string()),
                return_fields: None,
            }
        );
    }

    #[test]
    fn test_parse_replay_with_return_empty_list() {
        let input = r#"REPLAY FOR user-123 RETURN []"#;
        let tokens = tokenize(input);

        let command = replay::parse(&tokens).expect("Failed to parse REPLAY with empty RETURN");

        assert_eq!(
            command,
            Command::Replay {
                event_type: None,
                context_id: "user-123".to_string(),
                since: None,
                return_fields: None,
            }
        );
    }
}
