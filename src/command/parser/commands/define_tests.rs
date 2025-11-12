use crate::command::parser::commands::define;
use crate::command::parser::error::ParseError;
use crate::command::parser::tokenizer::tokenize;
use crate::command::types::{Command, FieldSpec, MiniSchema};

#[cfg(test)]
mod define_tests {
    use super::*;

    #[test]
    fn test_parse_define_simple() {
        let input = r#"DEFINE order_created FIELDS { "id": "int", "status": "string" }"#;
        let tokens = tokenize(input);

        let command = define::parse(&tokens).expect("Failed to parse DEFINE command");

        assert_eq!(
            command,
            Command::Define {
                event_type: "order_created".to_string(),
                version: None,
                schema: MiniSchema {
                    fields: {
                        let mut map = std::collections::HashMap::new();
                        map.insert("id".to_string(), FieldSpec::Primitive("int".to_string()));
                        map.insert(
                            "status".to_string(),
                            FieldSpec::Primitive("string".to_string()),
                        );
                        map
                    },
                }
            }
        );
    }

    #[test]
    fn test_parse_define_with_nullable_fields() {
        let input = r#"DEFINE user_created FIELDS { "id": "int", "email": "string | null" }"#;
        let tokens = tokenize(input);

        let command = define::parse(&tokens).expect("Failed to parse DEFINE with nullable fields");

        assert_eq!(
            command,
            Command::Define {
                event_type: "user_created".to_string(),
                version: None,
                schema: MiniSchema {
                    fields: {
                        let mut map = std::collections::HashMap::new();
                        map.insert("id".to_string(), FieldSpec::Primitive("int".to_string()));
                        map.insert(
                            "email".to_string(),
                            FieldSpec::Primitive("string | null".to_string()),
                        );
                        map
                    },
                }
            }
        );
    }

    #[test]
    fn test_parse_define_with_version() {
        let input = r#"DEFINE invoice_created AS 3 FIELDS { "invoice_id": "int" }"#;
        let tokens = tokenize(input);

        let command = define::parse(&tokens).expect("Failed to parse DEFINE with version");

        assert_eq!(
            command,
            Command::Define {
                event_type: "invoice_created".to_string(),
                version: Some(3),
                schema: MiniSchema {
                    fields: {
                        let mut map = std::collections::HashMap::new();
                        map.insert(
                            "invoice_id".to_string(),
                            FieldSpec::Primitive("int".to_string()),
                        );
                        map
                    },
                }
            }
        );
    }

    #[test]
    fn test_parse_define_invalid_event_type_should_fail() {
        let input = r#"DEFINE 9invalid FIELDS { "id": "int" }"#;
        let tokens = tokenize(input);

        let result = define::parse(&tokens);

        assert!(matches!(result, Err(ParseError::UnexpectedToken(_))));
    }

    #[test]
    fn test_parse_define_missing_fields_should_fail() {
        let input = r#"DEFINE order_created"#;
        let tokens = tokenize(input);

        let result = define::parse(&tokens);

        assert!(result.is_err());
    }

    #[test]
    fn test_parse_define_invalid_json_in_fields_should_fail() {
        let input = r#"DEFINE order_created FIELDS { "id": "int" "status": "string" }"#; // missing comma
        let tokens = tokenize(input);

        let result = define::parse(&tokens);

        assert!(matches!(result, Err(ParseError::InvalidJson(_))));
    }

    #[test]
    fn test_parse_define_with_empty_fields_should_fail() {
        let input = r#"DEFINE empty_event FIELDS { }"#;
        let tokens = tokenize(input);

        let result = define::parse(&tokens);

        assert!(
            result.is_err(),
            "Expected failure for DEFINE with empty fields"
        );
    }

    #[test]
    fn test_parse_define_with_invalid_json_fields_should_fail() {
        let input = r#"DEFINE empty_event FIELDS { "invalid_json" }"#;
        let tokens = tokenize(input);

        let result = define::parse(&tokens);

        assert!(
            matches!(result, Err(ParseError::InvalidJson(_))),
            "Expected failure due to invalid JSON inside FIELDS block"
        );
    }

    #[test]
    fn test_parse_define_with_trailing_garbage_should_fail() {
        let input = r#"DEFINE event_name FIELDS { "id": "int" } unexpected"#;
        let tokens = tokenize(input);

        let result = define::parse(&tokens);

        assert!(
            result.is_err(),
            "Expected failure due to trailing tokens after FIELDS block"
        );
    }

    #[test]
    fn test_parse_define_with_enum_field() {
        let input = r#"DEFINE subscription FIELDS { "plan": ["pro", "basic"] }"#;
        let tokens = tokenize(input);

        let command = define::parse(&tokens).expect("Failed to parse DEFINE with enum field");

        assert_eq!(
            command,
            Command::Define {
                event_type: "subscription".to_string(),
                version: None,
                schema: MiniSchema {
                    fields: {
                        let mut map = std::collections::HashMap::new();
                        map.insert(
                            "plan".to_string(),
                            FieldSpec::Enum(vec!["pro".to_string(), "basic".to_string()]),
                        );
                        map
                    },
                },
            }
        );
    }

    #[test]
    fn test_parse_define_with_invalid_enum_variants_should_fail() {
        // Non-string in the array should fail
        let input = r#"DEFINE subscription FIELDS { "plan": ["pro", 123] }"#;
        let tokens = tokenize(input);

        let result = define::parse(&tokens);

        assert!(matches!(result, Err(ParseError::InvalidJson(_))));
    }
}
