use crate::command::parser::commands::query;
use crate::command::parser::tokenizer::tokenize;
use crate::command::types::{Command, CompareOp, Expr};
use serde_json::json;

#[cfg(test)]
mod query_tests {
    use super::*;

    #[test]
    fn test_parse_query_minimal() {
        let input = r#"QUERY order_created"#;
        let tokens = tokenize(input);

        let command = query::parse(&tokens).expect("Failed to parse minimal QUERY");

        assert_eq!(
            command,
            Command::Query {
                event_type: "order_created".to_string(),
                context_id: None,
                since: None,
                where_clause: None,
                limit: None,
                return_fields: None,
            }
        );
    }

    #[test]
    fn test_parse_query_with_context_id() {
        let input = r#"QUERY order_created FOR user-123"#;
        let tokens = tokenize(input);

        let command = query::parse(&tokens).expect("Failed to parse QUERY with FOR");

        assert_eq!(
            command,
            Command::Query {
                event_type: "order_created".to_string(),
                context_id: Some("user-123".to_string()),
                since: None,
                where_clause: None,
                limit: None,
                return_fields: None,
            }
        );
    }

    #[test]
    fn test_parse_query_with_since() {
        let input = r#"QUERY order_created SINCE "2024-01-01T00:00:00Z""#;
        let tokens = tokenize(input);

        let command = query::parse(&tokens).expect("Failed to parse QUERY with SINCE");

        assert_eq!(
            command,
            Command::Query {
                event_type: "order_created".to_string(),
                context_id: None,
                since: Some("2024-01-01T00:00:00Z".to_string()),
                where_clause: None,
                limit: None,
                return_fields: None,
            }
        );
    }

    #[test]
    fn test_parse_query_with_limit() {
        let input = r#"QUERY order_created LIMIT 100"#;
        let tokens = tokenize(input);

        let command = query::parse(&tokens).expect("Failed to parse QUERY with LIMIT");

        assert_eq!(
            command,
            Command::Query {
                event_type: "order_created".to_string(),
                context_id: None,
                since: None,
                where_clause: None,
                limit: Some(100),
                return_fields: None,
            }
        );
    }

    #[test]
    fn test_parse_query_with_simple_where() {
        let input = r#"QUERY order_created WHERE status = "pending""#;
        let tokens = tokenize(input);

        let command = query::parse(&tokens).expect("Failed to parse QUERY with WHERE");

        assert_eq!(
            command,
            Command::Query {
                event_type: "order_created".to_string(),
                context_id: None,
                since: None,
                where_clause: Some(Expr::Compare {
                    field: "status".to_string(),
                    op: CompareOp::Eq,
                    value: json!("pending"),
                }),
                limit: None,
                return_fields: None,
            }
        );
    }

    #[test]
    fn test_parse_query_with_complex_where_and_or() {
        let input =
            r#"QUERY order_created WHERE status = "pending" OR amount > 100 AND retries < 3"#;
        let tokens = tokenize(input);

        let command = query::parse(&tokens).expect("Failed to parse QUERY with WHERE and OR/AND");

        assert_eq!(
            command,
            Command::Query {
                event_type: "order_created".to_string(),
                context_id: None,
                since: None,
                where_clause: Some(Expr::Or(
                    Box::new(Expr::Compare {
                        field: "status".to_string(),
                        op: CompareOp::Eq,
                        value: json!("pending"),
                    }),
                    Box::new(Expr::And(
                        Box::new(Expr::Compare {
                            field: "amount".to_string(),
                            op: CompareOp::Gt,
                            value: json!(100.0),
                        }),
                        Box::new(Expr::Compare {
                            field: "retries".to_string(),
                            op: CompareOp::Lt,
                            value: json!(3.0),
                        }),
                    )),
                )),
                limit: None,
                return_fields: None,
            }
        );
    }

    #[test]
    fn test_parse_query_with_not_operator() {
        let input = r#"QUERY order_created WHERE NOT status = "cancelled""#;
        let tokens = tokenize(input);

        let command = query::parse(&tokens).expect("Failed to parse QUERY with NOT");

        assert_eq!(
            command,
            Command::Query {
                event_type: "order_created".to_string(),
                context_id: None,
                since: None,
                where_clause: Some(Expr::Not(Box::new(Expr::Compare {
                    field: "status".to_string(),
                    op: CompareOp::Eq,
                    value: json!("cancelled"),
                }))),
                limit: None,
                return_fields: None,
            }
        );
    }

    #[test]
    fn test_parse_query_full() {
        let input = r#"QUERY order_created FOR user-456 SINCE "2024-01-01T00:00:00Z" WHERE amount >= 500 LIMIT 50"#;
        let tokens = tokenize(input);

        let command = query::parse(&tokens).expect("Failed to parse full QUERY");

        assert_eq!(
            command,
            Command::Query {
                event_type: "order_created".to_string(),
                context_id: Some("user-456".to_string()),
                since: Some("2024-01-01T00:00:00Z".to_string()),
                where_clause: Some(Expr::Compare {
                    field: "amount".to_string(),
                    op: CompareOp::Gte,
                    value: json!(500.0),
                }),
                limit: Some(50),
                return_fields: None,
            }
        );
    }

    #[test]
    fn test_parse_query_with_double_not() {
        let input = r#"QUERY order_created WHERE NOT NOT retries = 0"#;
        let tokens = tokenize(input);

        let command = query::parse(&tokens).expect("Failed to parse QUERY with double NOT");

        assert_eq!(
            command,
            Command::Query {
                event_type: "order_created".to_string(),
                context_id: None,
                since: None,
                where_clause: Some(Expr::Not(Box::new(Expr::Not(Box::new(Expr::Compare {
                    field: "retries".to_string(),
                    op: CompareOp::Eq,
                    value: json!(0.0),
                }))))),
                limit: None,
                return_fields: None,
            }
        );
    }

    #[test]
    fn test_parse_query_with_numeric_comparison() {
        let input = r#"QUERY product_price WHERE price >= 100"#;
        let tokens = tokenize(input);

        let command = query::parse(&tokens).expect("Failed to parse QUERY with numeric comparison");

        assert_eq!(
            command,
            Command::Query {
                event_type: "product_price".to_string(),
                context_id: None,
                since: None,
                where_clause: Some(Expr::Compare {
                    field: "price".to_string(),
                    op: CompareOp::Gte,
                    value: json!(100.0),
                }),
                limit: None,
                return_fields: None,
            }
        );
    }

    #[test]
    fn test_parse_query_with_extra_whitespace() {
        let input = r#"QUERY order_created WHERE    status   =   "pending"   "#;
        let tokens = tokenize(input);

        let command = query::parse(&tokens).expect("Failed to parse QUERY with extra spaces");

        assert_eq!(
            command,
            Command::Query {
                event_type: "order_created".to_string(),
                context_id: None,
                since: None,
                where_clause: Some(Expr::Compare {
                    field: "status".to_string(),
                    op: CompareOp::Eq,
                    value: json!("pending"),
                }),
                limit: None,
                return_fields: None,
            }
        );
    }

    #[test]
    fn test_parse_query_with_and_or_not() {
        let input = r#"QUERY session_data WHERE status = "active" OR NOT retries > 3"#;
        let tokens = tokenize(input);

        let command = query::parse(&tokens).expect("Failed to parse QUERY with AND/OR/NOT");

        assert_eq!(
            command,
            Command::Query {
                event_type: "session_data".to_string(),
                context_id: None,
                since: None,
                where_clause: Some(Expr::Or(
                    Box::new(Expr::Compare {
                        field: "status".to_string(),
                        op: CompareOp::Eq,
                        value: json!("active"),
                    }),
                    Box::new(Expr::Not(Box::new(Expr::Compare {
                        field: "retries".to_string(),
                        op: CompareOp::Gt,
                        value: json!(3.0),
                    })))
                )),
                limit: None,
                return_fields: None,
            }
        );
    }

    #[test]
    fn test_parse_query_with_trailing_garbage_after_for_should_fail() {
        let input = r#"QUERY order_created FOR user-123 garbage"#;
        let tokens = tokenize(input);

        let result = query::parse(&tokens);

        assert!(
            result.is_err(),
            "Expected failure due to trailing garbage after FOR in QUERY"
        );
    }

    #[test]
    fn test_parse_query_with_trailing_garbage_after_where_should_fail() {
        let input = r#"QUERY order_created WHERE status = "pending" garbage"#;
        let tokens = tokenize(input);

        let result = query::parse(&tokens);

        assert!(
            result.is_err(),
            "Expected failure due to trailing garbage after WHERE in QUERY"
        );
    }

    #[test]
    fn test_parse_query_with_return_ignored() {
        let input = r#"QUERY order_created RETURN [context_id, event_type, "timestamp", payload] WHERE status = "pending" LIMIT 10"#;
        let tokens = tokenize(input);

        let command = query::parse(&tokens).expect("Failed to parse QUERY with RETURN");

        assert_eq!(
            command,
            Command::Query {
                event_type: "order_created".to_string(),
                context_id: None,
                since: None,
                where_clause: Some(Expr::Compare {
                    field: "status".to_string(),
                    op: CompareOp::Eq,
                    value: json!("pending"),
                }),
                limit: Some(10),
                return_fields: None,
            }
        );
    }

    #[test]
    fn test_parse_query_with_return_empty_list() {
        let input = r#"QUERY order_created RETURN [] WHERE status = "pending""#;
        let tokens = tokenize(input);

        let command = query::parse(&tokens).expect("Failed to parse QUERY with empty RETURN");

        assert_eq!(
            command,
            Command::Query {
                event_type: "order_created".to_string(),
                context_id: None,
                since: None,
                where_clause: Some(Expr::Compare {
                    field: "status".to_string(),
                    op: CompareOp::Eq,
                    value: json!("pending"),
                }),
                limit: None,
                return_fields: None,
            }
        );
    }

    #[test]
    fn test_parse_query_with_return_and_since() {
        let input =
            r#"QUERY order_created SINCE "2024-01-01T00:00:00Z" RETURN ["plan", country] LIMIT 5"#;
        let tokens = tokenize(input);

        let command = query::parse(&tokens).expect("Failed to parse QUERY with SINCE and RETURN");

        assert_eq!(
            command,
            Command::Query {
                event_type: "order_created".to_string(),
                context_id: None,
                since: Some("2024-01-01T00:00:00Z".to_string()),
                where_clause: None,
                limit: Some(5),
                return_fields: None,
            }
        );
    }
}
