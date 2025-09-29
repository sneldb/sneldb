use crate::command::parser::commands::query;
use crate::command::parser::tokenizer::tokenize;
use crate::command::types::{AggSpec, Command, CompareOp, Expr, TimeGranularity};
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
                link_field: None,
                aggs: None,
                time_bucket: None,
                group_by: None,
                event_sequence: None,
            }
        );
    }

    #[test]
    fn test_parse_query_with_quoted_context_id() {
        let input = r#"QUERY orders FOR "user 123""#;
        let tokens = tokenize(input);

        let command = query::parse(&tokens).expect("Failed to parse QUERY with quoted context_id");

        if let Command::Query { context_id, .. } = command {
            assert_eq!(context_id, Some("user 123".to_string()));
        } else {
            panic!("Expected Query");
        }
    }

    #[test]
    fn test_parse_find_sequence_via_alias() {
        let input = r#"FIND orders FOLLOWED BY payment_succeeded"#;
        let mut tokens = tokenize(input);
        if let Some(crate::command::parser::tokenizer::Token::Word(w)) = tokens.first_mut() {
            *w = "QUERY".to_string();
        }

        let command = query::parse(&tokens).expect("Failed to parse FIND sequence after aliasing");
        if let Command::Query {
            event_sequence: Some(seq),
            ..
        } = command
        {
            assert_eq!(seq.head.event, "orders".to_string());
            assert_eq!(seq.links.len(), 1);
            assert_eq!(seq.links[0].1.event, "payment_succeeded".to_string());
        } else {
            panic!("Expected Query with event_sequence");
        }
    }

    #[test]
    fn test_parse_query_linked_by_dotted_field_should_fail() {
        let input = r#"QUERY orders LINKED BY account.id"#;
        let tokens = tokenize(input);
        let result = query::parse(&tokens);
        assert!(result.is_err(), "Expected error for dotted LINKED BY field");
    }

    #[test]
    fn test_parse_find_full_combo_via_alias() {
        let input = r#"FIND orders FOLLOWED BY payment_succeeded LINKED BY account_id WHERE status = "ok" COUNT PER hour BY country LIMIT 5"#;
        let mut tokens = tokenize(input);
        if let Some(crate::command::parser::tokenizer::Token::Word(w)) = tokens.first_mut() {
            *w = "QUERY".to_string();
        }

        let command = query::parse(&tokens).expect("Failed to parse full FIND combo via alias");
        if let Command::Query {
            event_sequence: Some(seq),
            link_field,
            where_clause,
            aggs: Some(aggs),
            time_bucket: Some(tb),
            group_by: Some(g),
            limit: Some(lim),
            ..
        } = command
        {
            assert_eq!(seq.head.event, "orders");
            assert_eq!(seq.links[0].1.event, "payment_succeeded");
            assert_eq!(link_field.as_deref(), Some("account_id"));
            assert!(matches!(tb, TimeGranularity::Hour));
            assert_eq!(g, vec!["country".to_string()]);
            assert_eq!(lim, 5);
            assert!(where_clause.is_some());
            assert!(aggs.iter().any(|a| matches!(a, AggSpec::Count { .. })));
        } else {
            panic!("Expected Query with all fields set");
        }
    }

    #[test]
    fn test_parse_query_per_hour() {
        let input = r#"QUERY orders COUNT PER hour"#;
        let tokens = tokenize(input);
        let command = query::parse(&tokens).expect("Failed to parse PER hour");
        if let Command::Query { time_bucket, .. } = command {
            assert!(matches!(time_bucket, Some(TimeGranularity::Hour)));
        } else {
            panic!("Expected Query");
        }
    }

    #[test]
    fn test_parse_event_sequence_followed_by() {
        let input = r#"QUERY order_created FOLLOWED BY payment_succeeded"#;
        let tokens = tokenize(input);

        let command = query::parse(&tokens).expect("Failed to parse event sequence FOLLOWED BY");

        if let Command::Query {
            event_type,
            event_sequence: Some(seq),
            ..
        } = command
        {
            assert_eq!(event_type, "order_created".to_string());
            assert_eq!(seq.head.event, "order_created".to_string());
            assert_eq!(seq.head.field, None);
            assert_eq!(seq.links.len(), 1);
            assert_eq!(seq.links[0].1.event, "payment_succeeded".to_string());
        } else {
            panic!("Expected Query with event_sequence");
        }
    }

    #[test]
    fn test_parse_event_sequence_preceded_by_with_dotted_targets_should_fail() {
        let input = r#"QUERY order_created.status PRECEDED BY payment_succeeded.kind"#;
        let tokens = tokenize(input);
        let result = query::parse(&tokens);
        assert!(
            result.is_err(),
            "Expected error for qualified EventType in EventSeq"
        );
    }

    #[test]
    fn test_parse_query_aggs_trailing_comma_allowed() {
        // Current parser tolerates trailing comma by stopping at EOF
        let input = r#"QUERY orders COUNT,"#;
        let tokens = tokenize(input);
        let command = query::parse(&tokens).expect("Failed to parse COUNT with trailing comma");
        if let Command::Query { aggs, .. } = command {
            let a = aggs.expect("aggs missing");
            assert!(matches!(a[0], AggSpec::Count { unique_field: None }));
        } else {
            panic!("Expected Query");
        }
    }

    #[test]
    fn test_parse_query_per_case_insensitive() {
        let input = r#"QUERY orders COUNT PER WEEK"#;
        let tokens = tokenize(input);
        let command = query::parse(&tokens).expect("Failed to parse PER WEEK");
        if let Command::Query { time_bucket, .. } = command {
            assert!(matches!(time_bucket, Some(TimeGranularity::Week)));
        } else {
            panic!("Expected Query");
        }
    }

    #[test]
    fn test_parse_query_group_by_dotted_fields() {
        let input = r#"QUERY orders COUNT BY order.type, region.name"#;
        let tokens = tokenize(input);
        let command = query::parse(&tokens).expect("Failed to parse dotted group fields");
        if let Command::Query { group_by, .. } = command {
            let g = group_by.expect("group_by missing");
            assert_eq!(g, vec!["order.type".to_string(), "region.name".to_string()]);
        } else {
            panic!("Expected Query");
        }
    }

    #[test]
    fn test_parse_query_linked_after_where_should_fail() {
        // LINKED must appear before WHERE in our grammar
        let input = r#"QUERY orders WHERE status="ok" LINKED BY account_id"#;
        let tokens = tokenize(input);
        let result = query::parse(&tokens);
        assert!(result.is_err(), "Expected error for LINKED after WHERE");
    }

    #[test]
    fn test_parse_query_count_unique_with_string_field_should_fail() {
        let input = r#"QUERY orders COUNT UNIQUE "field""#;
        let tokens = tokenize(input);
        let result = query::parse(&tokens);
        assert!(
            result.is_err(),
            "Expected error for UNIQUE with quoted field"
        );
    }

    #[test]
    fn test_parse_query_group_by_missing_field_should_fail() {
        let input = r#"QUERY orders COUNT BY"#;
        let tokens = tokenize(input);
        let result = query::parse(&tokens);
        assert!(result.is_err(), "Expected error for BY without fields");
    }

    #[test]
    fn test_parse_query_avg_missing_field_should_fail() {
        let input = r#"QUERY orders AVG"#;
        let tokens = tokenize(input);
        let result = query::parse(&tokens);
        assert!(result.is_err(), "Expected error for AVG without field");
    }

    #[test]
    fn test_parse_query_multiple_group_by_clauses_should_fail() {
        let input = r#"QUERY orders COUNT BY a BY b"#;
        let tokens = tokenize(input);
        let result = query::parse(&tokens);
        assert!(result.is_err(), "Expected error for multiple BY clauses");
    }

    #[test]
    fn test_parse_query_limit_not_last_should_fail() {
        let input = r#"QUERY orders COUNT LIMIT 5 PER day"#;
        let tokens = tokenize(input);
        let result = query::parse(&tokens);
        assert!(result.is_err(), "Expected error when LIMIT is not last");
    }

    #[test]
    fn test_parse_query_where_with_dotted_field() {
        let input = r#"QUERY orders WHERE order.status = "ok""#;
        let tokens = tokenize(input);
        let command = query::parse(&tokens).expect("Failed to parse WHERE with dotted field");
        if let Command::Query { where_clause, .. } = command {
            let wc = where_clause.expect("missing where");
            if let Expr::Compare { field, .. } = wc {
                assert_eq!(field, "order.status".to_string());
            } else {
                panic!("Expected Expr::Compare");
            }
        } else {
            panic!("Expected Query");
        }
    }

    #[test]
    fn test_parse_query_return_with_dotted_field() {
        let input = r#"QUERY orders RETURN [order.status]"#;
        let tokens = tokenize(input);
        let command = query::parse(&tokens).expect("Failed to parse RETURN with dotted field");
        if let Command::Query { return_fields, .. } = command {
            let rf = return_fields.expect("missing return_fields");
            assert_eq!(rf, vec!["order.status".to_string()]);
        } else {
            panic!("Expected Query");
        }
    }

    #[test]
    fn test_parse_query_with_linked_by() {
        let input = r#"QUERY orders LINKED BY user_id"#;
        let tokens = tokenize(input);

        let command = query::parse(&tokens).expect("Failed to parse QUERY with LINKED BY");

        assert_eq!(
            command,
            Command::Query {
                event_type: "orders".to_string(),
                context_id: None,
                since: None,
                where_clause: None,
                limit: None,
                return_fields: None,
                link_field: Some("user_id".to_string()),
                aggs: None,
                time_bucket: None,
                group_by: None,
                event_sequence: None,
            }
        );
    }

    #[test]
    fn test_parse_query_with_aggs_simple() {
        let input = r#"QUERY orders COUNT"#;
        let tokens = tokenize(input);

        let command = query::parse(&tokens).expect("Failed to parse QUERY with COUNT");

        assert_eq!(
            command,
            Command::Query {
                event_type: "orders".to_string(),
                context_id: None,
                since: None,
                where_clause: None,
                limit: None,
                return_fields: None,
                link_field: None,
                aggs: Some(vec![AggSpec::Count { unique_field: None }]),
                time_bucket: None,
                group_by: None,
                event_sequence: None,
            }
        );
    }

    #[test]
    fn test_parse_query_with_aggs_multiple() {
        let input = r#"QUERY orders COUNT, AVG amount, MIN amount, MAX amount, TOTAL amount"#;
        let tokens = tokenize(input);

        let command = query::parse(&tokens).expect("Failed to parse QUERY with multiple aggs");

        assert_eq!(
            command,
            Command::Query {
                event_type: "orders".to_string(),
                context_id: None,
                since: None,
                where_clause: None,
                limit: None,
                return_fields: None,
                link_field: None,
                aggs: Some(vec![
                    AggSpec::Count { unique_field: None },
                    AggSpec::Avg {
                        field: "amount".to_string()
                    },
                    AggSpec::Min {
                        field: "amount".to_string()
                    },
                    AggSpec::Max {
                        field: "amount".to_string()
                    },
                    AggSpec::Total {
                        field: "amount".to_string()
                    },
                ]),
                time_bucket: None,
                group_by: None,
                event_sequence: None,
            }
        );
    }

    #[test]
    fn test_parse_query_count_unique_dotted_field() {
        let input = r#"QUERY orders COUNT UNIQUE order.status"#;
        let tokens = tokenize(input);

        let command =
            query::parse(&tokens).expect("Failed to parse COUNT UNIQUE with dotted field");

        assert_eq!(
            command,
            Command::Query {
                event_type: "orders".to_string(),
                context_id: None,
                since: None,
                where_clause: None,
                limit: None,
                return_fields: None,
                link_field: None,
                aggs: Some(vec![AggSpec::Count {
                    unique_field: Some("order.status".to_string())
                }]),
                time_bucket: None,
                group_by: None,
                event_sequence: None,
            }
        );
    }

    #[test]
    fn test_parse_query_with_per_and_by() {
        let input = r#"QUERY orders COUNT PER day BY country, plan"#;
        let tokens = tokenize(input);

        let command = query::parse(&tokens).expect("Failed to parse QUERY with PER and BY");

        assert_eq!(
            command,
            Command::Query {
                event_type: "orders".to_string(),
                context_id: None,
                since: None,
                where_clause: None,
                limit: None,
                return_fields: None,
                link_field: None,
                aggs: Some(vec![AggSpec::Count { unique_field: None }]),
                time_bucket: Some(TimeGranularity::Day),
                group_by: Some(vec!["country".to_string(), "plan".to_string()]),
                event_sequence: None,
            }
        );
    }

    #[test]
    fn test_parse_query_full_with_new_clauses() {
        let input = r#"QUERY orders LINKED BY account_id WHERE status = "ok" COUNT UNIQUE context_id, AVG amount PER month BY country, plan LIMIT 10"#;
        let tokens = tokenize(input);

        let command = query::parse(&tokens).expect("Failed to parse full QUERY with new clauses");

        assert_eq!(
            command,
            Command::Query {
                event_type: "orders".to_string(),
                context_id: None,
                since: None,
                where_clause: Some(Expr::Compare {
                    field: "status".to_string(),
                    op: CompareOp::Eq,
                    value: json!("ok"),
                }),
                limit: Some(10),
                return_fields: None,
                link_field: Some("account_id".to_string()),
                aggs: Some(vec![
                    AggSpec::Count {
                        unique_field: Some("context_id".to_string())
                    },
                    AggSpec::Avg {
                        field: "amount".to_string()
                    },
                ]),
                time_bucket: Some(TimeGranularity::Month),
                group_by: Some(vec!["country".to_string(), "plan".to_string()]),
                event_sequence: None,
            }
        );
    }

    #[test]
    fn test_parse_find_alias() {
        let input = r#"FIND orders WHERE status = "ok""#;
        let tokens = tokenize(input);

        let command = query::parse(&tokens);
        assert!(
            command.is_err(),
            "Direct query::parse should reject FIND token"
        );

        // Simulate top-level parser alias by replacing the first token to QUERY
        let mut tokens = tokenize(input);
        if let Some(crate::command::parser::tokenizer::Token::Word(w)) = tokens.first_mut() {
            *w = "QUERY".to_string();
        }
        let command =
            query::parse(&tokens).expect("Failed to parse FIND alias after aliasing to QUERY");
        if let Command::Query { where_clause, .. } = command {
            assert!(where_clause.is_some());
        } else {
            panic!("Expected Command::Query");
        }
    }

    #[test]
    fn test_parse_query_count_unique_missing_field_should_fail() {
        let input = r#"QUERY orders COUNT UNIQUE"#;
        let tokens = tokenize(input);
        let result = query::parse(&tokens);
        assert!(
            result.is_err(),
            "Expected error for COUNT UNIQUE without field"
        );
    }

    #[test]
    fn test_parse_query_per_invalid_unit_should_fail() {
        let input = r#"QUERY orders COUNT PER year"#;
        let tokens = tokenize(input);
        let result = query::parse(&tokens);
        assert!(result.is_err(), "Expected error for invalid PER unit");
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
                link_field: None,
                aggs: None,
                time_bucket: None,
                group_by: None,
                event_sequence: None,
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
                link_field: None,
                aggs: None,
                time_bucket: None,
                group_by: None,
                event_sequence: None,
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
                link_field: None,
                aggs: None,
                time_bucket: None,
                group_by: None,
                event_sequence: None,
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
                link_field: None,
                aggs: None,
                time_bucket: None,
                group_by: None,
                event_sequence: None,
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
                link_field: None,
                aggs: None,
                time_bucket: None,
                group_by: None,
                event_sequence: None,
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
                link_field: None,
                aggs: None,
                time_bucket: None,
                group_by: None,
                event_sequence: None,
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
                link_field: None,
                aggs: None,
                time_bucket: None,
                group_by: None,
                event_sequence: None,
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
                link_field: None,
                aggs: None,
                time_bucket: None,
                group_by: None,
                event_sequence: None,
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
                link_field: None,
                aggs: None,
                time_bucket: None,
                group_by: None,
                event_sequence: None,
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
                link_field: None,
                aggs: None,
                time_bucket: None,
                group_by: None,
                event_sequence: None,
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
                link_field: None,
                aggs: None,
                time_bucket: None,
                group_by: None,
                event_sequence: None,
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
                return_fields: Some(vec![
                    "context_id".to_string(),
                    "event_type".to_string(),
                    "timestamp".to_string(),
                    "payload".to_string(),
                ]),
                link_field: None,
                aggs: None,
                time_bucket: None,
                group_by: None,
                event_sequence: None,
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
                return_fields: Some(vec![]),
                link_field: None,
                aggs: None,
                time_bucket: None,
                group_by: None,
                event_sequence: None,
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
                return_fields: Some(vec!["plan".to_string(), "country".to_string()]),
                link_field: None,
                aggs: None,
                time_bucket: None,
                group_by: None,
                event_sequence: None,
            }
        );
    }

    #[test]
    fn test_parse_query_with_return_mixed_whitespace_and_quotes() {
        let input = r#"QUERY order_created RETURN [ name , "country" , plan ] WHERE status = "ok""#;
        let tokens = tokenize(input);

        let command = query::parse(&tokens).expect("Failed to parse QUERY with mixed RETURN list");

        assert_eq!(
            command,
            Command::Query {
                event_type: "order_created".to_string(),
                context_id: None,
                since: None,
                where_clause: Some(Expr::Compare {
                    field: "status".to_string(),
                    op: CompareOp::Eq,
                    value: json!("ok"),
                }),
                limit: None,
                return_fields: Some(vec![
                    "name".to_string(),
                    "country".to_string(),
                    "plan".to_string(),
                ]),
                link_field: None,
                aggs: None,
                time_bucket: None,
                group_by: None,
                event_sequence: None,
            }
        );
    }

    #[test]
    fn test_parse_query_with_return_duplicates_preserved() {
        let input = r#"QUERY order_created RETURN [name, name, "name"]"#;
        let tokens = tokenize(input);

        let command =
            query::parse(&tokens).expect("Failed to parse QUERY with duplicate RETURN fields");

        assert_eq!(
            command,
            Command::Query {
                event_type: "order_created".to_string(),
                context_id: None,
                since: None,
                where_clause: None,
                limit: None,
                return_fields: Some(vec![
                    "name".to_string(),
                    "name".to_string(),
                    "name".to_string(),
                ]),
                link_field: None,
                aggs: None,
                time_bucket: None,
                group_by: None,
                event_sequence: None,
            }
        );
    }

    #[test]
    fn test_parse_query_with_return_missing_right_bracket_should_fail() {
        let input = r#"QUERY order_created RETURN ["name", country"#; // missing closing ]
        let tokens = tokenize(input);

        let result = query::parse(&tokens);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_query_with_return_missing_left_bracket_should_fail() {
        let input = r#"QUERY order_created RETURN name]"#; // missing opening [
        let tokens = tokenize(input);

        let result = query::parse(&tokens);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_query_with_return_invalid_token_should_fail() {
        let input = r#"QUERY order_created RETURN [123, name]"#; // 123 is invalid token for field
        let tokens = tokenize(input);

        let result = query::parse(&tokens);
        assert!(result.is_err());
    }
}
