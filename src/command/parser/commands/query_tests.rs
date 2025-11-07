use crate::command::parser::commands::query::parse as parse_query_peg;
use crate::command::types::{
    AggSpec, Command, CompareOp, EventSequence, EventTarget, Expr, SequenceLink, TimeGranularity,
};
use serde_json::Value;

#[cfg(test)]
mod query_peg_tests {
    use super::*;

    fn parse(input: &str) -> Command {
        parse_query_peg(input).expect("Failed to parse QUERY command")
    }

    // ─────────────────────────────
    // 1. Minimal Query
    // ─────────────────────────────
    #[test]
    fn test_parse_query_minimal() {
        let input = r#"QUERY order_created"#;
        let command = parse(input);

        assert_eq!(
            command,
            Command::Query {
                event_type: "order_created".to_string(),
                context_id: None,
                since: None,
                time_field: None,
                sequence_time_field: None,
                where_clause: None,
                limit: None,
                offset: None,
                order_by: None,
                picked_zones: None,
                return_fields: None,
                link_field: None,
                aggs: None,
                time_bucket: None,
                group_by: None,
                event_sequence: None,
            }
        );
    }

    // ─────────────────────────────
    // 2. FOR and SINCE
    // ─────────────────────────────
    #[test]
    fn test_parse_query_with_for() {
        let input = r#"QUERY order_created FOR ctx-123"#;
        let command = parse(input);

        assert_eq!(
            command,
            Command::Query {
                event_type: "order_created".to_string(),
                context_id: Some("ctx-123".to_string()),
                since: None,
                time_field: None,
                sequence_time_field: None,
                where_clause: None,
                limit: None,
                offset: None,
                order_by: None,
                picked_zones: None,
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
        let input = r#"QUERY order_created SINCE "2025-01-01T00:00:00Z""#;
        let command = parse(input);

        assert_eq!(
            command,
            Command::Query {
                event_type: "order_created".to_string(),
                context_id: None,
                since: Some("2025-01-01T00:00:00Z".to_string()),
                time_field: None,
                sequence_time_field: None,
                where_clause: None,
                limit: None,
                offset: None,
                order_by: None,
                picked_zones: None,
                return_fields: None,
                link_field: None,
                aggs: None,
                time_bucket: None,
                group_by: None,
                event_sequence: None,
            }
        );
    }

    // ─────────────────────────────
    // 3. Event Sequences
    // ─────────────────────────────
    #[test]
    fn test_parse_query_followed_by() {
        let input = r#"QUERY page_view FOLLOWED BY order_created"#;
        let command = parse(input);

        assert_eq!(
            command,
            Command::Query {
                event_type: "page_view".to_string(),
                context_id: None,
                since: None,
                time_field: None,
                sequence_time_field: None,
                where_clause: None,
                limit: None,
                offset: None,
                order_by: None,
                picked_zones: None,
                return_fields: None,
                link_field: None,
                aggs: None,
                time_bucket: None,
                group_by: None,
                event_sequence: Some(EventSequence {
                    head: EventTarget {
                        event: "page_view".to_string(),
                        field: None,
                    },
                    links: vec![(
                        SequenceLink::FollowedBy,
                        EventTarget {
                            event: "order_created".to_string(),
                            field: None,
                        }
                    )],
                }),
            }
        );
    }

    #[test]
    fn test_parse_query_preceded_by() {
        let input = r#"QUERY payment_succeeded PRECEDED BY order_created"#;
        let command = parse(input);

        assert_eq!(
            command,
            Command::Query {
                event_type: "payment_succeeded".to_string(),
                context_id: None,
                since: None,
                time_field: None,
                sequence_time_field: None,
                where_clause: None,
                limit: None,
                offset: None,
                order_by: None,
                picked_zones: None,
                return_fields: None,
                link_field: None,
                aggs: None,
                time_bucket: None,
                group_by: None,
                event_sequence: Some(EventSequence {
                    head: EventTarget {
                        event: "payment_succeeded".to_string(),
                        field: None,
                    },
                    links: vec![(
                        SequenceLink::PrecededBy,
                        EventTarget {
                            event: "order_created".to_string(),
                            field: None,
                        }
                    )],
                }),
            }
        );
    }

    // ─────────────────────────────
    // 4. LINKED BY
    // ─────────────────────────────
    #[test]
    fn test_parse_query_linked_by() {
        let input = r#"QUERY order_created LINKED BY user_id"#;
        let command = parse(input);

        assert_eq!(
            command,
            Command::Query {
                event_type: "order_created".to_string(),
                context_id: None,
                since: None,
                time_field: None,
                sequence_time_field: None,
                where_clause: None,
                limit: None,
                offset: None,
                order_by: None,
                picked_zones: None,
                return_fields: None,
                link_field: Some("user_id".to_string()),
                aggs: None,
                time_bucket: None,
                group_by: None,
                event_sequence: None,
            }
        );
    }

    // ─────────────────────────────
    // 5. WHERE Clauses
    // ─────────────────────────────
    #[test]
    fn test_parse_query_where_eq() {
        let input = r#"QUERY order_created WHERE status = "done""#;
        let command = parse(input);

        assert_eq!(
            command,
            Command::Query {
                event_type: "order_created".to_string(),
                context_id: None,
                since: None,
                time_field: None,
                sequence_time_field: None,
                where_clause: Some(Expr::Compare {
                    field: "status".to_string(),
                    op: CompareOp::Eq,
                    value: Value::String("done".to_string()),
                }),
                limit: None,
                offset: None,
                order_by: None,
                picked_zones: None,
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
    fn test_parse_query_where_gt_and_and() {
        let input = r#"QUERY order_created WHERE total >= 50 AND country = "NL""#;
        let command = parse(input);

        assert_eq!(
            command,
            Command::Query {
                event_type: "order_created".to_string(),
                context_id: None,
                since: None,
                time_field: None,
                sequence_time_field: None,
                where_clause: Some(Expr::And(
                    Box::new(Expr::Compare {
                        field: "total".to_string(),
                        op: CompareOp::Gte,
                        value: Value::Number(50.into()),
                    }),
                    Box::new(Expr::Compare {
                        field: "country".to_string(),
                        op: CompareOp::Eq,
                        value: Value::String("NL".to_string()),
                    })
                )),
                limit: None,
                offset: None,
                order_by: None,
                picked_zones: None,
                return_fields: None,
                link_field: None,
                aggs: None,
                time_bucket: None,
                group_by: None,
                event_sequence: None,
            }
        );
    }

    // ─────────────────────────────
    // 6. RETURN Clause
    // ─────────────────────────────
    #[test]
    fn test_parse_query_return_list() {
        let input = r#"QUERY order_created RETURN [id, total, status]"#;
        let command = parse(input);

        assert_eq!(
            command,
            Command::Query {
                event_type: "order_created".to_string(),
                context_id: None,
                since: None,
                time_field: None,
                sequence_time_field: None,
                where_clause: None,
                limit: None,
                offset: None,
                order_by: None,
                picked_zones: None,
                return_fields: Some(vec![
                    "id".to_string(),
                    "total".to_string(),
                    "status".to_string()
                ]),
                link_field: None,
                aggs: None,
                time_bucket: None,
                group_by: None,
                event_sequence: None,
            }
        );
    }

    // ─────────────────────────────
    // 7. Aggregations
    // ─────────────────────────────
    #[test]
    fn test_parse_query_count_unique() {
        let input = r#"QUERY order_created COUNT UNIQUE user_id"#;
        let command = parse(input);

        assert_eq!(
            command,
            Command::Query {
                event_type: "order_created".to_string(),
                context_id: None,
                since: None,
                time_field: None,
                sequence_time_field: None,
                where_clause: None,
                limit: None,
                offset: None,
                order_by: None,
                picked_zones: None,
                return_fields: None,
                link_field: None,
                aggs: Some(vec![AggSpec::Count {
                    unique_field: Some("user_id".to_string())
                }]),
                time_bucket: None,
                group_by: None,
                event_sequence: None,
            }
        );
    }

    #[test]
    fn test_parse_query_total_amount() {
        let input = r#"QUERY order_created TOTAL amount"#;
        let command = parse(input);

        assert_eq!(
            command,
            Command::Query {
                event_type: "order_created".to_string(),
                context_id: None,
                since: None,
                time_field: None,
                sequence_time_field: None,
                where_clause: None,
                limit: None,
                offset: None,
                order_by: None,
                picked_zones: None,
                return_fields: None,
                link_field: None,
                aggs: Some(vec![AggSpec::Total {
                    field: "amount".to_string()
                }]),
                time_bucket: None,
                group_by: None,
                event_sequence: None,
            }
        );
    }

    // ─────────────────────────────
    // 8. Time Bucketing and USING
    // ─────────────────────────────
    #[test]
    fn test_parse_query_per_day_using() {
        let input = r#"QUERY order_created COUNT PER day USING created_at"#;
        let command = parse(input);

        assert_eq!(
            command,
            Command::Query {
                event_type: "order_created".to_string(),
                context_id: None,
                since: None,
                time_field: Some("created_at".to_string()),
                sequence_time_field: None,
                where_clause: None,
                limit: None,
                offset: None,
                order_by: None,
                picked_zones: None,
                return_fields: None,
                link_field: None,
                aggs: Some(vec![AggSpec::Count { unique_field: None }]),
                time_bucket: Some(TimeGranularity::Day),
                group_by: None,
                event_sequence: None,
            }
        );
    }

    // ─────────────────────────────
    // 9. Grouping
    // ─────────────────────────────
    #[test]
    fn test_parse_query_group_by_country() {
        let input = r#"QUERY order_created COUNT BY country"#;
        let command = parse(input);

        assert_eq!(
            command,
            Command::Query {
                event_type: "order_created".to_string(),
                context_id: None,
                since: None,
                time_field: None,
                sequence_time_field: None,
                where_clause: None,
                limit: None,
                offset: None,
                order_by: None,
                picked_zones: None,
                return_fields: None,
                link_field: None,
                aggs: Some(vec![AggSpec::Count { unique_field: None }]),
                time_bucket: None,
                group_by: Some(vec!["country".to_string()]),
                event_sequence: None,
            }
        );
    }

    // ─────────────────────────────
    // 10. Limit
    // ─────────────────────────────
    #[test]
    fn test_parse_query_limit_clause() {
        let input = r#"QUERY order_created COUNT LIMIT 100"#;
        let command = parse(input);

        assert_eq!(
            command,
            Command::Query {
                event_type: "order_created".to_string(),
                context_id: None,
                since: None,
                time_field: None,
                sequence_time_field: None,
                where_clause: None,
                limit: Some(100),
                offset: None,
                order_by: None,
                picked_zones: None,
                return_fields: None,
                link_field: None,
                aggs: Some(vec![AggSpec::Count { unique_field: None }]),
                time_bucket: None,
                group_by: None,
                event_sequence: None,
            }
        );
    }

    // ─────────────────────────────
    // 11. Combined Example
    // ─────────────────────────────
    #[test]
    fn test_parse_query_combined_complex() {
        let input = r#"QUERY page_view FOLLOWED BY order_created LINKED BY user_id WHERE order_created.status = "paid" COUNT UNIQUE user_id PER week USING created_at BY country LIMIT 50"#;
        let command = parse(input);

        assert_eq!(
            command,
            Command::Query {
                event_type: "page_view".to_string(),
                context_id: None,
                since: None,
                time_field: Some("created_at".to_string()),
                sequence_time_field: None,
                where_clause: Some(Expr::Compare {
                    field: "order_created.status".to_string(),
                    op: CompareOp::Eq,
                    value: Value::String("paid".to_string()),
                }),
                limit: Some(50),
                offset: None,
                order_by: None,
                picked_zones: None,
                return_fields: None,
                link_field: Some("user_id".to_string()),
                aggs: Some(vec![AggSpec::Count {
                    unique_field: Some("user_id".to_string())
                }]),
                time_bucket: Some(TimeGranularity::Week),
                group_by: Some(vec!["country".to_string()]),
                event_sequence: Some(EventSequence {
                    head: EventTarget {
                        event: "page_view".to_string(),
                        field: None,
                    },
                    links: vec![(
                        SequenceLink::FollowedBy,
                        EventTarget {
                            event: "order_created".to_string(),
                            field: None,
                        }
                    )],
                }),
            }
        );
    }

    // ─────────────────────────────
    // 12. Negative Tests
    // ─────────────────────────────
    #[test]
    fn test_parse_query_missing_event_should_fail() {
        let input = r#"QUERY"#;
        assert!(parse_query_peg(input).is_err());
    }

    #[test]
    fn test_parse_query_invalid_followed_should_fail() {
        let input = r#"QUERY order_created FOLLOWED"#;
        assert!(parse_query_peg(input).is_err());
    }

    #[test]
    fn test_parse_query_invalid_where_should_fail() {
        let input = r#"QUERY order_created WHERE = 10"#;
        assert!(parse_query_peg(input).is_err());
    }

    #[test]
    fn test_parse_query_invalid_return_should_fail() {
        let input = r#"QUERY order_created RETURN id, total"#;
        assert!(parse_query_peg(input).is_err());
    }

    #[test]
    fn test_parse_query_invalid_keyword_ci_guard() {
        let input = r#"QUEER order_created"#;
        assert!(parse_query_peg(input).is_err());
    }

    #[test]
    fn test_parse_query_return_trailing_comma_should_fail() {
        let input = r#"QUERY e RETURN [id,]"#;
        assert!(parse_query_peg(input).is_err());
    }

    #[test]
    fn test_parse_query_aggs_trailing_comma_should_fail() {
        let input = r#"QUERY e COUNT, "#;
        assert!(parse_query_peg(input).is_err());
    }

    #[test]
    fn test_parse_query_group_by_empty_should_fail() {
        let input = r#"QUERY e BY"#;
        assert!(parse_query_peg(input).is_err());
    }

    #[test]
    fn test_parse_query_invalid_per_unit_should_fail() {
        let input = r#"QUERY e COUNT PER year"#;
        assert!(parse_query_peg(input).is_err());
    }

    #[test]
    fn test_parse_query_unterminated_string_in_where_should_fail() {
        let input = r#"QUERY e WHERE status = "open"#;
        assert!(parse_query_peg(input).is_err());
    }

    #[test]
    fn test_parse_query_unterminated_string_in_return_should_fail() {
        let input = r#"QUERY e RETURN ["id, total]"#;
        assert!(parse_query_peg(input).is_err());
    }

    #[test]
    fn test_parse_query_return_mixed_quoted_and_identifiers_allowed() {
        let input = r#"QUERY e RETURN ["id", total]"#;
        let command = parse(input);
        assert_eq!(
            command,
            Command::Query {
                event_type: "e".to_string(),
                context_id: None,
                since: None,
                time_field: None,
                sequence_time_field: None,
                where_clause: None,
                limit: None,
                offset: None,
                order_by: None,
                picked_zones: None,
                return_fields: Some(vec!["id".to_string(), "total".to_string()]),
                link_field: None,
                aggs: None,
                time_bucket: None,
                group_by: None,
                event_sequence: None,
            }
        );
    }

    #[test]
    fn test_parse_query_field_with_quotes_in_by_should_fail() {
        let input = r#"QUERY e BY "country""#;
        assert!(parse_query_peg(input).is_err());
    }

    // ─────────────────────────────
    // 13. Additional Edge Cases
    // ─────────────────────────────
    #[test]
    fn test_parse_query_find_keyword_ci() {
        let input = r#"find order_created"#;
        let command = parse(input);

        assert_eq!(
            command,
            Command::Query {
                event_type: "order_created".to_string(),
                context_id: None,
                since: None,
                time_field: None,
                sequence_time_field: None,
                where_clause: None,
                limit: None,
                offset: None,
                order_by: None,
                picked_zones: None,
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
    fn test_parse_query_sequence_chain_mixed_links() {
        let input = r#"QUERY a FOLLOWED BY b PRECEDED BY c"#;
        let command = parse(input);

        assert_eq!(
            command,
            Command::Query {
                event_type: "a".to_string(),
                context_id: None,
                since: None,
                time_field: None,
                sequence_time_field: None,
                where_clause: None,
                limit: None,
                offset: None,
                order_by: None,
                picked_zones: None,
                return_fields: None,
                link_field: None,
                aggs: None,
                time_bucket: None,
                group_by: None,
                event_sequence: Some(EventSequence {
                    head: EventTarget {
                        event: "a".to_string(),
                        field: None
                    },
                    links: vec![
                        (
                            SequenceLink::FollowedBy,
                            EventTarget {
                                event: "b".to_string(),
                                field: None
                            }
                        ),
                        (
                            SequenceLink::PrecededBy,
                            EventTarget {
                                event: "c".to_string(),
                                field: None
                            }
                        ),
                    ],
                }),
            }
        );
    }

    #[test]
    fn test_parse_query_return_with_string_literals() {
        let input = r#"QUERY order_created RETURN ["id", "total"]"#;
        let command = parse(input);

        assert_eq!(
            command,
            Command::Query {
                event_type: "order_created".to_string(),
                context_id: None,
                since: None,
                time_field: None,
                sequence_time_field: None,
                where_clause: None,
                limit: None,
                offset: None,
                order_by: None,
                picked_zones: None,
                return_fields: Some(vec!["id".to_string(), "total".to_string()]),
                link_field: None,
                aggs: None,
                time_bucket: None,
                group_by: None,
                event_sequence: None,
            }
        );
    }

    #[test]
    fn test_parse_query_where_unquoted_identifier_value() {
        let input = r#"QUERY order_created WHERE status = done"#;
        let command = parse(input);

        assert_eq!(
            command,
            Command::Query {
                event_type: "order_created".to_string(),
                context_id: None,
                since: None,
                time_field: None,
                sequence_time_field: None,
                where_clause: Some(Expr::Compare {
                    field: "status".to_string(),
                    op: CompareOp::Eq,
                    value: Value::String("done".to_string()),
                }),
                limit: None,
                offset: None,
                order_by: None,
                picked_zones: None,
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
    fn test_parse_query_where_not_and_atom_precedence() {
        let input = r#"QUERY e WHERE NOT is_deleted AND active"#;
        let command = parse(input);

        assert_eq!(
            command,
            Command::Query {
                event_type: "e".to_string(),
                context_id: None,
                since: None,
                time_field: None,
                sequence_time_field: None,
                where_clause: Some(Expr::And(
                    Box::new(Expr::Not(Box::new(Expr::Compare {
                        field: "is_deleted".to_string(),
                        op: CompareOp::Eq,
                        value: Value::Bool(true),
                    }))),
                    Box::new(Expr::Compare {
                        field: "active".to_string(),
                        op: CompareOp::Eq,
                        value: Value::Bool(true),
                    })
                )),
                limit: None,
                offset: None,
                order_by: None,
                picked_zones: None,
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
    fn test_parse_query_count_field_agg() {
        let input = r#"QUERY order_created COUNT total"#;
        let command = parse(input);

        assert_eq!(
            command,
            Command::Query {
                event_type: "order_created".to_string(),
                context_id: None,
                since: None,
                time_field: None,
                sequence_time_field: None,
                where_clause: None,
                limit: None,
                offset: None,
                order_by: None,
                picked_zones: None,
                return_fields: None,
                link_field: None,
                aggs: Some(vec![AggSpec::CountField {
                    field: "total".to_string()
                }]),
                time_bucket: None,
                group_by: None,
                event_sequence: None,
            }
        );
    }

    #[test]
    fn test_parse_query_multiple_aggs_with_commas() {
        let input = r#"QUERY e COUNT, AVG amount, MAX total"#;
        let command = parse(input);

        assert_eq!(
            command,
            Command::Query {
                event_type: "e".to_string(),
                context_id: None,
                since: None,
                time_field: None,
                sequence_time_field: None,
                where_clause: None,
                limit: None,
                offset: None,
                order_by: None,
                picked_zones: None,
                return_fields: None,
                link_field: None,
                aggs: Some(vec![
                    AggSpec::Count { unique_field: None },
                    AggSpec::Avg {
                        field: "amount".to_string()
                    },
                    AggSpec::Max {
                        field: "total".to_string()
                    },
                ]),
                time_bucket: None,
                group_by: None,
                event_sequence: None,
            }
        );
    }

    #[test]
    fn test_parse_query_group_by_multiple_fields_and_using() {
        let input = r#"QUERY e BY country, city USING created_at"#;
        let command = parse(input);

        assert_eq!(
            command,
            Command::Query {
                event_type: "e".to_string(),
                context_id: None,
                since: None,
                time_field: Some("created_at".to_string()),
                sequence_time_field: None,
                where_clause: None,
                limit: None,
                offset: None,
                order_by: None,
                picked_zones: None,
                return_fields: None,
                link_field: None,
                aggs: None,
                time_bucket: None,
                group_by: Some(vec!["country".to_string(), "city".to_string()]),
                event_sequence: None,
            }
        );
    }

    #[test]
    fn test_parse_query_per_month_time_bucket() {
        let input = r#"QUERY e COUNT PER month"#;
        let command = parse(input);

        assert_eq!(
            command,
            Command::Query {
                event_type: "e".to_string(),
                context_id: None,
                since: None,
                time_field: None,
                sequence_time_field: None,
                where_clause: None,
                limit: None,
                offset: None,
                order_by: None,
                picked_zones: None,
                return_fields: None,
                link_field: None,
                aggs: Some(vec![AggSpec::Count { unique_field: None }]),
                time_bucket: Some(TimeGranularity::Month),
                group_by: None,
                event_sequence: None,
            }
        );
    }
}
