use super::plotql;
use crate::command::parser::parse_command;
use crate::command::types::{
    AggSpec, Command, CompareOp, EventSequence, EventTarget, Expr, OrderSpec, SequenceLink,
    TimeGranularity,
};

#[test]
fn parses_total_over_day() {
    let cmd =
        plotql::parse("plot total(amount) of payment_succeeded over day(created_at)").unwrap();
    if let Command::Query {
        event_type,
        aggs,
        time_bucket,
        time_field,
        group_by,
        event_sequence,
        ..
    } = cmd
    {
        assert_eq!(event_type, "payment_succeeded");
        assert_eq!(
            aggs,
            Some(vec![AggSpec::Total {
                field: "amount".to_string()
            }])
        );
        assert_eq!(time_bucket, Some(TimeGranularity::Day));
        assert_eq!(time_field, Some("created_at".to_string()));
        assert!(group_by.is_none());
        assert!(event_sequence.is_none());
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_funnel_with_breakdown_and_time() {
    let cmd = plotql::parse("plot count(user_id) of page_view->add_to_cart->payment_succeeded breakdown by category over week(created_at)").unwrap();
    if let Command::Query {
        event_type,
        aggs,
        group_by,
        time_bucket,
        event_sequence,
        ..
    } = cmd
    {
        assert_eq!(event_type, "page_view");
        assert_eq!(
            aggs,
            Some(vec![AggSpec::CountField {
                field: "user_id".to_string()
            }])
        );
        assert_eq!(group_by, Some(vec!["category".to_string()]));
        assert_eq!(time_bucket, Some(TimeGranularity::Week));
        let seq = event_sequence.expect("expected sequence");
        assert_eq!(seq.head.event, "page_view");
        assert_eq!(seq.links.len(), 2);
        assert_eq!(seq.links[0].0, SequenceLink::FollowedBy);
        assert_eq!(seq.links[0].1.event, "add_to_cart");
        assert_eq!(seq.links[1].1.event, "payment_succeeded");
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_filters_and_expressions() {
    let cmd = plotql::parse(
        "plot count of orders filter status=\"paid\" and country=\"NL\" over day(created_at)",
    )
    .unwrap();
    if let Command::Query {
        where_clause,
        time_bucket,
        aggs,
        ..
    } = cmd
    {
        let filter = where_clause.expect("expected where clause");
        match filter {
            Expr::And(left, right) => {
                match *left {
                    Expr::Compare {
                        field,
                        op: CompareOp::Eq,
                        value,
                    } => {
                        assert_eq!(field, "status");
                        assert_eq!(value, serde_json::json!("paid"));
                    }
                    _ => panic!("unexpected left expression"),
                }
                match *right {
                    Expr::Compare {
                        field,
                        op: CompareOp::Eq,
                        value,
                    } => {
                        assert_eq!(field, "country");
                        assert_eq!(value, serde_json::json!("NL"));
                    }
                    _ => panic!("unexpected right expression"),
                }
            }
            _ => panic!("unexpected expression tree"),
        }
        assert_eq!(time_bucket, Some(TimeGranularity::Day));
        assert_eq!(aggs, Some(vec![AggSpec::Count { unique_field: None }]));
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_top_clause_with_order() {
    let cmd = plotql::parse(
        "plot total(amount) of payment_succeeded breakdown by product_id top 10 by total(amount)",
    )
    .unwrap();
    if let Command::Query {
        limit,
        order_by,
        group_by,
        ..
    } = cmd
    {
        assert_eq!(limit, Some(10));
        assert_eq!(
            order_by,
            Some(OrderSpec {
                field: "total_amount".to_string(),
                desc: true
            })
        );
        assert_eq!(group_by, Some(vec!["product_id".to_string()]));
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_top_clause_without_order() {
    // Top clause without explicit "by" should default to ordering by main metric
    let cmd =
        plotql::parse("plot total(amount) of payment_succeeded breakdown by product_id top 10")
            .unwrap();
    if let Command::Query {
        limit,
        order_by,
        group_by,
        ..
    } = cmd
    {
        assert_eq!(limit, Some(10));
        assert_eq!(
            order_by,
            Some(OrderSpec {
                field: "total_amount".to_string(),
                desc: true
            })
        );
        assert_eq!(group_by, Some(vec!["product_id".to_string()]));
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_top_by_count() {
    let cmd =
        plotql::parse("plot count of orders breakdown by product_id top 10 by count").unwrap();
    if let Command::Query {
        limit,
        order_by,
        aggs,
        ..
    } = cmd
    {
        assert_eq!(limit, Some(10));
        assert_eq!(
            order_by,
            Some(OrderSpec {
                field: "count".to_string(),
                desc: true
            })
        );
        assert_eq!(aggs, Some(vec![AggSpec::Count { unique_field: None }]));
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_top_by_avg() {
    let cmd = plotql::parse(
        "plot avg(rating) of review_submitted breakdown by product_id top 5 by avg(rating)",
    )
    .unwrap();
    if let Command::Query {
        limit,
        order_by,
        aggs,
        ..
    } = cmd
    {
        assert_eq!(limit, Some(5));
        assert_eq!(
            order_by,
            Some(OrderSpec {
                field: "avg_rating".to_string(),
                desc: true
            })
        );
        assert_eq!(
            aggs,
            Some(vec![AggSpec::Avg {
                field: "rating".to_string()
            }])
        );
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_top_by_count_field() {
    let cmd = plotql::parse(
        "plot count(customer_id) of orders breakdown by product_id top 10 by count(customer_id)",
    )
    .unwrap();
    if let Command::Query {
        limit,
        order_by,
        aggs,
        ..
    } = cmd
    {
        assert_eq!(limit, Some(10));
        assert_eq!(
            order_by,
            Some(OrderSpec {
                field: "count_customer_id".to_string(),
                desc: true
            })
        );
        assert_eq!(
            aggs,
            Some(vec![AggSpec::CountField {
                field: "customer_id".to_string()
            }])
        );
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_top_by_unique() {
    let cmd = plotql::parse(
        "plot unique(customer_id) of orders breakdown by product_id top 10 by unique(customer_id)",
    )
    .unwrap();
    if let Command::Query {
        limit,
        order_by,
        aggs,
        ..
    } = cmd
    {
        assert_eq!(limit, Some(10));
        assert_eq!(
            order_by,
            Some(OrderSpec {
                field: "count_unique_customer_id".to_string(),
                desc: true
            })
        );
        assert_eq!(
            aggs,
            Some(vec![AggSpec::Count {
                unique_field: Some("customer_id".to_string())
            }])
        );
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_top_by_min() {
    let cmd =
        plotql::parse("plot min(price) of products breakdown by category top 5 by min(price)")
            .unwrap();
    if let Command::Query {
        limit,
        order_by,
        aggs,
        ..
    } = cmd
    {
        assert_eq!(limit, Some(5));
        assert_eq!(
            order_by,
            Some(OrderSpec {
                field: "min_price".to_string(),
                desc: true
            })
        );
        assert_eq!(
            aggs,
            Some(vec![AggSpec::Min {
                field: "price".to_string()
            }])
        );
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_top_by_max() {
    let cmd =
        plotql::parse("plot max(price) of products breakdown by category top 5 by max(price)")
            .unwrap();
    if let Command::Query {
        limit,
        order_by,
        aggs,
        ..
    } = cmd
    {
        assert_eq!(limit, Some(5));
        assert_eq!(
            order_by,
            Some(OrderSpec {
                field: "max_price".to_string(),
                desc: true
            })
        );
        assert_eq!(
            aggs,
            Some(vec![AggSpec::Max {
                field: "price".to_string()
            }])
        );
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_top_by_sum() {
    let cmd = plotql::parse(
        "plot sum(price) of payment_succeeded breakdown by product_id top 10 by sum(price)",
    )
    .unwrap();
    if let Command::Query {
        limit,
        order_by,
        aggs,
        ..
    } = cmd
    {
        assert_eq!(limit, Some(10));
        assert_eq!(
            order_by,
            Some(OrderSpec {
                field: "total_price".to_string(),
                desc: true
            })
        );
        assert_eq!(
            aggs,
            Some(vec![AggSpec::Total {
                field: "price".to_string()
            }])
        );
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parse_command_routes_plotql_queries() {
    let cmd = parse_command("plot count(customer_id) of page_view->add_to_cart->checkout_started->payment_succeeded over day(created_at)").unwrap();
    match cmd {
        Command::Query {
            event_type,
            event_sequence,
            ..
        } => {
            assert_eq!(event_type, "page_view");
            assert!(matches!(
                event_sequence,
                Some(EventSequence {
                    head: EventTarget { ref event, .. },
                    ..
                }) if event == "page_view"
            ));
        }
        _ => panic!("expected query command"),
    }
}

#[test]
fn parses_unique_metric() {
    let cmd = plotql::parse("plot unique(customer_id) of page_view breakdown by region").unwrap();
    if let Command::Query { aggs, group_by, .. } = cmd {
        assert_eq!(
            aggs,
            Some(vec![AggSpec::Count {
                unique_field: Some("customer_id".to_string())
            }])
        );
        assert_eq!(group_by, Some(vec!["region".to_string()]));
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_sum_metric() {
    let cmd = plotql::parse("plot sum(price) of payment_succeeded").unwrap();
    if let Command::Query { aggs, .. } = cmd {
        assert_eq!(
            aggs,
            Some(vec![AggSpec::Total {
                field: "price".to_string()
            }])
        );
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_avg_min_max_metrics() {
    let cmd = plotql::parse("plot avg(rating) of review_submitted").unwrap();
    if let Command::Query { aggs, .. } = cmd {
        assert_eq!(
            aggs,
            Some(vec![AggSpec::Avg {
                field: "rating".to_string()
            }])
        );
    } else {
        panic!("expected query command");
    }

    let cmd = plotql::parse("plot min(price) of products").unwrap();
    if let Command::Query { aggs, .. } = cmd {
        assert_eq!(
            aggs,
            Some(vec![AggSpec::Min {
                field: "price".to_string()
            }])
        );
    } else {
        panic!("expected query command");
    }

    let cmd = plotql::parse("plot max(price) of products").unwrap();
    if let Command::Query { aggs, .. } = cmd {
        assert_eq!(
            aggs,
            Some(vec![AggSpec::Max {
                field: "price".to_string()
            }])
        );
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_in_expression() {
    let cmd =
        plotql::parse("plot count of orders filter status in (\"paid\", \"pending\")").unwrap();
    if let Command::Query { where_clause, .. } = cmd {
        let filter = where_clause.expect("expected where clause");
        match filter {
            Expr::In { field, values } => {
                assert_eq!(field, "status");
                assert_eq!(values.len(), 2);
                assert_eq!(values[0], serde_json::json!("paid"));
                assert_eq!(values[1], serde_json::json!("pending"));
            }
            _ => panic!("unexpected expression"),
        }
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_not_expression() {
    let cmd = plotql::parse("plot count of orders filter not status=\"cancelled\"").unwrap();
    if let Command::Query { where_clause, .. } = cmd {
        let filter = where_clause.expect("expected where clause");
        match filter {
            Expr::Not(inner) => match *inner {
                Expr::Compare { field, op, value } => {
                    assert_eq!(field, "status");
                    assert_eq!(op, CompareOp::Eq);
                    assert_eq!(value, serde_json::json!("cancelled"));
                }
                _ => panic!("unexpected inner expression"),
            },
            _ => panic!("unexpected expression"),
        }
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_complex_filter_with_or_and_and() {
    let cmd = plotql::parse(
        "plot count of orders filter (status=\"paid\" or status=\"pending\") and price>100",
    )
    .unwrap();
    if let Command::Query { where_clause, .. } = cmd {
        let filter = where_clause.expect("expected where clause");
        match filter {
            Expr::And(left, right) => {
                match *left {
                    Expr::Or(..) => {}
                    _ => panic!("expected OR in left side"),
                }
                match *right {
                    Expr::Compare { field, op, .. } => {
                        assert_eq!(field, "price");
                        assert_eq!(op, CompareOp::Gt);
                    }
                    _ => panic!("unexpected right expression"),
                }
            }
            _ => panic!("unexpected expression tree"),
        }
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_compare_clause() {
    // Note: Compare clause is parsed but execution needs special handling
    let cmd = plotql::parse(
        "plot total(price) of refund_issued vs total(price) of payment_succeeded over month(created_at)",
    )
    .unwrap();
    if let Command::Query {
        event_type,
        aggs,
        time_bucket,
        ..
    } = cmd
    {
        assert_eq!(event_type, "refund_issued");
        assert_eq!(
            aggs,
            Some(vec![AggSpec::Total {
                field: "price".to_string()
            }])
        );
        assert_eq!(time_bucket, Some(TimeGranularity::Month));
        // Compare clause is stored but needs executor support
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_multiple_breakdown_fields() {
    let cmd =
        plotql::parse("plot total(price) of payment_succeeded breakdown by region, product_name")
            .unwrap();
    if let Command::Query { group_by, .. } = cmd {
        assert_eq!(
            group_by,
            Some(vec!["region".to_string(), "product_name".to_string()])
        );
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_qualified_field_names() {
    let cmd = plotql::parse("plot total(payment_succeeded.price) of payment_succeeded").unwrap();
    if let Command::Query { aggs, .. } = cmd {
        assert_eq!(
            aggs,
            Some(vec![AggSpec::Total {
                field: "payment_succeeded.price".to_string()
            }])
        );
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_all_time_granularities() {
    let cmd = plotql::parse("plot count of orders over hour(created_at)").unwrap();
    if let Command::Query { time_bucket, .. } = cmd {
        assert_eq!(time_bucket, Some(TimeGranularity::Hour));
    } else {
        panic!("expected query command");
    }

    let cmd = plotql::parse("plot count of orders over day(created_at)").unwrap();
    if let Command::Query { time_bucket, .. } = cmd {
        assert_eq!(time_bucket, Some(TimeGranularity::Day));
    } else {
        panic!("expected query command");
    }

    let cmd = plotql::parse("plot count of orders over week(created_at)").unwrap();
    if let Command::Query { time_bucket, .. } = cmd {
        assert_eq!(time_bucket, Some(TimeGranularity::Week));
    } else {
        panic!("expected query command");
    }

    let cmd = plotql::parse("plot count of orders over month(created_at)").unwrap();
    if let Command::Query { time_bucket, .. } = cmd {
        assert_eq!(time_bucket, Some(TimeGranularity::Month));
    } else {
        panic!("expected query command");
    }

    let cmd = plotql::parse("plot count of orders over year(created_at)").unwrap();
    if let Command::Query { time_bucket, .. } = cmd {
        assert_eq!(time_bucket, Some(TimeGranularity::Year));
    } else {
        panic!("expected query command");
    }
}

// ==========
// EDGE CASES: Identifier parsing with dashes
// ==========

#[test]
fn parses_identifiers_with_dashes_in_middle() {
    // Identifiers with dashes in the middle should work fine
    let cmd = plotql::parse(
        "plot count(user_id) of page-view->add-to-cart->payment-succeeded over day(created_at)",
    )
    .unwrap();
    if let Command::Query {
        event_type,
        event_sequence,
        ..
    } = cmd
    {
        assert_eq!(event_type, "page-view");
        let seq = event_sequence.expect("expected sequence");
        assert_eq!(seq.head.event, "page-view");
        assert_eq!(seq.links[0].1.event, "add-to-cart");
        assert_eq!(seq.links[1].1.event, "payment-succeeded");
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_identifiers_with_multiple_dashes() {
    // Multiple dashes should be fine as long as not trailing
    let cmd = plotql::parse("plot total(amount) of order-item-2024 over day(created_at)").unwrap();
    if let Command::Query { event_type, .. } = cmd {
        assert_eq!(event_type, "order-item-2024");
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_mixed_underscore_and_dash_identifiers() {
    // Mix of underscores and dashes should work
    let cmd = plotql::parse(
        "plot count(user_id) of page_view->add-to-cart->payment_succeeded over day(created_at)",
    )
    .unwrap();
    if let Command::Query {
        event_type,
        event_sequence,
        ..
    } = cmd
    {
        assert_eq!(event_type, "page_view");
        let seq = event_sequence.expect("expected sequence");
        assert_eq!(seq.head.event, "page_view");
        assert_eq!(seq.links[0].1.event, "add-to-cart");
        assert_eq!(seq.links[1].1.event, "payment_succeeded");
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_event_sequence_with_whitespace_around_arrow() {
    // Whitespace around -> should be handled
    let cmd = plotql::parse(
        "plot count(user_id) of page_view -> add_to_cart -> payment_succeeded over day(created_at)",
    )
    .unwrap();
    if let Command::Query {
        event_type,
        event_sequence,
        ..
    } = cmd
    {
        assert_eq!(event_type, "page_view");
        let seq = event_sequence.expect("expected sequence");
        assert_eq!(seq.links.len(), 2);
        assert_eq!(seq.links[0].1.event, "add_to_cart");
        assert_eq!(seq.links[1].1.event, "payment_succeeded");
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_long_event_sequence() {
    // Long sequences should work
    let cmd = plotql::parse(
        "plot count(user_id) of view->browse->add->cart->checkout->pay->complete over day(created_at)",
    )
    .unwrap();
    if let Command::Query {
        event_type,
        event_sequence,
        ..
    } = cmd
    {
        assert_eq!(event_type, "view");
        let seq = event_sequence.expect("expected sequence");
        assert_eq!(seq.links.len(), 6);
        assert_eq!(seq.links[0].1.event, "browse");
        assert_eq!(seq.links[5].1.event, "complete");
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_field_names_with_dashes() {
    // Field names with dashes should work
    let cmd = plotql::parse("plot total(order-amount) of payment_succeeded over day(created-at)")
        .unwrap();
    if let Command::Query {
        aggs, time_field, ..
    } = cmd
    {
        assert_eq!(
            aggs,
            Some(vec![AggSpec::Total {
                field: "order-amount".to_string()
            }])
        );
        assert_eq!(time_field, Some("created-at".to_string()));
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_breakdown_with_dashed_fields() {
    // Breakdown by fields with dashes should work
    let cmd = plotql::parse(
        "plot count of orders breakdown by product-category, order-status over day(created_at)",
    )
    .unwrap();
    if let Command::Query { group_by, .. } = cmd {
        assert_eq!(
            group_by,
            Some(vec![
                "product-category".to_string(),
                "order-status".to_string()
            ])
        );
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_filter_with_dashed_field_names() {
    // Filter fields with dashes should work
    let cmd =
        plotql::parse("plot count of orders filter order-status=\"paid\" over day(created_at)")
            .unwrap();
    if let Command::Query { where_clause, .. } = cmd {
        let filter = where_clause.expect("expected where clause");
        match filter {
            Expr::Compare { field, value, .. } => {
                assert_eq!(field, "order-status");
                assert_eq!(value, serde_json::json!("paid"));
            }
            _ => panic!("unexpected expression"),
        }
    } else {
        panic!("expected query command");
    }
}

#[test]
fn rejects_identifier_ending_with_dash() {
    // Identifiers ending with dash should be rejected
    let result = plotql::parse("plot count(user_id) of page_view- over day(created_at)");
    assert!(result.is_err(), "should reject identifier ending with dash");
}

#[test]
fn rejects_event_sequence_with_trailing_dash() {
    // Event sequences with trailing dash should be rejected
    let result =
        plotql::parse("plot count(user_id) of page-view->add-to-cart- over day(created_at)");
    assert!(result.is_err(), "should reject event name ending with dash");
}

#[test]
fn parses_single_char_identifiers() {
    // Single character identifiers should work
    let cmd = plotql::parse("plot count(x) of a->b->c over day(created_at)").unwrap();
    if let Command::Query {
        event_type,
        event_sequence,
        ..
    } = cmd
    {
        assert_eq!(event_type, "a");
        let seq = event_sequence.expect("expected sequence");
        assert_eq!(seq.links.len(), 2);
        assert_eq!(seq.links[0].1.event, "b");
        assert_eq!(seq.links[1].1.event, "c");
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_identifiers_starting_with_underscore() {
    // Identifiers starting with underscore should work
    let cmd =
        plotql::parse("plot count(_user_id) of _page_view->_add_to_cart over day(_created_at)")
            .unwrap();
    if let Command::Query {
        event_type,
        event_sequence,
        ..
    } = cmd
    {
        assert_eq!(event_type, "_page_view");
        let seq = event_sequence.expect("expected sequence");
        assert_eq!(seq.links[0].1.event, "_add_to_cart");
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_numeric_suffixes_after_dash() {
    // Numeric suffixes after dash should work (e.g., product-2024)
    let cmd = plotql::parse("plot count of order-2024->order-2025 over day(created_at)").unwrap();
    if let Command::Query {
        event_type,
        event_sequence,
        ..
    } = cmd
    {
        assert_eq!(event_type, "order-2024");
        let seq = event_sequence.expect("expected sequence");
        assert_eq!(seq.links[0].1.event, "order-2025");
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_complex_event_sequence_with_all_features() {
    // Complex query with all features and dashed identifiers
    let cmd = plotql::parse("plot count(user-id) of page-view->add-to-cart->checkout-started->payment-succeeded breakdown by product-category, order-status filter order-status=\"paid\" over week(created-at)").unwrap();
    if let Command::Query {
        event_type,
        aggs,
        group_by,
        where_clause,
        time_bucket,
        time_field,
        event_sequence,
        ..
    } = cmd
    {
        assert_eq!(event_type, "page-view");
        assert_eq!(
            aggs,
            Some(vec![AggSpec::CountField {
                field: "user-id".to_string()
            }])
        );
        assert_eq!(
            group_by,
            Some(vec![
                "product-category".to_string(),
                "order-status".to_string()
            ])
        );
        assert!(where_clause.is_some());
        assert_eq!(time_bucket, Some(TimeGranularity::Week));
        assert_eq!(time_field, Some("created-at".to_string()));
        let seq = event_sequence.expect("expected sequence");
        assert_eq!(seq.links.len(), 3);
        assert_eq!(seq.links[0].1.event, "add-to-cart");
        assert_eq!(seq.links[2].1.event, "payment-succeeded");
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_query_without_of_clause() {
    // Query without "of" clause should still parse (events will be empty)
    let cmd = plotql::parse("plot count").unwrap();
    if let Command::Query {
        event_type,
        aggs,
        event_sequence,
        ..
    } = cmd
    {
        assert_eq!(event_type, "*");
        assert_eq!(aggs, Some(vec![AggSpec::Count { unique_field: None }]));
        assert!(event_sequence.is_none());
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_comparison_operators() {
    let cmd = plotql::parse("plot count of orders filter price>100").unwrap();
    if let Command::Query { where_clause, .. } = cmd {
        match where_clause.unwrap() {
            Expr::Compare { op, .. } => assert_eq!(op, CompareOp::Gt),
            _ => panic!("expected comparison"),
        }
    } else {
        panic!("expected query command");
    }

    let cmd = plotql::parse("plot count of orders filter price>=100").unwrap();
    if let Command::Query { where_clause, .. } = cmd {
        match where_clause.unwrap() {
            Expr::Compare { op, .. } => assert_eq!(op, CompareOp::Gte),
            _ => panic!("expected comparison"),
        }
    } else {
        panic!("expected query command");
    }

    let cmd = plotql::parse("plot count of orders filter price<100").unwrap();
    if let Command::Query { where_clause, .. } = cmd {
        match where_clause.unwrap() {
            Expr::Compare { op, .. } => assert_eq!(op, CompareOp::Lt),
            _ => panic!("expected comparison"),
        }
    } else {
        panic!("expected query command");
    }

    let cmd = plotql::parse("plot count of orders filter price<=100").unwrap();
    if let Command::Query { where_clause, .. } = cmd {
        match where_clause.unwrap() {
            Expr::Compare { op, .. } => assert_eq!(op, CompareOp::Lte),
            _ => panic!("expected comparison"),
        }
    } else {
        panic!("expected query command");
    }

    let cmd = plotql::parse("plot count of orders filter price!=100").unwrap();
    if let Command::Query { where_clause, .. } = cmd {
        match where_clause.unwrap() {
            Expr::Compare { op, .. } => assert_eq!(op, CompareOp::Neq),
            _ => panic!("expected comparison"),
        }
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_numeric_values() {
    let cmd = plotql::parse("plot count of orders filter price=100").unwrap();
    if let Command::Query { where_clause, .. } = cmd {
        match where_clause.unwrap() {
            Expr::Compare { value, .. } => {
                assert_eq!(value, serde_json::json!(100));
            }
            _ => panic!("expected comparison"),
        }
    } else {
        panic!("expected query command");
    }

    let cmd = plotql::parse("plot count of orders filter price=100.5").unwrap();
    if let Command::Query { where_clause, .. } = cmd {
        match where_clause.unwrap() {
            Expr::Compare { value, .. } => {
                assert_eq!(value, serde_json::json!(100.5));
            }
            _ => panic!("expected comparison"),
        }
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_negative_numbers() {
    let cmd = plotql::parse("plot count of orders filter price=-100").unwrap();
    if let Command::Query { where_clause, .. } = cmd {
        match where_clause.unwrap() {
            Expr::Compare { value, .. } => {
                assert_eq!(value, serde_json::json!(-100));
            }
            _ => panic!("expected comparison"),
        }
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_case_insensitive_keywords() {
    // All keywords should be case-insensitive
    let cmd1 = plotql::parse("PLOT COUNT OF orders").unwrap();
    let cmd2 = plotql::parse("plot count of orders").unwrap();
    let cmd3 = plotql::parse("Plot Count Of orders").unwrap();

    // All should produce equivalent commands
    match (cmd1, cmd2, cmd3) {
        (
            Command::Query {
                aggs: a1,
                event_type: e1,
                ..
            },
            Command::Query {
                aggs: a2,
                event_type: e2,
                ..
            },
            Command::Query {
                aggs: a3,
                event_type: e3,
                ..
            },
        ) => {
            assert_eq!(a1, a2);
            assert_eq!(a2, a3);
            assert_eq!(e1, e2);
            assert_eq!(e2, e3);
        }
        _ => panic!("expected query commands"),
    }
}
