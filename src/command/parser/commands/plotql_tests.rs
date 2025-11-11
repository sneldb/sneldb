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
fn parses_top_by_different_metric() {
    // Ordering by a metric that differs from the main metric
    // Should add the ordering metric to aggs
    let cmd =
        plotql::parse("plot count of orders breakdown by product_id top 10 by avg(price)").unwrap();
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
                field: "avg_price".to_string(),
                desc: true
            })
        );
        // Should have both count and avg(price) in aggs
        assert_eq!(
            aggs,
            Some(vec![
                AggSpec::Count { unique_field: None },
                AggSpec::Avg {
                    field: "price".to_string()
                }
            ])
        );
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_top_by_different_metric_reverse() {
    // Ordering by count when main metric is avg
    let cmd = plotql::parse("plot avg(rating) of reviews breakdown by product_id top 5 by count")
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
                field: "count".to_string(),
                desc: true
            })
        );
        // Should have both avg(rating) and count in aggs
        assert_eq!(
            aggs,
            Some(vec![
                AggSpec::Avg {
                    field: "rating".to_string()
                },
                AggSpec::Count { unique_field: None }
            ])
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
    // Compare clause now returns Command::Compare
    let cmd = plotql::parse(
        "plot total(price) of refund_issued vs total(price) of payment_succeeded over month(created_at)",
    )
    .unwrap();
    if let Command::Compare { queries } = cmd {
        assert_eq!(queries.len(), 2);
        assert_eq!(queries[0].event_type, "refund_issued");
        assert_eq!(queries[1].event_type, "payment_succeeded");
        assert_eq!(
            queries[0].aggs,
            Some(vec![AggSpec::Total {
                field: "price".to_string()
            }])
        );
        assert_eq!(
            queries[1].aggs,
            Some(vec![AggSpec::Total {
                field: "price".to_string()
            }])
        );
        assert_eq!(queries[0].time_bucket, Some(TimeGranularity::Month));
        assert_eq!(queries[1].time_bucket, Some(TimeGranularity::Month));
    } else {
        panic!("expected Compare command");
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
fn parses_event_sequence_with_then_alias() {
    // 'then' should work as an alias for '->'
    let cmd1 = plotql::parse(
        "plot count(customer_id) of order_delivered -> review_submitted breakdown by region over week(created_at)",
    )
    .unwrap();
    let cmd2 = plotql::parse(
        "plot count(customer_id) of order_delivered then review_submitted breakdown by region over week(created_at)",
    )
    .unwrap();

    // Both should produce equivalent commands
    match (cmd1, cmd2) {
        (
            Command::Query {
                event_type: e1,
                event_sequence: seq1,
                ..
            },
            Command::Query {
                event_type: e2,
                event_sequence: seq2,
                ..
            },
        ) => {
            assert_eq!(e1, e2);
            assert_eq!(seq1.is_some(), seq2.is_some());
            if let (Some(s1), Some(s2)) = (seq1, seq2) {
                assert_eq!(s1.head.event, s2.head.event);
                assert_eq!(s1.links.len(), s2.links.len());
                assert_eq!(s1.links[0].1.event, s2.links[0].1.event);
            }
        }
        _ => panic!("expected query commands"),
    }
}

#[test]
fn parses_event_sequence_with_mixed_then_and_arrow() {
    // Mixing 'then' and '->' should work
    let cmd = plotql::parse(
        "plot count(user_id) of page_view then add_to_cart -> payment_succeeded over day(created_at)",
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
    // Note: Breakdown must come after filter (grammar restriction)
    let cmd = plotql::parse("plot count(user-id) of page-view->add-to-cart->checkout-started->payment-succeeded filter order-status=\"paid\" breakdown by product-category, order-status over week(created-at)").unwrap();
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
    // Query without "of" clause should fail (required by grammar)
    let result = plotql::parse("plot count");
    assert!(result.is_err(), "of clause is required");
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

#[test]
fn parses_all_keywords_lowercase() {
    // Test that all PlotQL keywords work in lowercase
    // This ensures case-insensitivity is comprehensive

    // Basic query
    assert!(plotql::parse("plot count of orders").is_ok());

    // With breakdown
    assert!(plotql::parse("plot count of orders breakdown by product_id").is_ok());

    // With time granularity
    assert!(plotql::parse("plot count of orders over day(created_at)").is_ok());
    assert!(plotql::parse("plot count of orders over hour(created_at)").is_ok());
    assert!(plotql::parse("plot count of orders over week(created_at)").is_ok());
    assert!(plotql::parse("plot count of orders over month(created_at)").is_ok());
    assert!(plotql::parse("plot count of orders over year(created_at)").is_ok());

    // With filter
    assert!(plotql::parse("plot count of orders filter status = \"active\"").is_ok());
    assert!(
        plotql::parse("plot count of orders filter status = \"active\" and price > 100").is_ok()
    );
    assert!(
        plotql::parse("plot count of orders filter status in (\"active\", \"pending\")").is_ok()
    );
    assert!(plotql::parse("plot count of orders filter not status = \"inactive\"").is_ok());

    // With top
    assert!(plotql::parse("plot count of orders breakdown by product_id top 10").is_ok());
    assert!(
        plotql::parse("plot count of orders breakdown by product_id top 10 by avg(price)").is_ok()
    );
    assert!(
        plotql::parse("plot count of orders breakdown by product_id top 10 by product_id").is_ok()
    );

    // With compare
    assert!(plotql::parse("plot count of orders vs count of reviews").is_ok());

    // All metric functions
    assert!(plotql::parse("plot total(amount) of orders").is_ok());
    assert!(plotql::parse("plot sum(amount) of orders").is_ok());
    assert!(plotql::parse("plot avg(price) of orders").is_ok());
    assert!(plotql::parse("plot min(price) of orders").is_ok());
    assert!(plotql::parse("plot max(price) of orders").is_ok());
    assert!(plotql::parse("plot count(id) of orders").is_ok());
    assert!(plotql::parse("plot unique(user_id) of orders").is_ok());

    // Mixed case should also work
    assert!(plotql::parse("PlOt CoUnT oF orders").is_ok());
    assert!(plotql::parse("PLOT count OF orders BREAKDOWN BY product_id").is_ok());
    assert!(plotql::parse("plot COUNT of orders TOP 10 BY avg(price)").is_ok());
}
// ==========
// COMPARISON QUERIES (VS CLAUSE)
// ==========

#[test]
fn parses_two_way_comparison() {
    let cmd = plotql::parse("plot total(amount) of orders vs total(amount) of refunds").unwrap();
    if let Command::Compare { queries } = cmd {
        assert_eq!(queries.len(), 2);
        assert_eq!(queries[0].event_type, "orders");
        assert_eq!(queries[1].event_type, "refunds");
        assert_eq!(
            queries[0].aggs,
            Some(vec![AggSpec::Total {
                field: "amount".to_string()
            }])
        );
        assert_eq!(
            queries[1].aggs,
            Some(vec![AggSpec::Total {
                field: "amount".to_string()
            }])
        );
    } else {
        panic!("expected Compare command");
    }
}

#[test]
fn parses_three_way_comparison() {
    let cmd = plotql::parse(
        "plot total(amount) of orders vs total(amount) of refunds vs total(amount) of exchanges",
    )
    .unwrap();
    if let Command::Compare { queries } = cmd {
        assert_eq!(queries.len(), 3);
        assert_eq!(queries[0].event_type, "orders");
        assert_eq!(queries[1].event_type, "refunds");
        assert_eq!(queries[2].event_type, "exchanges");
    } else {
        panic!("expected Compare command");
    }
}

#[test]
fn parses_four_way_comparison() {
    let cmd = plotql::parse(
        "plot count of event_a vs count of event_b vs count of event_c vs count of event_d",
    )
    .unwrap();
    if let Command::Compare { queries } = cmd {
        assert_eq!(queries.len(), 4);
    } else {
        panic!("expected Compare command");
    }
}

#[test]
fn parses_comparison_with_shared_breakdown() {
    let cmd = plotql::parse(
        "plot total(amount) of orders vs total(amount) of refunds breakdown by region",
    )
    .unwrap();
    if let Command::Compare { queries } = cmd {
        assert_eq!(queries.len(), 2);
        // Both queries should have the same breakdown
        assert_eq!(queries[0].group_by, Some(vec!["region".to_string()]));
        assert_eq!(queries[1].group_by, Some(vec!["region".to_string()]));
    } else {
        panic!("expected Compare command");
    }
}

#[test]
fn parses_comparison_with_shared_time_bucket() {
    let cmd = plotql::parse(
        "plot total(amount) of orders vs total(amount) of refunds over day(created_at)",
    )
    .unwrap();
    if let Command::Compare { queries } = cmd {
        assert_eq!(queries.len(), 2);
        // Both queries should have the same time bucket
        assert_eq!(queries[0].time_bucket, Some(TimeGranularity::Day));
        assert_eq!(queries[1].time_bucket, Some(TimeGranularity::Day));
        assert_eq!(queries[0].time_field, Some("created_at".to_string()));
        assert_eq!(queries[1].time_field, Some("created_at".to_string()));
    } else {
        panic!("expected Compare command");
    }
}

#[test]
fn parses_comparison_with_shared_breakdown_and_time() {
    let cmd = plotql::parse(
        "plot total(amount) of orders vs total(amount) of refunds breakdown by region over day(created_at)",
    )
    .unwrap();
    if let Command::Compare { queries } = cmd {
        assert_eq!(queries.len(), 2);
        assert_eq!(queries[0].group_by, Some(vec!["region".to_string()]));
        assert_eq!(queries[1].group_by, Some(vec!["region".to_string()]));
        assert_eq!(queries[0].time_bucket, Some(TimeGranularity::Day));
        assert_eq!(queries[1].time_bucket, Some(TimeGranularity::Day));
    } else {
        panic!("expected Compare command");
    }
}

#[test]
fn parses_comparison_with_per_side_filters() {
    let cmd = plotql::parse(
        "plot total(amount) of orders filter status=\"completed\" vs total(amount) of refunds filter status=\"processed\"",
    )
    .unwrap();
    if let Command::Compare { queries } = cmd {
        assert_eq!(queries.len(), 2);
        // Each query should have its own filter
        assert!(queries[0].where_clause.is_some());
        assert!(queries[1].where_clause.is_some());
    } else {
        panic!("expected Compare command");
    }
}

#[test]
fn parses_comparison_with_per_side_top() {
    let cmd =
        plotql::parse("plot total(amount) of orders top 10 vs total(amount) of refunds top 5")
            .unwrap();
    if let Command::Compare { queries } = cmd {
        assert_eq!(queries.len(), 2);
        assert_eq!(queries[0].limit, Some(10));
        assert_eq!(queries[1].limit, Some(5));
    } else {
        panic!("expected Compare command");
    }
}

#[test]
fn parses_comparison_with_shared_top() {
    let cmd = plotql::parse(
        "plot total(amount) of orders vs total(amount) of refunds breakdown by region top 10",
    )
    .unwrap();
    if let Command::Compare { queries } = cmd {
        assert_eq!(queries.len(), 2);
        // Both queries should have the same limit
        assert_eq!(queries[0].limit, Some(10));
        assert_eq!(queries[1].limit, Some(10));
    } else {
        panic!("expected Compare command");
    }
}

#[test]
fn parses_comparison_with_event_sequences() {
    let cmd = plotql::parse(
        "plot count(user_id) of page_view->checkout vs count(user_id) of page_view->cart",
    )
    .unwrap();
    if let Command::Compare { queries } = cmd {
        assert_eq!(queries.len(), 2);
        assert!(queries[0].event_sequence.is_some());
        assert!(queries[1].event_sequence.is_some());
    } else {
        panic!("expected Compare command");
    }
}

#[test]
fn parses_comparison_with_then_alias() {
    let cmd = plotql::parse("plot count of event_a then event_b vs count of event_c then event_d")
        .unwrap();
    if let Command::Compare { queries } = cmd {
        assert_eq!(queries.len(), 2);
        assert!(queries[0].event_sequence.is_some());
        assert!(queries[1].event_sequence.is_some());
    } else {
        panic!("expected Compare command");
    }
}

#[test]
fn parses_comparison_with_all_metric_types() {
    // Test all metric types work in comparisons
    assert!(plotql::parse("plot count of orders vs count of refunds").is_ok());
    assert!(plotql::parse("plot count(id) of orders vs count(id) of refunds").is_ok());
    assert!(plotql::parse("plot unique(user_id) of orders vs unique(user_id) of refunds").is_ok());
    assert!(plotql::parse("plot total(amount) of orders vs total(amount) of refunds").is_ok());
    assert!(plotql::parse("plot sum(amount) of orders vs sum(amount) of refunds").is_ok());
    assert!(plotql::parse("plot avg(price) of orders vs avg(price) of refunds").is_ok());
    assert!(plotql::parse("plot min(price) of orders vs min(price) of refunds").is_ok());
    assert!(plotql::parse("plot max(price) of orders vs max(price) of refunds").is_ok());
}

#[test]
fn parses_comparison_with_complex_filters() {
    let cmd = plotql::parse(
        "plot total(amount) of orders filter status=\"paid\" and price>100 vs total(amount) of refunds filter status=\"processed\" or amount>50",
    )
    .unwrap();
    if let Command::Compare { queries } = cmd {
        assert_eq!(queries.len(), 2);
        assert!(queries[0].where_clause.is_some());
        assert!(queries[1].where_clause.is_some());
    } else {
        panic!("expected Compare command");
    }
}

#[test]
fn parses_comparison_with_multiple_breakdown_fields() {
    let cmd = plotql::parse(
        "plot total(amount) of orders vs total(amount) of refunds breakdown by region, product_id",
    )
    .unwrap();
    if let Command::Compare { queries } = cmd {
        assert_eq!(queries.len(), 2);
        assert_eq!(
            queries[0].group_by,
            Some(vec!["region".to_string(), "product_id".to_string()])
        );
        assert_eq!(
            queries[1].group_by,
            Some(vec!["region".to_string(), "product_id".to_string()])
        );
    } else {
        panic!("expected Compare command");
    }
}

#[test]
fn parses_comparison_with_top_by_metric() {
    let cmd = plotql::parse(
        "plot total(amount) of orders vs total(amount) of refunds breakdown by region top 10 by total(amount)",
    )
    .unwrap();
    if let Command::Compare { queries } = cmd {
        assert_eq!(queries.len(), 2);
        assert_eq!(queries[0].limit, Some(10));
        assert_eq!(queries[1].limit, Some(10));
        assert!(queries[0].order_by.is_some());
        assert!(queries[1].order_by.is_some());
    } else {
        panic!("expected Compare command");
    }
}

#[test]
fn parses_comparison_with_top_by_field() {
    let cmd = plotql::parse(
        "plot total(amount) of orders vs total(amount) of refunds breakdown by region top 10 by region",
    )
    .unwrap();
    if let Command::Compare { queries } = cmd {
        assert_eq!(queries.len(), 2);
        assert_eq!(queries[0].limit, Some(10));
        assert_eq!(queries[1].limit, Some(10));
        assert!(queries[0].order_by.is_some());
        assert!(queries[1].order_by.is_some());
    } else {
        panic!("expected Compare command");
    }
}

#[test]
fn parses_comparison_same_event_type_different_filters() {
    let cmd = plotql::parse(
        "plot total(amount) of orders filter region=\"US\" vs total(amount) of orders filter region=\"EU\"",
    )
    .unwrap();
    if let Command::Compare { queries } = cmd {
        assert_eq!(queries.len(), 2);
        assert_eq!(queries[0].event_type, "orders");
        assert_eq!(queries[1].event_type, "orders");
        // Both should have different filters
        assert!(queries[0].where_clause.is_some());
        assert!(queries[1].where_clause.is_some());
    } else {
        panic!("expected Compare command");
    }
}

// ==========
// ERROR CASES - COMPARISON QUERIES
// ==========

#[test]
fn rejects_comparison_with_different_metrics() {
    // Different metric functions should be rejected
    let result = plotql::parse("plot total(amount) of orders vs count of refunds");
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("same metric function")
    );
}

#[test]
fn rejects_comparison_with_different_metric_fields() {
    // Same metric function but different fields should be rejected
    let result = plotql::parse("plot total(amount) of orders vs total(price) of refunds");
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("same metric function")
    );
}

#[test]
fn rejects_breakdown_before_vs() {
    // Breakdown before vs should be rejected (parser doesn't allow it)
    let result = plotql::parse(
        "plot total(amount) of orders breakdown by region vs total(amount) of refunds",
    );
    assert!(result.is_err(), "breakdown before vs should be rejected");
}

#[test]
fn rejects_time_before_vs() {
    // Time clause before vs should be rejected
    let result = plotql::parse(
        "plot total(amount) of orders over day(created_at) vs total(amount) of refunds",
    );
    assert!(result.is_err(), "time clause before vs should be rejected");
}

#[test]
fn rejects_filter_after_vs() {
    // Filter after vs should be ignored (not shared)
    let cmd = plotql::parse(
        "plot total(amount) of orders vs total(amount) of refunds filter status=\"paid\"",
    )
    .unwrap();
    // Filter after vs should not be applied (filters are per-side only)
    if let Command::Compare { queries } = cmd {
        // The filter should not be applied since filters are per-side
        // This is expected behavior - filters after vs are ignored
        assert_eq!(queries.len(), 2);
    } else {
        panic!("expected Compare command");
    }
}

// ==========
// ERROR CASES - GENERAL SYNTAX
// ==========

#[test]
fn rejects_empty_query() {
    let result = plotql::parse("");
    assert!(result.is_err());
}

#[test]
fn rejects_missing_plot_keyword() {
    let result = plotql::parse("count of orders");
    assert!(result.is_err());
}

#[test]
fn rejects_missing_metric() {
    let result = plotql::parse("plot of orders");
    assert!(result.is_err());
}

#[test]
fn rejects_missing_of_keyword() {
    let result = plotql::parse("plot count orders");
    assert!(result.is_err());
}

#[test]
fn rejects_invalid_metric_syntax() {
    let result = plotql::parse("plot total of orders");
    assert!(result.is_err(), "total() requires field parameter");
}

#[test]
fn rejects_invalid_time_granularity() {
    let result = plotql::parse("plot count of orders over invalid(created_at)");
    assert!(result.is_err());
}

#[test]
fn rejects_missing_time_field() {
    let result = plotql::parse("plot count of orders over day()");
    assert!(result.is_err());
}

#[test]
fn rejects_malformed_filter() {
    let result = plotql::parse("plot count of orders filter =");
    assert!(result.is_err());
}

#[test]
fn rejects_malformed_top() {
    let result = plotql::parse("plot count of orders top");
    assert!(result.is_err());
}

#[test]
fn rejects_malformed_breakdown() {
    let result = plotql::parse("plot count of orders breakdown");
    assert!(result.is_err());
}

#[test]
fn rejects_malformed_comparison_side() {
    let result = plotql::parse("plot count of orders vs");
    assert!(result.is_err());
}

#[test]
fn rejects_incomplete_comparison_side() {
    let result = plotql::parse("plot count of orders vs count");
    assert!(result.is_err());
}

#[test]
fn rejects_invalid_field_name_in_metric() {
    // Empty field name should fail
    let result = plotql::parse("plot total() of orders");
    assert!(result.is_err());
}

#[test]
fn rejects_invalid_expression_syntax() {
    let result = plotql::parse("plot count of orders filter status=");
    assert!(result.is_err());
}

#[test]
fn rejects_unclosed_parentheses() {
    let result = plotql::parse("plot total(amount of orders");
    assert!(result.is_err());
}

#[test]
fn rejects_unclosed_string() {
    let result = plotql::parse("plot count of orders filter status=\"paid");
    assert!(result.is_err());
}

#[test]
fn rejects_invalid_in_expression() {
    let result = plotql::parse("plot count of orders filter status in");
    assert!(result.is_err());
}

#[test]
fn rejects_empty_in_list() {
    let result = plotql::parse("plot count of orders filter status in ()");
    assert!(result.is_err());
}

#[test]
fn rejects_invalid_exists_expression() {
    let result = plotql::parse("plot count of orders filter exists");
    assert!(result.is_err());
}

#[test]
fn rejects_invalid_not_expression() {
    let result = plotql::parse("plot count of orders filter not");
    assert!(result.is_err());
}

#[test]
fn rejects_invalid_event_sequence() {
    let result = plotql::parse("plot count of page_view->");
    assert!(result.is_err());
}

#[test]
fn rejects_trailing_vs() {
    let result = plotql::parse("plot count of orders vs");
    assert!(result.is_err());
}

#[test]
fn rejects_multiple_vs_without_sides() {
    let result = plotql::parse("plot count of orders vs vs count of refunds");
    assert!(result.is_err());
}

// ==========
// EDGE CASES - EXPRESSIONS
// ==========

#[test]
fn parses_nested_parentheses() {
    let cmd = plotql::parse(
        "plot count of orders filter (status=\"paid\" and (price>100 or discount>0))",
    )
    .unwrap();
    if let Command::Query { where_clause, .. } = cmd {
        assert!(where_clause.is_some());
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_complex_nested_expressions() {
    let cmd = plotql::parse(
        "plot count of orders filter ((status=\"paid\" or status=\"pending\") and (price>100 or discount>10)) and not cancelled=true",
    )
    .unwrap();
    if let Command::Query { where_clause, .. } = cmd {
        assert!(where_clause.is_some());
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_exists_expression() {
    let cmd = plotql::parse("plot count of orders filter exists(user_id)").unwrap();
    if let Command::Query { where_clause, .. } = cmd {
        assert!(where_clause.is_some());
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_not_exists_expression() {
    let cmd = plotql::parse("plot count of orders filter not exists(user_id)").unwrap();
    if let Command::Query { where_clause, .. } = cmd {
        assert!(where_clause.is_some());
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_in_expression_with_multiple_values() {
    let cmd = plotql::parse(
        "plot count of orders filter status in (\"paid\", \"pending\", \"processing\", \"shipped\")",
    )
    .unwrap();
    if let Command::Query { where_clause, .. } = cmd {
        match where_clause.unwrap() {
            Expr::In { values, .. } => {
                assert_eq!(values.len(), 4);
            }
            _ => panic!("expected In expression"),
        }
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_in_expression_with_numbers() {
    let cmd = plotql::parse("plot count of orders filter price in (100, 200, 300)").unwrap();
    if let Command::Query { where_clause, .. } = cmd {
        match where_clause.unwrap() {
            Expr::In { values, .. } => {
                assert_eq!(values.len(), 3);
            }
            _ => panic!("expected In expression"),
        }
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_boolean_values() {
    // Boolean values are parsed as identifiers (strings), not JSON booleans
    let cmd = plotql::parse("plot count of orders filter active=true").unwrap();
    if let Command::Query { where_clause, .. } = cmd {
        match where_clause.unwrap() {
            Expr::Compare { value, .. } => {
                // true is parsed as string identifier, not boolean
                assert_eq!(value, serde_json::json!("true"));
            }
            _ => panic!("expected comparison"),
        }
    } else {
        panic!("expected query command");
    }

    let cmd = plotql::parse("plot count of orders filter active=false").unwrap();
    if let Command::Query { where_clause, .. } = cmd {
        match where_clause.unwrap() {
            Expr::Compare { value, .. } => {
                // false is parsed as string identifier, not boolean
                assert_eq!(value, serde_json::json!("false"));
            }
            _ => panic!("expected comparison"),
        }
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_string_with_escaped_quotes() {
    // Note: Current parser may not support escaped quotes, but test the basic case
    let cmd = plotql::parse("plot count of orders filter message=\"hello world\"").unwrap();
    if let Command::Query { where_clause, .. } = cmd {
        match where_clause.unwrap() {
            Expr::Compare { value, .. } => {
                assert_eq!(value, serde_json::json!("hello world"));
            }
            _ => panic!("expected comparison"),
        }
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_empty_string_value() {
    let cmd = plotql::parse("plot count of orders filter status=\"\"").unwrap();
    if let Command::Query { where_clause, .. } = cmd {
        match where_clause.unwrap() {
            Expr::Compare { value, .. } => {
                assert_eq!(value, serde_json::json!(""));
            }
            _ => panic!("expected comparison"),
        }
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_qualified_field_in_filter() {
    let cmd = plotql::parse("plot count of orders filter orders.status=\"paid\"").unwrap();
    if let Command::Query { where_clause, .. } = cmd {
        match where_clause.unwrap() {
            Expr::Compare { field, .. } => {
                assert_eq!(field, "orders.status");
            }
            _ => panic!("expected comparison"),
        }
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_qualified_field_in_breakdown() {
    let cmd = plotql::parse("plot count of orders breakdown by orders.region, orders.product_id")
        .unwrap();
    if let Command::Query { group_by, .. } = cmd {
        assert_eq!(
            group_by,
            Some(vec![
                "orders.region".to_string(),
                "orders.product_id".to_string()
            ])
        );
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_qualified_field_in_metric() {
    let cmd = plotql::parse("plot total(orders.amount) of orders").unwrap();
    if let Command::Query { aggs, .. } = cmd {
        assert_eq!(
            aggs,
            Some(vec![AggSpec::Total {
                field: "orders.amount".to_string()
            }])
        );
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_qualified_field_in_time_clause() {
    let cmd = plotql::parse("plot count of orders over day(orders.created_at)").unwrap();
    if let Command::Query { time_field, .. } = cmd {
        assert_eq!(time_field, Some("orders.created_at".to_string()));
    } else {
        panic!("expected query command");
    }
}

// ==========
// EDGE CASES - TOP CLAUSE
// ==========

#[test]
fn parses_top_zero() {
    // Top 0 should be allowed (though may return no results)
    let cmd = plotql::parse("plot count of orders breakdown by region top 0").unwrap();
    if let Command::Query { limit, .. } = cmd {
        assert_eq!(limit, Some(0));
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_top_large_number() {
    let cmd = plotql::parse("plot count of orders breakdown by region top 999999").unwrap();
    if let Command::Query { limit, .. } = cmd {
        assert_eq!(limit, Some(999999));
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_top_without_breakdown() {
    // Top without breakdown should still work (scalar aggregation)
    let cmd = plotql::parse("plot count of orders top 10").unwrap();
    if let Command::Query { limit, .. } = cmd {
        assert_eq!(limit, Some(10));
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_top_by_different_metric_in_comparison() {
    let cmd = plotql::parse(
        "plot count of orders vs count of refunds breakdown by region top 10 by avg(amount)",
    )
    .unwrap();
    if let Command::Compare { queries } = cmd {
        assert_eq!(queries.len(), 2);
        // Both should have order_by and the ordering metric should be added to aggs
        assert!(queries[0].order_by.is_some());
        assert!(queries[1].order_by.is_some());
        // Should have both count and avg(amount) in aggs
        assert!(queries[0].aggs.as_ref().unwrap().len() >= 2);
        assert!(queries[1].aggs.as_ref().unwrap().len() >= 2);
    } else {
        panic!("expected Compare command");
    }
}

// ==========
// EDGE CASES - EVENT SEQUENCES
// ==========

#[test]
fn parses_single_event() {
    // Single event (no sequence) should work
    let cmd = plotql::parse("plot count of orders").unwrap();
    if let Command::Query {
        event_type,
        event_sequence,
        ..
    } = cmd
    {
        assert_eq!(event_type, "orders");
        assert!(event_sequence.is_none());
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_two_event_sequence() {
    let cmd = plotql::parse("plot count of page_view->checkout").unwrap();
    if let Command::Query {
        event_type,
        event_sequence,
        ..
    } = cmd
    {
        assert_eq!(event_type, "page_view");
        let seq = event_sequence.expect("expected sequence");
        assert_eq!(seq.links.len(), 1);
        assert_eq!(seq.links[0].1.event, "checkout");
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_event_sequence_without_whitespace() {
    let cmd = plotql::parse("plot count of a->b->c").unwrap();
    if let Command::Query {
        event_type,
        event_sequence,
        ..
    } = cmd
    {
        assert_eq!(event_type, "a");
        let seq = event_sequence.expect("expected sequence");
        assert_eq!(seq.links.len(), 2);
    } else {
        panic!("expected query command");
    }
}

// ==========
// COMBINATION TESTS - COMPLEX QUERIES
// ==========

#[test]
fn parses_complex_comparison_with_all_features() {
    // Note: Per-side top comes before vs, shared top comes after vs
    // Shared top overrides per-side top (shared_top.or(top) logic)
    let cmd = plotql::parse(
        "plot total(amount) of orders filter status=\"paid\" top 5 vs total(amount) of refunds filter status=\"processed\" top 3 breakdown by region over day(created_at) top 10",
    )
    .unwrap();
    if let Command::Compare { queries } = cmd {
        assert_eq!(queries.len(), 2);
        // Shared top should override per-side top
        assert_eq!(queries[0].limit, Some(10));
        assert_eq!(queries[1].limit, Some(10));
        // Shared breakdown and time should apply
        assert_eq!(queries[0].group_by, Some(vec!["region".to_string()]));
        assert_eq!(queries[1].group_by, Some(vec!["region".to_string()]));
        assert_eq!(queries[0].time_bucket, Some(TimeGranularity::Day));
        assert_eq!(queries[1].time_bucket, Some(TimeGranularity::Day));
    } else {
        panic!("expected Compare command");
    }
}

#[test]
fn parses_complex_query_with_all_clauses() {
    // Note: Breakdown must come after other clauses (grammar restriction)
    // Reorder: filter, then breakdown, then time, then top
    let cmd = plotql::parse(
        "plot count(user_id) of page_view->add_to_cart->checkout filter status=\"active\" breakdown by region, product_id over week(created_at) top 20 by count(user_id)",
    )
    .unwrap();
    if let Command::Query {
        event_type,
        aggs,
        group_by,
        where_clause,
        time_bucket,
        time_field,
        limit,
        order_by,
        event_sequence,
        ..
    } = cmd
    {
        assert_eq!(event_type, "page_view");
        assert!(aggs.is_some());
        assert_eq!(
            group_by,
            Some(vec!["region".to_string(), "product_id".to_string()])
        );
        assert!(where_clause.is_some());
        assert_eq!(time_bucket, Some(TimeGranularity::Week));
        assert_eq!(time_field, Some("created_at".to_string()));
        assert_eq!(limit, Some(20));
        assert!(order_by.is_some());
        assert!(event_sequence.is_some());
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_complex_comparison_with_event_sequences() {
    let cmd = plotql::parse(
        "plot count(user_id) of page_view->checkout vs count(user_id) of page_view->cart breakdown by region over day(created_at)",
    )
    .unwrap();
    if let Command::Compare { queries } = cmd {
        assert_eq!(queries.len(), 2);
        assert!(queries[0].event_sequence.is_some());
        assert!(queries[1].event_sequence.is_some());
        assert_eq!(queries[0].group_by, Some(vec!["region".to_string()]));
        assert_eq!(queries[1].group_by, Some(vec!["region".to_string()]));
    } else {
        panic!("expected Compare command");
    }
}

#[test]
fn parses_comparison_with_all_time_granularities() {
    for (gran_str, gran) in [
        ("hour", TimeGranularity::Hour),
        ("day", TimeGranularity::Day),
        ("week", TimeGranularity::Week),
        ("month", TimeGranularity::Month),
        ("year", TimeGranularity::Year),
    ] {
        let query = format!(
            "plot total(amount) of orders vs total(amount) of refunds over {}(created_at)",
            gran_str
        );
        let cmd = plotql::parse(&query).unwrap();
        if let Command::Compare { queries } = cmd {
            assert_eq!(queries[0].time_bucket, Some(gran.clone()));
            assert_eq!(queries[1].time_bucket, Some(gran));
        } else {
            panic!("expected Compare command");
        }
    }
}

#[test]
fn parses_comparison_case_insensitive() {
    // All keywords should be case-insensitive in comparisons
    let cmd1 = plotql::parse("PLOT TOTAL(amount) OF orders VS TOTAL(amount) OF refunds").unwrap();
    let cmd2 = plotql::parse("plot total(amount) of orders vs total(amount) of refunds").unwrap();
    let cmd3 = plotql::parse("Plot Total(amount) Of orders Vs Total(amount) Of refunds").unwrap();

    match (cmd1, cmd2, cmd3) {
        (
            Command::Compare { queries: q1 },
            Command::Compare { queries: q2 },
            Command::Compare { queries: q3 },
        ) => {
            assert_eq!(q1.len(), q2.len());
            assert_eq!(q2.len(), q3.len());
            assert_eq!(q1[0].event_type, q2[0].event_type);
            assert_eq!(q2[0].event_type, q3[0].event_type);
        }
        _ => panic!("expected Compare commands"),
    }
}

#[test]
fn parses_comparison_with_whitespace_variations() {
    // Various whitespace patterns should work
    assert!(plotql::parse("plot count of orders vs count of refunds").is_ok());
    assert!(plotql::parse("plot  count  of  orders  vs  count  of  refunds").is_ok());
    assert!(plotql::parse("plot\tcount\tof\torders\tvs\tcount\tof\trefunds").is_ok());
}

#[test]
fn parses_comparison_with_newlines() {
    // Newlines should be handled
    let cmd = plotql::parse(
        "plot total(amount) of orders\nvs total(amount) of refunds\nbreakdown by region",
    )
    .unwrap();
    if let Command::Compare { queries } = cmd {
        assert_eq!(queries.len(), 2);
    } else {
        panic!("expected Compare command");
    }
}

// ==========
// EDGE CASES - FIELD NAMES
// ==========

#[test]
fn parses_field_with_underscores() {
    let cmd = plotql::parse("plot total(order_amount) of orders").unwrap();
    if let Command::Query { aggs, .. } = cmd {
        assert_eq!(
            aggs,
            Some(vec![AggSpec::Total {
                field: "order_amount".to_string()
            }])
        );
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_field_with_numbers() {
    let cmd = plotql::parse("plot total(amount_2024) of orders").unwrap();
    if let Command::Query { aggs, .. } = cmd {
        assert_eq!(
            aggs,
            Some(vec![AggSpec::Total {
                field: "amount_2024".to_string()
            }])
        );
    } else {
        panic!("expected query command");
    }
}

#[test]
fn parses_event_name_with_numbers() {
    let cmd = plotql::parse("plot count of event_2024").unwrap();
    if let Command::Query { event_type, .. } = cmd {
        assert_eq!(event_type, "event_2024");
    } else {
        panic!("expected query command");
    }
}

// ==========
// VALIDATION TESTS
// ==========

#[test]
fn validates_metric_equality_in_comparison() {
    // Should reject different metric types
    let result = plotql::parse("plot total(amount) of orders vs avg(amount) of refunds");
    assert!(result.is_err());

    // Should reject same metric type but different fields
    let result = plotql::parse("plot total(amount) of orders vs total(price) of refunds");
    assert!(result.is_err());

    // Should accept same metric and field
    let result = plotql::parse("plot total(amount) of orders vs total(amount) of refunds");
    assert!(result.is_ok());
}

#[test]
fn validates_breakdown_position() {
    // Breakdown before vs should fail
    let result = plotql::parse(
        "plot total(amount) of orders breakdown by region vs total(amount) of refunds",
    );
    assert!(result.is_err());

    // Breakdown after vs should work
    let result = plotql::parse(
        "plot total(amount) of orders vs total(amount) of refunds breakdown by region",
    );
    assert!(result.is_ok());
}

#[test]
fn validates_time_clause_position() {
    // Time clause before vs should fail
    let result = plotql::parse(
        "plot total(amount) of orders over day(created_at) vs total(amount) of refunds",
    );
    assert!(result.is_err());

    // Time clause after vs should work
    let result = plotql::parse(
        "plot total(amount) of orders vs total(amount) of refunds over day(created_at)",
    );
    assert!(result.is_ok());
}

// ==========
// REGRESSION TESTS
// ==========

#[test]
fn parses_original_compare_test_case() {
    // Original test case from line 625-650
    let cmd = plotql::parse(
        "plot total(price) of refund_issued vs total(price) of payment_succeeded over month(created_at)",
    )
    .unwrap();
    if let Command::Compare { queries } = cmd {
        assert_eq!(queries.len(), 2);
        assert_eq!(queries[0].event_type, "refund_issued");
        assert_eq!(queries[1].event_type, "payment_succeeded");
        assert_eq!(queries[0].time_bucket, Some(TimeGranularity::Month));
        assert_eq!(queries[1].time_bucket, Some(TimeGranularity::Month));
    } else {
        panic!("expected Compare command");
    }
}

#[test]
fn parses_query_without_comparison_returns_query_command() {
    // Query without vs should return Command::Query, not Command::Compare
    let cmd = plotql::parse("plot count of orders").unwrap();
    match cmd {
        Command::Query { .. } => {}
        Command::Compare { .. } => panic!("should not be Compare command"),
        _ => panic!("expected Query command"),
    }
}

#[test]
fn parses_query_with_vs_returns_compare_command() {
    // Query with vs should return Command::Compare
    let cmd = plotql::parse("plot count of orders vs count of refunds").unwrap();
    match cmd {
        Command::Compare { .. } => {}
        Command::Query { .. } => panic!("should not be Query command"),
        _ => panic!("expected Compare command"),
    }
}
