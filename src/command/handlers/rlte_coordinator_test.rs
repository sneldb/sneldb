use super::rlte_coordinator::RlteCoordinator;
use crate::command::types::{Command, OrderSpec};

#[test]
fn should_plan_returns_true_with_order_by() {
    let cmd = Command::Query {
        event_type: "click".to_string(),
        context_id: None,
        since: None,
        time_field: None,
        where_clause: None,
        limit: Some(10),
        offset: None,
        order_by: Some(OrderSpec {
            field: "timestamp".to_string(),
            desc: false,
        }),
        picked_zones: None,
        return_fields: None,
        link_field: None,
        aggs: None,
        time_bucket: None,
        group_by: None,
        event_sequence: None,
    };

    assert!(RlteCoordinator::should_plan(&cmd));
}

#[test]
fn should_plan_returns_false_without_order_by() {
    let cmd = Command::Query {
        event_type: "click".to_string(),
        context_id: None,
        since: None,
        time_field: None,
        where_clause: None,
        limit: Some(10),
        offset: None,
        order_by: None,
        picked_zones: None,
        return_fields: None,
        link_field: None,
        aggs: None,
        time_bucket: None,
        group_by: None,
        event_sequence: None,
    };

    assert!(!RlteCoordinator::should_plan(&cmd));
}

#[test]
fn should_plan_returns_false_for_non_query() {
    let cmd = Command::Ping;
    assert!(!RlteCoordinator::should_plan(&cmd));

    let cmd = Command::Flush;
    assert!(!RlteCoordinator::should_plan(&cmd));
}

#[test]
fn should_plan_with_descending_order() {
    let cmd = Command::Query {
        event_type: "purchase".to_string(),
        context_id: None,
        since: None,
        time_field: None,
        where_clause: None,
        limit: Some(100),
        offset: None,
        order_by: Some(OrderSpec {
            field: "amount".to_string(),
            desc: true,
        }),
        picked_zones: None,
        return_fields: None,
        link_field: None,
        aggs: None,
        time_bucket: None,
        group_by: None,
        event_sequence: None,
    };

    assert!(RlteCoordinator::should_plan(&cmd));
}

#[test]
fn should_plan_with_various_order_fields() {
    let fields = vec!["timestamp", "context_id", "event_type", "custom_field"];

    for field in fields {
        let cmd = Command::Query {
            event_type: "test".to_string(),
            context_id: None,
            since: None,
            time_field: None,
            where_clause: None,
            limit: None,
            offset: None,
            order_by: Some(OrderSpec {
                field: field.to_string(),
                desc: false,
            }),
            picked_zones: None,
            return_fields: None,
            link_field: None,
            aggs: None,
            time_bucket: None,
            group_by: None,
            event_sequence: None,
        };

        assert!(RlteCoordinator::should_plan(&cmd));
    }
}

// Note: Full integration tests for RlteCoordinator::plan() would require
// actual schema registry, segments, and RLTE index data, which is better
// tested in the existing query_tests.rs integration tests.
// The core logic is tested via should_plan() above and integration tests.
