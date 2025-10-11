use crate::command::types::{Command, OrderSpec, PickedZones};
use crate::engine::core::QueryContext;

#[test]
fn creates_empty_context() {
    let ctx = QueryContext::empty();
    assert!(!ctx.has_zone_filter());
    assert!(!ctx.has_ordering());
    assert!(!ctx.should_defer_limit());
    assert_eq!(ctx.zone_filter_count(), 0);
}

#[test]
fn creates_from_non_query_command() {
    let cmd = Command::Ping;
    let ctx = QueryContext::from_command(&cmd);
    assert!(!ctx.has_zone_filter());
    assert!(!ctx.has_ordering());
}

#[test]
fn creates_from_flush_command() {
    let cmd = Command::Flush;
    let ctx = QueryContext::from_command(&cmd);
    assert!(!ctx.has_zone_filter());
    assert!(!ctx.has_ordering());
}

#[test]
fn creates_from_query_without_options() {
    let cmd = Command::Query {
        event_type: "test".to_string(),
        context_id: None,
        since: None,
        time_field: None,
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
    };

    let ctx = QueryContext::from_command(&cmd);
    assert!(!ctx.has_zone_filter());
    assert!(!ctx.has_ordering());
    assert!(!ctx.should_defer_limit());
}

#[test]
fn creates_from_query_with_order_by() {
    let cmd = Command::Query {
        event_type: "test".to_string(),
        context_id: None,
        since: None,
        time_field: None,
        where_clause: None,
        limit: None,
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

    let ctx = QueryContext::from_command(&cmd);
    assert!(!ctx.has_zone_filter());
    assert!(ctx.has_ordering());
    assert!(ctx.should_defer_limit());

    let order = ctx.order_by.as_ref().unwrap();
    assert_eq!(order.field, "timestamp");
    assert!(!order.desc);
}

#[test]
fn creates_from_query_with_picked_zones() {
    let picked = PickedZones {
        uid: "test-uid".to_string(),
        field: "timestamp".to_string(),
        asc: true,
        cutoff: "1000".to_string(),
        k: 10,
        zones: vec![
            ("segment-1".to_string(), 0),
            ("segment-1".to_string(), 1),
            ("segment-2".to_string(), 0),
        ],
    };

    let cmd = Command::Query {
        event_type: "test".to_string(),
        context_id: None,
        since: None,
        time_field: None,
        where_clause: None,
        limit: None,
        offset: None,
        order_by: None,
        picked_zones: Some(picked),
        return_fields: None,
        link_field: None,
        aggs: None,
        time_bucket: None,
        group_by: None,
        event_sequence: None,
    };

    let ctx = QueryContext::from_command(&cmd);
    assert!(ctx.has_zone_filter());
    assert!(!ctx.has_ordering());
    assert_eq!(ctx.zone_filter_count(), 3);

    let zones = ctx.picked_zones.as_ref().unwrap();
    assert!(zones.contains(&("segment-1".to_string(), 0)));
    assert!(zones.contains(&("segment-1".to_string(), 1)));
    assert!(zones.contains(&("segment-2".to_string(), 0)));
}

#[test]
fn creates_from_query_with_both_order_and_zones() {
    let picked = PickedZones {
        uid: "test-uid".to_string(),
        field: "score".to_string(),
        asc: false,
        cutoff: "100".to_string(),
        k: 5,
        zones: vec![("segment-1".to_string(), 0)],
    };

    let cmd = Command::Query {
        event_type: "test".to_string(),
        context_id: None,
        since: None,
        time_field: None,
        where_clause: None,
        limit: Some(10),
        offset: None,
        order_by: Some(OrderSpec {
            field: "score".to_string(),
            desc: true,
        }),
        picked_zones: Some(picked),
        return_fields: None,
        link_field: None,
        aggs: None,
        time_bucket: None,
        group_by: None,
        event_sequence: None,
    };

    let ctx = QueryContext::from_command(&cmd);
    assert!(ctx.has_zone_filter());
    assert!(ctx.has_ordering());
    assert!(ctx.should_defer_limit());
    assert_eq!(ctx.zone_filter_count(), 1);
}

#[test]
fn zone_filter_count_returns_zero_when_none() {
    let ctx = QueryContext::empty();
    assert_eq!(ctx.zone_filter_count(), 0);
}

#[test]
fn zone_filter_count_returns_correct_count() {
    let picked = PickedZones {
        uid: "test-uid".to_string(),
        field: "id".to_string(),
        asc: true,
        cutoff: "0".to_string(),
        k: 100,
        zones: vec![
            ("s1".to_string(), 0),
            ("s1".to_string(), 1),
            ("s2".to_string(), 0),
            ("s2".to_string(), 1),
            ("s3".to_string(), 0),
        ],
    };

    let cmd = Command::Query {
        event_type: "test".to_string(),
        context_id: None,
        since: None,
        time_field: None,
        where_clause: None,
        limit: None,
        offset: None,
        order_by: None,
        picked_zones: Some(picked),
        return_fields: None,
        link_field: None,
        aggs: None,
        time_bucket: None,
        group_by: None,
        event_sequence: None,
    };

    let ctx = QueryContext::from_command(&cmd);
    assert_eq!(ctx.zone_filter_count(), 5);
}

#[test]
fn default_creates_empty_context() {
    let ctx = QueryContext::default();
    assert!(!ctx.has_zone_filter());
    assert!(!ctx.has_ordering());
}

#[test]
fn clone_works() {
    let cmd = Command::Query {
        event_type: "test".to_string(),
        context_id: None,
        since: None,
        time_field: None,
        where_clause: None,
        limit: None,
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

    let ctx = QueryContext::from_command(&cmd);
    let cloned = ctx.clone();

    assert_eq!(ctx.has_ordering(), cloned.has_ordering());
    assert_eq!(ctx.has_zone_filter(), cloned.has_zone_filter());
}

#[test]
fn debug_format_is_readable() {
    let ctx = QueryContext::empty();
    let debug_str = format!("{:?}", ctx);
    assert!(debug_str.contains("QueryContext"));
}

#[test]
fn display_format_is_readable() {
    let ctx = QueryContext::empty();
    assert_eq!(ctx.to_string(), "QueryContext(zones=false, order=false)");

    let cmd = Command::Query {
        event_type: "test".to_string(),
        context_id: None,
        since: None,
        time_field: None,
        where_clause: None,
        limit: None,
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

    let ctx_with_order = QueryContext::from_command(&cmd);
    assert_eq!(
        ctx_with_order.to_string(),
        "QueryContext(zones=false, order=true)"
    );
}

#[test]
fn picked_zones_deduplicates_automatically() {
    // HashSet should automatically deduplicate
    let picked = PickedZones {
        uid: "test-uid".to_string(),
        field: "id".to_string(),
        asc: true,
        cutoff: "0".to_string(),
        k: 10,
        zones: vec![
            ("segment-1".to_string(), 0),
            ("segment-1".to_string(), 0), // duplicate
            ("segment-1".to_string(), 1),
        ],
    };

    let cmd = Command::Query {
        event_type: "test".to_string(),
        context_id: None,
        since: None,
        time_field: None,
        where_clause: None,
        limit: None,
        offset: None,
        order_by: None,
        picked_zones: Some(picked),
        return_fields: None,
        link_field: None,
        aggs: None,
        time_bucket: None,
        group_by: None,
        event_sequence: None,
    };

    let ctx = QueryContext::from_command(&cmd);
    // Should be 2, not 3 (duplicate removed by HashSet)
    assert_eq!(ctx.zone_filter_count(), 2);
}

#[test]
fn empty_picked_zones_list() {
    let picked = PickedZones {
        uid: "test-uid".to_string(),
        field: "id".to_string(),
        asc: true,
        cutoff: "0".to_string(),
        k: 0,
        zones: vec![], // empty list
    };

    let cmd = Command::Query {
        event_type: "test".to_string(),
        context_id: None,
        since: None,
        time_field: None,
        where_clause: None,
        limit: None,
        offset: None,
        order_by: None,
        picked_zones: Some(picked),
        return_fields: None,
        link_field: None,
        aggs: None,
        time_bucket: None,
        group_by: None,
        event_sequence: None,
    };

    let ctx = QueryContext::from_command(&cmd);
    assert!(ctx.has_zone_filter());
    assert_eq!(ctx.zone_filter_count(), 0);
}

#[test]
fn should_defer_limit_only_with_ordering() {
    let ctx_empty = QueryContext::empty();
    assert!(!ctx_empty.should_defer_limit());

    let cmd_with_order = Command::Query {
        event_type: "test".to_string(),
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

    let ctx_with_order = QueryContext::from_command(&cmd_with_order);
    assert!(ctx_with_order.should_defer_limit());
}

