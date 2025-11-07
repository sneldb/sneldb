use super::shard_command_builder::ShardCommandBuilder;
use crate::command::types::{Command, OrderSpec, PickedZones};
use std::borrow::Cow;
use std::collections::HashMap;

fn make_base_query() -> Command {
    Command::Query {
        event_type: "click".to_string(),
        context_id: None,
        since: None,
        time_field: None,
        sequence_time_field: None,
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
    }
}

#[test]
fn returns_borrowed_when_no_picked_zones() {
    let cmd = make_base_query();
    let builder = ShardCommandBuilder::new(&cmd, None);

    let result = builder.build_for_shard(0);

    // Should be borrowed, not owned
    assert!(matches!(result, Cow::Borrowed(_)));
}

#[test]
fn returns_owned_when_picked_zones_exist() {
    let cmd = make_base_query();

    let mut map = HashMap::new();
    map.insert(
        0,
        PickedZones {
            uid: "uid-123".to_string(),
            field: "timestamp".to_string(),
            asc: true,
            cutoff: "1000".to_string(),
            k: 10,
            zones: vec![("00001".to_string(), 0)],
        },
    );

    let builder = ShardCommandBuilder::new(&cmd, Some(&map));
    let result = builder.build_for_shard(0);

    // Should be owned (cloned and modified)
    assert!(matches!(result, Cow::Owned(_)));

    // Verify picked_zones were injected
    if let Command::Query { picked_zones, .. } = result.as_ref() {
        assert!(picked_zones.is_some());
        let pz = picked_zones.as_ref().unwrap();
        assert_eq!(pz.uid, "uid-123");
        assert_eq!(pz.zones.len(), 1);
    } else {
        panic!("Expected Query command");
    }
}

#[test]
fn returns_owned_with_empty_zones_when_shard_not_in_map() {
    let cmd = make_base_query();

    let mut map = HashMap::new();
    map.insert(
        0,
        PickedZones {
            uid: "uid-123".to_string(),
            field: "timestamp".to_string(),
            asc: true,
            cutoff: "1000".to_string(),
            k: 10,
            zones: vec![("00001".to_string(), 0)],
        },
    );

    let builder = ShardCommandBuilder::new(&cmd, Some(&map));
    let result = builder.build_for_shard(999); // shard not in map

    // Should be owned with empty picked_zones
    assert!(matches!(result, Cow::Owned(_)));

    if let Command::Query { picked_zones, .. } = result.as_ref() {
        assert!(picked_zones.is_some());
        let pz = picked_zones.as_ref().unwrap();
        assert_eq!(pz.zones.len(), 0); // Empty zones
        assert_eq!(pz.k, 10); // k should match limit
    } else {
        panic!("Expected Query command");
    }
}

#[test]
fn preserves_all_query_fields() {
    let cmd = Command::Query {
        event_type: "purchase".to_string(),
        context_id: Some("ctx-123".to_string()),
        since: Some("2024-01-01".to_string()),
        time_field: Some("custom_time".to_string()),
        sequence_time_field: None,
        where_clause: None,
        limit: Some(100),
        offset: Some(50),
        order_by: Some(OrderSpec {
            field: "amount".to_string(),
            desc: true,
        }),
        picked_zones: None,
        return_fields: Some(vec!["field1".to_string(), "field2".to_string()]),
        link_field: Some("user_id".to_string()),
        aggs: None,
        time_bucket: None,
        group_by: Some(vec!["region".to_string()]),
        event_sequence: None,
    };

    let mut map = HashMap::new();
    map.insert(
        0,
        PickedZones {
            uid: "uid".to_string(),
            field: "amount".to_string(),
            asc: false,
            cutoff: "0".to_string(),
            k: 150,
            zones: vec![("00001".to_string(), 0)],
        },
    );

    let builder = ShardCommandBuilder::new(&cmd, Some(&map));
    let result = builder.build_for_shard(0);

    if let Command::Query {
        event_type,
        context_id,
        since,
        time_field,
        limit,
        offset,
        order_by,
        return_fields,
        link_field,
        group_by,
        ..
    } = result.as_ref()
    {
        assert_eq!(event_type, "purchase");
        assert_eq!(context_id.as_ref().unwrap(), "ctx-123");
        assert_eq!(since.as_ref().unwrap(), "2024-01-01");
        assert_eq!(time_field.as_ref().unwrap(), "custom_time");
        assert_eq!(*limit, Some(100));
        assert_eq!(*offset, Some(50));
        assert_eq!(order_by.as_ref().unwrap().field, "amount");
        assert_eq!(return_fields.as_ref().unwrap().len(), 2);
        assert_eq!(link_field.as_ref().unwrap(), "user_id");
        assert_eq!(group_by.as_ref().unwrap().len(), 1);
    } else {
        panic!("Expected Query command");
    }
}

#[test]
fn calculates_correct_k_for_empty_zones() {
    let cmd = Command::Query {
        event_type: "click".to_string(),
        context_id: None,
        since: None,
        time_field: None,
        sequence_time_field: None,
        where_clause: None,
        limit: Some(10),
        offset: Some(5),
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

    let map = HashMap::new(); // Empty map
    let builder = ShardCommandBuilder::new(&cmd, Some(&map));
    let result = builder.build_for_shard(0);

    if let Command::Query { picked_zones, .. } = result.as_ref() {
        let pz = picked_zones.as_ref().unwrap();
        // k should be limit + offset = 10 + 5 = 15
        assert_eq!(pz.k, 15);
    } else {
        panic!("Expected Query command");
    }
}

#[test]
fn handles_no_limit_or_offset() {
    let cmd = Command::Query {
        event_type: "click".to_string(),
        context_id: None,
        since: None,
        time_field: None,
        sequence_time_field: None,
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

    let map = HashMap::new();
    let builder = ShardCommandBuilder::new(&cmd, Some(&map));
    let result = builder.build_for_shard(0);

    if let Command::Query { picked_zones, .. } = result.as_ref() {
        let pz = picked_zones.as_ref().unwrap();
        // k should be 0 when no limit/offset
        assert_eq!(pz.k, 0);
    } else {
        panic!("Expected Query command");
    }
}

#[test]
fn extracts_correct_order_spec_info() {
    let cmd = Command::Query {
        event_type: "click".to_string(),
        context_id: None,
        since: None,
        time_field: None,
        sequence_time_field: None,
        where_clause: None,
        limit: Some(10),
        offset: None,
        order_by: Some(OrderSpec {
            field: "score".to_string(),
            desc: true, // descending
        }),
        picked_zones: None,
        return_fields: None,
        link_field: None,
        aggs: None,
        time_bucket: None,
        group_by: None,
        event_sequence: None,
    };

    let map = HashMap::new();
    let builder = ShardCommandBuilder::new(&cmd, Some(&map));
    let result = builder.build_for_shard(0);

    if let Command::Query { picked_zones, .. } = result.as_ref() {
        let pz = picked_zones.as_ref().unwrap();
        assert_eq!(pz.field, "score");
        assert!(!pz.asc); // desc=true means asc=false
    } else {
        panic!("Expected Query command");
    }
}

#[test]
fn handles_missing_order_by() {
    let cmd = Command::Query {
        event_type: "click".to_string(),
        context_id: None,
        since: None,
        time_field: None,
        sequence_time_field: None,
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

    let map = HashMap::new();
    let builder = ShardCommandBuilder::new(&cmd, Some(&map));
    let result = builder.build_for_shard(0);

    if let Command::Query { picked_zones, .. } = result.as_ref() {
        let pz = picked_zones.as_ref().unwrap();
        assert_eq!(pz.field, ""); // Empty field when no order_by
        assert!(pz.asc); // Default to asc=true
    } else {
        panic!("Expected Query command");
    }
}

#[test]
fn returns_borrowed_for_non_query_commands() {
    let ping_cmd = Command::Ping;
    let builder = ShardCommandBuilder::new(&ping_cmd, None);

    let result = builder.build_for_shard(0);

    // Should be borrowed for non-Query commands
    assert!(matches!(result, Cow::Borrowed(_)));
}

#[test]
fn multiple_shards_get_different_zones() {
    let cmd = make_base_query();

    let mut map = HashMap::new();
    map.insert(
        0,
        PickedZones {
            uid: "uid-0".to_string(),
            field: "timestamp".to_string(),
            asc: true,
            cutoff: "1000".to_string(),
            k: 10,
            zones: vec![("00001".to_string(), 0)],
        },
    );
    map.insert(
        1,
        PickedZones {
            uid: "uid-1".to_string(),
            field: "timestamp".to_string(),
            asc: true,
            cutoff: "1000".to_string(),
            k: 10,
            zones: vec![("00002".to_string(), 0), ("00003".to_string(), 1)],
        },
    );

    let builder = ShardCommandBuilder::new(&cmd, Some(&map));

    let result0 = builder.build_for_shard(0);
    let result1 = builder.build_for_shard(1);

    // Both should be owned
    assert!(matches!(result0, Cow::Owned(_)));
    assert!(matches!(result1, Cow::Owned(_)));

    // Verify different zones
    if let Command::Query { picked_zones, .. } = result0.as_ref() {
        let pz = picked_zones.as_ref().unwrap();
        assert_eq!(pz.zones.len(), 1);
        assert_eq!(pz.uid, "uid-0");
    }

    if let Command::Query { picked_zones, .. } = result1.as_ref() {
        let pz = picked_zones.as_ref().unwrap();
        assert_eq!(pz.zones.len(), 2);
        assert_eq!(pz.uid, "uid-1");
    }
}
