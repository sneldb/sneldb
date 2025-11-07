use crate::command::handlers::query::context::QueryContext;
use crate::command::handlers::query::merge::ordered::OrderedMerger;
use crate::command::types::Command;
use crate::engine::core::read::result::{ColumnSpec, QueryResult, SelectionResult};
use crate::engine::shard::manager::ShardManager;
use crate::engine::types::ScalarValue;
use crate::test_helpers::factories::SchemaRegistryFactory;
use serde_json::{Value as JsonValue, json};

fn create_context() -> QueryContext<'static> {
    let command = Box::leak(Box::new(Command::Query {
        event_type: "test".to_string(),
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
    }));

    let manager = Box::leak(Box::new(ShardManager { shards: Vec::new() }));
    let registry = SchemaRegistryFactory::new().registry();
    QueryContext::new(command, manager, registry)
}

fn create_selection_result(
    columns: Vec<ColumnSpec>,
    rows: Vec<Vec<serde_json::Value>>,
) -> QueryResult {
    let scalar_rows: Vec<Vec<ScalarValue>> = rows
        .into_iter()
        .map(|row| row.into_iter().map(ScalarValue::from).collect())
        .collect();
    QueryResult::Selection(SelectionResult {
        columns,
        rows: scalar_rows,
    })
}

#[test]
fn merge_combines_sorted_results_ascending() {
    let merger = OrderedMerger::new("value".to_string(), true, None, None);
    let ctx = create_context();

    // RowComparator expects: [context_id, event_type, timestamp, payload]
    // where payload is a JSON object containing the field to compare
    let columns = vec![
        ColumnSpec {
            name: "context_id".to_string(),
            logical_type: "String".to_string(),
        },
        ColumnSpec {
            name: "event_type".to_string(),
            logical_type: "String".to_string(),
        },
        ColumnSpec {
            name: "timestamp".to_string(),
            logical_type: "Timestamp".to_string(),
        },
        ColumnSpec {
            name: "payload".to_string(),
            logical_type: "Object".to_string(),
        },
    ];

    let shard1 = create_selection_result(
        columns.clone(),
        vec![
            vec![
                json!("ctx1"),
                json!("evt"),
                json!(1000),
                json!({"value": 10}),
            ],
            vec![
                json!("ctx2"),
                json!("evt"),
                json!(2000),
                json!({"value": 30}),
            ],
        ],
    );

    let shard2 = create_selection_result(
        columns.clone(),
        vec![
            vec![
                json!("ctx3"),
                json!("evt"),
                json!(1500),
                json!({"value": 20}),
            ],
            vec![
                json!("ctx4"),
                json!("evt"),
                json!(2500),
                json!({"value": 40}),
            ],
        ],
    );

    let result = merger
        .merge(&ctx, vec![shard1, shard2])
        .expect("merge should succeed");

    match result {
        QueryResult::Selection(sel) => {
            assert_eq!(sel.rows.len(), 4);
            let payload0 = sel.rows[0][3].to_json();
            let payload1 = sel.rows[1][3].to_json();
            let payload2 = sel.rows[2][3].to_json();
            let payload3 = sel.rows[3][3].to_json();
            assert_eq!(payload0.get("value"), Some(&json!(10)));
            assert_eq!(payload1.get("value"), Some(&json!(20)));
            assert_eq!(payload2.get("value"), Some(&json!(30)));
            assert_eq!(payload3.get("value"), Some(&json!(40)));
        }
        _ => panic!("Expected Selection result"),
    }
}

#[test]
fn merge_combines_sorted_results_descending() {
    let merger = OrderedMerger::new("value".to_string(), false, None, None);
    let ctx = create_context();

    // RowComparator expects: [context_id, event_type, timestamp, payload]
    // where payload is a JSON object containing the field to compare
    let columns = vec![
        ColumnSpec {
            name: "context_id".to_string(),
            logical_type: "String".to_string(),
        },
        ColumnSpec {
            name: "event_type".to_string(),
            logical_type: "String".to_string(),
        },
        ColumnSpec {
            name: "timestamp".to_string(),
            logical_type: "Timestamp".to_string(),
        },
        ColumnSpec {
            name: "payload".to_string(),
            logical_type: "Object".to_string(),
        },
    ];

    // For descending order, each shard's rows must be sorted descending
    let shard1 = create_selection_result(
        columns.clone(),
        vec![
            vec![
                json!("ctx1"),
                json!("evt"),
                json!(1000),
                json!({"value": 30}),
            ],
            vec![
                json!("ctx2"),
                json!("evt"),
                json!(2000),
                json!({"value": 10}),
            ],
        ],
    );

    let shard2 = create_selection_result(
        columns.clone(),
        vec![
            vec![
                json!("ctx3"),
                json!("evt"),
                json!(1500),
                json!({"value": 40}),
            ],
            vec![
                json!("ctx4"),
                json!("evt"),
                json!(2500),
                json!({"value": 20}),
            ],
        ],
    );

    let result = merger
        .merge(&ctx, vec![shard1, shard2])
        .expect("merge should succeed");

    match result {
        QueryResult::Selection(sel) => {
            assert_eq!(sel.rows.len(), 4);
            let payload0 = sel.rows[0][3].to_json();
            let payload1 = sel.rows[1][3].to_json();
            let payload2 = sel.rows[2][3].to_json();
            let payload3 = sel.rows[3][3].to_json();
            assert_eq!(payload0.get("value"), Some(&json!(40)));
            assert_eq!(payload1.get("value"), Some(&json!(30)));
            assert_eq!(payload2.get("value"), Some(&json!(20)));
            assert_eq!(payload3.get("value"), Some(&json!(10)));
        }
        _ => panic!("Expected Selection result"),
    }
}

#[test]
fn merge_applies_limit() {
    let merger = OrderedMerger::new("value".to_string(), true, Some(2), None);
    let ctx = create_context();

    // RowComparator expects: [context_id, event_type, timestamp, payload]
    // where payload is a JSON object containing the field to compare
    let columns = vec![
        ColumnSpec {
            name: "context_id".to_string(),
            logical_type: "String".to_string(),
        },
        ColumnSpec {
            name: "event_type".to_string(),
            logical_type: "String".to_string(),
        },
        ColumnSpec {
            name: "timestamp".to_string(),
            logical_type: "Timestamp".to_string(),
        },
        ColumnSpec {
            name: "payload".to_string(),
            logical_type: "Object".to_string(),
        },
    ];

    let shard1 = create_selection_result(
        columns.clone(),
        vec![
            vec![
                json!("ctx1"),
                json!("evt"),
                json!(1000),
                json!({"value": 10}),
            ],
            vec![
                json!("ctx2"),
                json!("evt"),
                json!(2000),
                json!({"value": 30}),
            ],
        ],
    );

    let shard2 = create_selection_result(
        columns.clone(),
        vec![
            vec![
                json!("ctx3"),
                json!("evt"),
                json!(1500),
                json!({"value": 20}),
            ],
            vec![
                json!("ctx4"),
                json!("evt"),
                json!(2500),
                json!({"value": 40}),
            ],
        ],
    );

    let result = merger
        .merge(&ctx, vec![shard1, shard2])
        .expect("merge should succeed");

    match result {
        QueryResult::Selection(sel) => {
            assert_eq!(sel.rows.len(), 2);
            let payload0 = sel.rows[0][3].to_json();
            let payload1 = sel.rows[1][3].to_json();
            assert_eq!(payload0.get("value"), Some(&json!(10)));
            assert_eq!(payload1.get("value"), Some(&json!(20)));
        }
        _ => panic!("Expected Selection result"),
    }
}

#[test]
fn merge_applies_offset() {
    let merger = OrderedMerger::new("value".to_string(), true, None, Some(1));
    let ctx = create_context();

    // RowComparator expects: [context_id, event_type, timestamp, payload]
    // where payload is a JSON object containing the field to compare
    let columns = vec![
        ColumnSpec {
            name: "context_id".to_string(),
            logical_type: "String".to_string(),
        },
        ColumnSpec {
            name: "event_type".to_string(),
            logical_type: "String".to_string(),
        },
        ColumnSpec {
            name: "timestamp".to_string(),
            logical_type: "Timestamp".to_string(),
        },
        ColumnSpec {
            name: "payload".to_string(),
            logical_type: "Object".to_string(),
        },
    ];

    let shard1 = create_selection_result(
        columns.clone(),
        vec![
            vec![
                json!("ctx1"),
                json!("evt"),
                json!(1000),
                json!({"value": 10}),
            ],
            vec![
                json!("ctx2"),
                json!("evt"),
                json!(2000),
                json!({"value": 30}),
            ],
        ],
    );

    let shard2 = create_selection_result(
        columns.clone(),
        vec![
            vec![
                json!("ctx3"),
                json!("evt"),
                json!(1500),
                json!({"value": 20}),
            ],
            vec![
                json!("ctx4"),
                json!("evt"),
                json!(2500),
                json!({"value": 40}),
            ],
        ],
    );

    let result = merger
        .merge(&ctx, vec![shard1, shard2])
        .expect("merge should succeed");

    match result {
        QueryResult::Selection(sel) => {
            assert_eq!(sel.rows.len(), 3);
            let payload0 = sel.rows[0][3].to_json();
            let payload1 = sel.rows[1][3].to_json();
            let payload2 = sel.rows[2][3].to_json();
            assert_eq!(payload0.get("value"), Some(&json!(20)));
            assert_eq!(payload1.get("value"), Some(&json!(30)));
            assert_eq!(payload2.get("value"), Some(&json!(40)));
        }
        _ => panic!("Expected Selection result"),
    }
}

#[test]
fn merge_applies_limit_and_offset() {
    let merger = OrderedMerger::new("value".to_string(), true, Some(2), Some(1));
    let ctx = create_context();

    // RowComparator expects: [context_id, event_type, timestamp, payload]
    // where payload is a JSON object containing the field to compare
    let columns = vec![
        ColumnSpec {
            name: "context_id".to_string(),
            logical_type: "String".to_string(),
        },
        ColumnSpec {
            name: "event_type".to_string(),
            logical_type: "String".to_string(),
        },
        ColumnSpec {
            name: "timestamp".to_string(),
            logical_type: "Timestamp".to_string(),
        },
        ColumnSpec {
            name: "payload".to_string(),
            logical_type: "Object".to_string(),
        },
    ];

    let shard1 = create_selection_result(
        columns.clone(),
        vec![
            vec![
                json!("ctx1"),
                json!("evt"),
                json!(1000),
                json!({"value": 10}),
            ],
            vec![
                json!("ctx2"),
                json!("evt"),
                json!(2000),
                json!({"value": 30}),
            ],
        ],
    );

    let shard2 = create_selection_result(
        columns.clone(),
        vec![
            vec![
                json!("ctx3"),
                json!("evt"),
                json!(1500),
                json!({"value": 20}),
            ],
            vec![
                json!("ctx4"),
                json!("evt"),
                json!(2500),
                json!({"value": 40}),
            ],
        ],
    );

    let result = merger
        .merge(&ctx, vec![shard1, shard2])
        .expect("merge should succeed");

    match result {
        QueryResult::Selection(sel) => {
            assert_eq!(sel.rows.len(), 2);
            let payload0 = sel.rows[0][3].to_json();
            let payload1 = sel.rows[1][3].to_json();
            assert_eq!(payload0.get("value"), Some(&json!(20)));
            assert_eq!(payload1.get("value"), Some(&json!(30)));
        }
        _ => panic!("Expected Selection result"),
    }
}

#[test]
fn merge_handles_empty_results() {
    let merger = OrderedMerger::new("value".to_string(), true, None, None);
    let ctx = create_context();

    let result = merger.merge(&ctx, vec![]).expect("merge should succeed");

    match result {
        QueryResult::Selection(sel) => {
            assert_eq!(sel.columns.len(), 0);
            assert_eq!(sel.rows.len(), 0);
        }
        _ => panic!("Expected Selection result"),
    }
}
