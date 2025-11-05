use crate::command::handlers::query::context::QueryContext;
use crate::command::handlers::query::merge::unordered::UnorderedMerger;
use crate::command::types::Command;
use crate::engine::core::read::result::{ColumnSpec, QueryResult, SelectionResult};
use crate::engine::shard::manager::ShardManager;
use crate::engine::types::ScalarValue;
use crate::test_helpers::factories::SchemaRegistryFactory;
use serde_json::json;

fn create_context() -> QueryContext<'static> {
    let command = Box::leak(Box::new(Command::Query {
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
fn merge_combines_multiple_shard_results() {
    let merger = UnorderedMerger::new(None, None);
    let ctx = create_context();

    let columns = vec![
        ColumnSpec {
            name: "id".to_string(),
            logical_type: "Integer".to_string(),
        },
        ColumnSpec {
            name: "value".to_string(),
            logical_type: "String".to_string(),
        },
    ];

    let shard1 = create_selection_result(
        columns.clone(),
        vec![vec![json!(1), json!("a")], vec![json!(2), json!("b")]],
    );

    let shard2 = create_selection_result(
        columns.clone(),
        vec![vec![json!(3), json!("c")], vec![json!(4), json!("d")]],
    );

    let result = merger
        .merge(&ctx, vec![shard1, shard2])
        .expect("merge should succeed");

    match result {
        QueryResult::Selection(sel) => {
            assert_eq!(sel.rows.len(), 4);
        }
        _ => panic!("Expected Selection result"),
    }
}

#[test]
fn merge_applies_limit() {
    let merger = UnorderedMerger::new(Some(2), None);
    let ctx = create_context();

    let columns = vec![ColumnSpec {
        name: "id".to_string(),
        logical_type: "Integer".to_string(),
    }];

    let shard1 = create_selection_result(
        columns.clone(),
        vec![vec![json!(1)], vec![json!(2)], vec![json!(3)]],
    );

    let shard2 = create_selection_result(columns.clone(), vec![vec![json!(4)]]);

    let result = merger
        .merge(&ctx, vec![shard1, shard2])
        .expect("merge should succeed");

    match result {
        QueryResult::Selection(sel) => {
            assert_eq!(sel.rows.len(), 2);
        }
        _ => panic!("Expected Selection result"),
    }
}

#[test]
fn merge_applies_offset() {
    let merger = UnorderedMerger::new(None, Some(2));
    let ctx = create_context();

    let columns = vec![ColumnSpec {
        name: "id".to_string(),
        logical_type: "Integer".to_string(),
    }];

    let shard1 = create_selection_result(
        columns.clone(),
        vec![vec![json!(1)], vec![json!(2)], vec![json!(3)]],
    );

    let shard2 = create_selection_result(columns.clone(), vec![vec![json!(4)]]);

    let result = merger
        .merge(&ctx, vec![shard1, shard2])
        .expect("merge should succeed");

    match result {
        QueryResult::Selection(sel) => {
            assert_eq!(sel.rows.len(), 2);
        }
        _ => panic!("Expected Selection result"),
    }
}

#[test]
fn merge_applies_limit_and_offset() {
    let merger = UnorderedMerger::new(Some(2), Some(1));
    let ctx = create_context();

    let columns = vec![ColumnSpec {
        name: "id".to_string(),
        logical_type: "Integer".to_string(),
    }];

    let shard1 = create_selection_result(
        columns.clone(),
        vec![vec![json!(1)], vec![json!(2)], vec![json!(3)]],
    );

    let shard2 = create_selection_result(columns.clone(), vec![vec![json!(4)]]);

    let result = merger
        .merge(&ctx, vec![shard1, shard2])
        .expect("merge should succeed");

    match result {
        QueryResult::Selection(sel) => {
            assert_eq!(sel.rows.len(), 2);
        }
        _ => panic!("Expected Selection result"),
    }
}

#[test]
fn merge_handles_offset_larger_than_results() {
    let merger = UnorderedMerger::new(None, Some(100));
    let ctx = create_context();

    let columns = vec![ColumnSpec {
        name: "id".to_string(),
        logical_type: "Integer".to_string(),
    }];

    let shard1 = create_selection_result(columns.clone(), vec![vec![json!(1)], vec![json!(2)]]);

    let result = merger
        .merge(&ctx, vec![shard1])
        .expect("merge should succeed");

    match result {
        QueryResult::Selection(sel) => {
            assert_eq!(sel.rows.len(), 0);
        }
        _ => panic!("Expected Selection result"),
    }
}

#[test]
fn merge_handles_empty_results() {
    let merger = UnorderedMerger::new(None, None);
    let ctx = create_context();

    let result = merger.merge(&ctx, vec![]);

    assert!(result.is_err());
    assert!(result.unwrap_err().contains("No results"));
}

#[test]
fn merge_handles_single_shard() {
    let merger = UnorderedMerger::new(None, None);
    let ctx = create_context();

    let columns = vec![ColumnSpec {
        name: "id".to_string(),
        logical_type: "Integer".to_string(),
    }];

    let shard1 = create_selection_result(columns.clone(), vec![vec![json!(1)], vec![json!(2)]]);

    let result = merger
        .merge(&ctx, vec![shard1])
        .expect("merge should succeed");

    match result {
        QueryResult::Selection(sel) => {
            assert_eq!(sel.rows.len(), 2);
        }
        _ => panic!("Expected Selection result"),
    }
}
