use super::batch_merger::BatchMerger;
use crate::command::handlers::query::context::QueryContext;
use crate::engine::core::read::result::{ColumnSpec, QueryResult, SelectionResult};
use crate::engine::shard::manager::ShardManager;
use crate::engine::types::ScalarValue;
use crate::test_helpers::factories::{CommandFactory, SchemaRegistryFactory};
use serde_json::json;

fn create_context() -> QueryContext<'static> {
    let command = Box::leak(Box::new(CommandFactory::query().create()));

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
fn for_command_returns_ordered_merger_with_order_by() {
    let command = CommandFactory::query()
        .with_limit(10)
        .with_offset(5)
        .with_order_by("timestamp", false)
        .create();

    let merger = BatchMerger::for_command(&command);
    match merger {
        BatchMerger::Ordered(_) => {}
        BatchMerger::Unordered(_) => panic!("Expected Ordered merger"),
    }
}

#[test]
fn for_command_returns_unordered_merger_without_order_by() {
    let command = CommandFactory::query()
        .with_limit(10)
        .with_offset(5)
        .create();

    let merger = BatchMerger::for_command(&command);
    match merger {
        BatchMerger::Ordered(_) => panic!("Expected Unordered merger"),
        BatchMerger::Unordered(_) => {}
    }
}

#[test]
fn merge_delegates_to_ordered_merger() {
    let command = Box::leak(Box::new(
        CommandFactory::query()
            .with_order_by("timestamp", false)
            .create(),
    ));

    let merger = BatchMerger::for_command(command);

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
        vec![vec![
            json!("ctx1"),
            json!("evt"),
            json!(1000),
            json!({"timestamp": 1000}),
        ]],
    );

    let ctx = create_context();
    let result = merger.merge(&ctx, vec![shard1]);
    assert!(result.is_ok());
}

#[test]
fn merge_delegates_to_unordered_merger() {
    let command = CommandFactory::query().create();

    let merger = BatchMerger::for_command(&command);

    let columns = vec![ColumnSpec {
        name: "id".to_string(),
        logical_type: "Integer".to_string(),
    }];

    let shard1 = create_selection_result(columns.clone(), vec![vec![json!(1)]]);
    let command_ref = Box::leak(Box::new(command));
    let manager = Box::leak(Box::new(ShardManager { shards: Vec::new() }));
    let registry = SchemaRegistryFactory::new().registry();
    let ctx = QueryContext::new(command_ref, manager, registry);

    let result = merger.merge(&ctx, vec![shard1]);
    assert!(result.is_ok());
}
