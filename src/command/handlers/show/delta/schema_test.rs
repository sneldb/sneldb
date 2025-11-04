use super::schema::SchemaBuilder;
use crate::command::types::MaterializedQuerySpec;
use crate::engine::materialize::MaterializationEntry;
use crate::engine::materialize::catalog::SchemaSnapshot;
use crate::test_helpers::factories::command_factory::CommandFactory;

fn build_entry_with_schema(root: &std::path::Path) -> MaterializationEntry {
    let command = CommandFactory::query().with_event_type("orders").create();

    let spec = MaterializedQuerySpec {
        name: "orders_view".to_string(),
        query: Box::new(command),
    };

    let mut entry = MaterializationEntry::new(spec, root).expect("entry");
    entry.schema = vec![
        SchemaSnapshot::new("timestamp", "Number"),
        SchemaSnapshot::new("event_id", "Number"),
    ];
    entry
}

#[test]
fn builds_batch_schema_from_snapshots() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let entry = build_entry_with_schema(temp_dir.path());

    let schema = SchemaBuilder::build(&entry).expect("schema");
    let column_names: Vec<_> = schema.columns().iter().map(|c| c.name.clone()).collect();
    assert_eq!(column_names, vec!["timestamp", "event_id"]);
}

#[test]
fn fails_when_schema_is_empty() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let mut entry = build_entry_with_schema(temp_dir.path());
    entry.schema.clear();

    let err = SchemaBuilder::build(&entry).expect_err("expected failure");
    assert!(err.message().contains("Failed to build batch schema"));
}
