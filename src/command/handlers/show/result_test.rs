use super::result::ShowRefreshOutcome;
use crate::command::types::MaterializedQuerySpec;
use crate::engine::materialize::MaterializationEntry;
use crate::engine::materialize::catalog::SchemaSnapshot;
use crate::test_helpers::factories::command_factory::CommandFactory;

fn make_entry(root: &std::path::Path) -> MaterializationEntry {
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
    entry.row_count = 42;
    entry.delta_rows_appended = 5;
    entry
}

#[test]
fn returns_wrapped_entry() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let entry = make_entry(temp_dir.path());

    let outcome = ShowRefreshOutcome::new(entry.clone());
    let inner = outcome.into_entry();

    assert_eq!(inner.name, entry.name);
    assert_eq!(inner.row_count, entry.row_count);
    assert_eq!(inner.schema, entry.schema);
}
