use super::entry::MaterializationEntry;
use super::policy::RetentionPolicy;
use super::schema::SchemaSnapshot;
use crate::command::types::MaterializedQuerySpec;
use crate::engine::materialize::MaterializationError;
use crate::test_helpers::factories::CommandFactory;
use tempfile::tempdir;

fn sample_spec() -> MaterializedQuerySpec {
    MaterializedQuerySpec {
        name: "orders_daily".into(),
        query: Box::new(CommandFactory::query().with_event_type("orders").create()),
    }
}

#[test]
fn materialization_entry_new_sets_defaults() -> Result<(), MaterializationError> {
    let dir = tempdir().unwrap();
    let spec = sample_spec();

    let entry = MaterializationEntry::new(spec, dir.path())?;

    assert_eq!(entry.name, "orders_daily");
    assert!(entry.storage_path.starts_with(dir.path()));
    assert!(entry.schema.is_empty());
    assert!(entry.high_water_mark.is_none());
    assert_eq!(entry.row_count, 0);
    assert!(entry.retention.is_none());
    Ok(())
}

#[test]
fn retention_policy_helpers_work() -> Result<(), MaterializationError> {
    let dir = tempdir().unwrap();
    let spec = sample_spec();
    let mut entry = MaterializationEntry::new(spec, dir.path())?;

    assert!(entry.retention_policy().is_none());

    let policy = RetentionPolicy {
        max_rows: Some(1_000),
        max_age_seconds: Some(60),
    };
    entry.set_retention(policy.clone());

    assert_eq!(entry.retention_policy(), Some(&policy));
    Ok(())
}

#[test]
fn telemetry_summary_reflects_entry_state() -> Result<(), MaterializationError> {
    let dir = tempdir().unwrap();
    let spec = sample_spec();
    let mut entry = MaterializationEntry::new(spec, dir.path())?;

    entry.schema = vec![SchemaSnapshot::new("timestamp", "Timestamp")];
    entry.row_count = 42;
    entry.delta_rows_appended = 10;
    entry.byte_size = 2048;
    entry.delta_bytes_appended = 512;
    entry.touch();

    let telemetry = entry.telemetry_summary();
    assert_eq!(telemetry.row_count, 42);
    assert_eq!(telemetry.delta_rows_appended, 10);
    assert_eq!(telemetry.byte_size, 2048);
    assert_eq!(telemetry.delta_bytes_appended, 512);
    Ok(())
}
