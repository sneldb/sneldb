use super::schema::SchemaSnapshot;

#[test]
fn schema_snapshot_new_assigns_fields() {
    let snapshot = SchemaSnapshot::new("event_id", "Integer");
    assert_eq!(snapshot.name, "event_id");
    assert_eq!(snapshot.logical_type, "Integer");
}

