use crate::engine::core::FieldXorFilter;
use crate::test_helpers::factories::{EventFactory, ZonePlannerFactory};
use serde_json::json;
use tempfile::tempdir;

#[test]
fn creates_and_checks_membership() {
    let values = vec![
        "apple".to_string(),
        "banana".to_string(),
        "cherry".to_string(),
    ];
    let filter = FieldXorFilter::new(&values);

    assert!(filter.contains("banana"));
    assert!(filter.contains("cherry"));
    assert!(!filter.contains("mango"));
}

#[test]
fn handles_serde_json_values() {
    let filter = FieldXorFilter::new(&["123".to_string(), "true".to_string(), "foo".to_string()]);
    assert!(filter.contains_value(&json!(123)));
    assert!(filter.contains_value(&json!(true)));
    assert!(filter.contains_value(&json!("foo")));
    assert!(!filter.contains_value(&json!("bar")));
    assert!(!filter.contains_value(&json!(null)));
}

#[test]
fn saves_and_loads_correctly() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.xf");

    let filter = FieldXorFilter::new(&["x".to_string(), "y".to_string()]);
    filter.save(&path).unwrap();

    let loaded = FieldXorFilter::load(&path).unwrap();

    assert!(loaded.contains("x"));
    assert!(loaded.contains("y"));
    assert!(!loaded.contains("z"));
}

#[tokio::test]
async fn builds_all_creates_filters_on_disk() {
    let dir = tempdir().unwrap();
    let segment_dir = dir.path();

    let events = EventFactory::new()
        .with("event_type", "login")
        .create_list(3);

    let zones = ZonePlannerFactory::new(events, "uid123")
        .with_segment_id(42)
        .plan();

    FieldXorFilter::build_all(&zones, segment_dir).unwrap();

    let filter_file = segment_dir.join("uid123_event_type.xf");
    assert!(filter_file.exists());

    let filter = FieldXorFilter::load(&filter_file).unwrap();
    assert!(filter.contains("login"));
}
