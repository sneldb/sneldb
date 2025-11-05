use crate::engine::core::zone::zone_xor_index::{ZoneXorFilterIndex, build_all_zxf_filtered};
use crate::engine::types::ScalarValue;
use crate::test_helpers::factories::{EventFactory, ZonePlanFactory};
use serde_json::json;
use std::collections::HashSet;

#[test]
fn build_all_zxf_filtered_writes_file_and_filters_work() {
    // Build two zones with distinct fruit distributions
    // Each zone needs at least 2 unique values to create a filter
    let e1 = EventFactory::new()
        .with("payload", json!({"fruit":"apple"}))
        .create();
    let e2 = EventFactory::new()
        .with("payload", json!({"fruit":"banana"}))
        .create();
    let e3 = EventFactory::new()
        .with("payload", json!({"fruit":"carrot"}))
        .create();
    let e4 = EventFactory::new()
        .with("payload", json!({"fruit":"date"}))
        .create();

    let z0 = ZonePlanFactory::new()
        .with("id", 0)
        .with("uid", "u01")
        .with("events", json!([e1.clone(), e2.clone()]))
        .create();
    let z1 = ZonePlanFactory::new()
        .with("id", 1)
        .with("uid", "u01")
        .with("events", json!([e3.clone(), e4.clone()]))
        .create();

    let dir = tempfile::tempdir().unwrap();
    let mut allowed: HashSet<String> = HashSet::new();
    allowed.insert("fruit".to_string());
    build_all_zxf_filtered(&[z0, z1], dir.path(), &allowed).unwrap();

    let path = dir.path().join("u01_fruit.zxf");
    let idx = ZoneXorFilterIndex::load(&path).unwrap();

    assert!(idx.contains_in_zone(0, &ScalarValue::from(json!("apple"))));
    assert!(idx.contains_in_zone(0, &ScalarValue::from(json!("banana"))));
    assert!(!idx.contains_in_zone(0, &ScalarValue::from(json!("carrot"))));
    assert!(idx.contains_in_zone(1, &ScalarValue::from(json!("carrot"))));
    assert!(idx.contains_in_zone(1, &ScalarValue::from(json!("date"))));
}

#[test]
fn zxf_filtered_handles_numeric_and_bool_values() {
    use tempfile::tempdir;

    // Build zones with numeric and boolean payloads
    // Each zone needs at least 2 unique values per field to create a filter
    let e1 = EventFactory::new()
        .with("payload", json!({"n": 42, "b": true}))
        .create();
    let e2 = EventFactory::new()
        .with("payload", json!({"n": 43, "b": false}))
        .create();
    let e3 = EventFactory::new()
        .with("payload", json!({"n": 7, "b": true}))
        .create();
    let e4 = EventFactory::new()
        .with("payload", json!({"n": 8, "b": false}))
        .create();

    let z0 = ZonePlanFactory::new()
        .with("id", 0)
        .with("uid", "u02")
        .with("events", json!([e1, e2]))
        .create();
    let z1 = ZonePlanFactory::new()
        .with("id", 1)
        .with("uid", "u02")
        .with("events", json!([e3, e4]))
        .create();

    let dir = tempdir().unwrap();
    let mut allowed: HashSet<String> = HashSet::new();
    allowed.insert("n".to_string());
    allowed.insert("b".to_string());
    build_all_zxf_filtered(&[z0, z1], dir.path(), &allowed).unwrap();

    let path_n = dir.path().join("u02_n.zxf");
    let idx_n = ZoneXorFilterIndex::load(&path_n).unwrap();
    assert!(idx_n.contains_in_zone(0, &ScalarValue::from(json!(42))));
    assert!(idx_n.contains_in_zone(1, &ScalarValue::from(json!(7))));
    assert!(!idx_n.contains_in_zone(0, &ScalarValue::from(json!(7))));

    let path_b = dir.path().join("u02_b.zxf");
    let idx_b = ZoneXorFilterIndex::load(&path_b).unwrap();
    assert!(idx_b.contains_in_zone(0, &ScalarValue::from(json!(true))));
    assert!(idx_b.contains_in_zone(0, &ScalarValue::from(json!(false)))); // Zone 0 has both true and false
    assert!(idx_b.contains_in_zone(1, &ScalarValue::from(json!(true))));
    assert!(idx_b.contains_in_zone(1, &ScalarValue::from(json!(false)))); // Zone 1 has both true and false
}

#[test]
fn zxf_zones_maybe_containing_multiple_zones() {
    use tempfile::tempdir;

    // Each zone needs at least 2 unique values per field to create a filter
    let e0 = EventFactory::new()
        .with("payload", json!({"v":"X"}))
        .create();
    let e0_2 = EventFactory::new()
        .with("payload", json!({"v":"Z"}))
        .create();
    let e1 = EventFactory::new()
        .with("payload", json!({"v":"X"}))
        .create();
    let e2 = EventFactory::new()
        .with("payload", json!({"v":"Y"}))
        .create();

    let z0 = ZonePlanFactory::new()
        .with("id", 0)
        .with("uid", "u03")
        .with("events", json!([e0, e0_2]))
        .create();
    let z1 = ZonePlanFactory::new()
        .with("id", 1)
        .with("uid", "u03")
        .with("events", json!([e1, e2]))
        .create();

    let dir = tempdir().unwrap();
    let mut allowed: HashSet<String> = HashSet::new();
    allowed.insert("v".to_string());
    build_all_zxf_filtered(&[z0, z1], dir.path(), &allowed).unwrap();
    let path = dir.path().join("u03_v.zxf");
    let idx = ZoneXorFilterIndex::load(&path).unwrap();
    let zones = idx.zones_maybe_containing(&ScalarValue::from(json!("X")));
    assert!(zones.contains(&0));
    assert!(zones.contains(&1));
}

#[test]
fn zxf_build_for_field_returns_none_when_no_values() {
    // Two zones, but field is absent in payloads
    let e0 = EventFactory::new().with("payload", json!({"a":1})).create();
    let e1 = EventFactory::new().with("payload", json!({"a":2})).create();
    let z0 = ZonePlanFactory::new()
        .with("id", 0)
        .with("uid", "u04")
        .with("events", json!([e0]))
        .create();
    let z1 = ZonePlanFactory::new()
        .with("id", 1)
        .with("uid", "u04")
        .with("events", json!([e1]))
        .create();

    let res = ZoneXorFilterIndex::build_for_field("u04", "missing", &[z0, z1]);
    assert!(res.is_none());
}

#[test]
fn zxf_filtered_skips_unallowed_fields() {
    use tempfile::tempdir;

    // Need at least 2 unique values for "keep" to create a filter
    let e0 = EventFactory::new()
        .with("payload", json!({"keep":"x", "skip":"y"}))
        .create();
    let e1 = EventFactory::new()
        .with("payload", json!({"keep":"z", "skip":"y"}))
        .create();
    let z0 = ZonePlanFactory::new()
        .with("id", 0)
        .with("uid", "u05")
        .with("events", json!([e0, e1]))
        .create();

    let dir = tempdir().unwrap();
    let mut allowed: HashSet<String> = HashSet::new();
    allowed.insert("keep".to_string());

    build_all_zxf_filtered(&[z0], dir.path(), &allowed).unwrap();

    // Allowed field exists
    assert!(dir.path().join("u05_keep.zxf").exists());
    // Not allowed field should not exist
    assert!(!dir.path().join("u05_skip.zxf").exists());
}

#[test]
fn zxf_load_parses_uid_and_field_from_filename_with_underscores() {
    use crate::test_helpers::factories::ZoneXorFilterIndexFactory;
    use tempfile::tempdir;

    let idx = ZoneXorFilterIndexFactory::new("orig", "orig_field")
        .with_zone_values(0, vec!["a".into()])
        .build();

    let dir = tempdir().unwrap();
    // Save under a filename with underscores; load should reflect filename, not original uid/field
    let path = dir.path().join("u_10_field_name_x.zxf");
    idx.save(&path).unwrap();
    let loaded = ZoneXorFilterIndex::load(&path).unwrap();
    // Current parser splits at the first underscore
    assert_eq!(loaded.uid, "u");
    assert_eq!(loaded.field, "10_field_name_x");
}

#[test]
fn zxf_file_path_naming() {
    let p = ZoneXorFilterIndex::file_path(std::path::Path::new("/tmp/seg"), "uZ", "fQ");
    assert_eq!(p, std::path::Path::new("/tmp/seg/uZ_fQ.zxf"));
}
