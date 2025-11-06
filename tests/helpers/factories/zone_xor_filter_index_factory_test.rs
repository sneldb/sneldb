use crate::engine::types::ScalarValue;
use crate::test_helpers::factories::ZoneXorFilterIndexFactory;
use serde_json::json;

#[test]
fn builds_index_and_checks_membership_per_zone() {
    let idx = ZoneXorFilterIndexFactory::new("u01", "color")
        .with_zone_values(0, vec!["red".into(), "blue".into()])
        .with_zone_values(1, vec!["green".into(), "blue".into()])
        .build();

    // Zone 0 should maybe contain red and blue, not green
    assert!(idx.contains_in_zone(0, &ScalarValue::from(json!("red"))));
    assert!(idx.contains_in_zone(0, &ScalarValue::from(json!("blue"))));
    assert!(!idx.contains_in_zone(0, &ScalarValue::from(json!("green"))));

    // Zone 1 should maybe contain green and blue, not red
    assert!(idx.contains_in_zone(1, &ScalarValue::from(json!("green"))));
    assert!(idx.contains_in_zone(1, &ScalarValue::from(json!("blue"))));
    assert!(!idx.contains_in_zone(1, &ScalarValue::from(json!("red"))));

    // zones_maybe_containing should list correct zones for a shared value
    let zones_for_blue = idx.zones_maybe_containing(&ScalarValue::from(json!("blue")));
    assert!(zones_for_blue.contains(&0));
    assert!(zones_for_blue.contains(&1));
}

#[test]
fn zxf_roundtrip_save_and_load() {
    use crate::engine::core::zone::zone_xor_index::ZoneXorFilterIndex;
    use tempfile::tempdir;

    let idx = ZoneXorFilterIndexFactory::new("u01", "fruit")
        .with_zone_values(0, vec!["apple".into(), "banana".into()])
        .with_zone_values(1, vec!["carrot".into()])
        .build();

    let dir = tempdir().unwrap();
    let path = dir.path().join("u01_fruit.zxf");
    idx.save(&path).unwrap();

    let loaded = ZoneXorFilterIndex::load(&path).unwrap();
    assert!(loaded.contains_in_zone(0, &ScalarValue::from(json!("apple"))));
    assert!(loaded.contains_in_zone(0, &ScalarValue::from(json!("banana"))));
    assert!(!loaded.contains_in_zone(0, &ScalarValue::from(json!("carrot"))));
    assert!(loaded.contains_in_zone(1, &ScalarValue::from(json!("carrot"))));
}
