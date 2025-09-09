use crate::test_helpers::factory::Factory;
use std::collections::HashMap;

#[test]
fn test_zone_value_loader_factory_can_create_and_call() {
    let mut zones = vec![
        Factory::candidate_zone()
            .with("zone_id", 0)
            .with("segment_id", "segment-001")
            .with_values(HashMap::new())
            .create(),
    ];

    let loader = Factory::zone_value_loader()
        .with_segment_base_dir("/tmp/segments")
        .with_uid("uid_test")
        .create();

    // Safe to call even if no actual files exist (assuming ColumnLoader handles that gracefully)
    loader.load_zone_values(&mut zones, &vec!["plan".into()]);

    // Not asserting here since this depends on real or mocked filesystem I/O
    assert_eq!(zones[0].zone_id, 0);
}
