use crate::test_helpers::factory::Factory;

#[test]
fn test_zone_meta_factory() {
    let meta = Factory::zone_meta()
        .with("zone_id", 3)
        .with("uid", "custom_uid")
        .with("timestamp_min", 1111)
        .with("timestamp_max", 9999)
        .create();

    assert_eq!(meta.zone_id, 3);
    assert_eq!(meta.uid, "custom_uid");
    assert_eq!(meta.timestamp_min, 1111);
    assert_eq!(meta.timestamp_max, 9999);
}
