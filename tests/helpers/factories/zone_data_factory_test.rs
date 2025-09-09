use crate::test_helpers::factories::ZoneDataFactory;

#[test]
fn test_zone_data_factory() {
    let zd = ZoneDataFactory::new()
        .with_offsets(&[10, 20])
        .with_values(&["a", "b"])
        .create();

    assert_eq!(zd.offsets, vec![10, 20]);
    assert_eq!(zd.values, vec!["a", "b"]);
}
