use crate::engine::core::ZoneIndex;
use crate::test_helpers::factories::ColumnOffsetsFactory;

#[test]
fn test_zone_index_populate_and_lookup() {
    let column_offsets = ColumnOffsetsFactory::new()
        .with_entry("signup_uid", "context_id", 0, vec![100], vec!["ctx1"])
        .with_entry("signup_uid", "context_id", 1, vec![200], vec!["ctx2"])
        .create();

    let mut index = ZoneIndex::default();
    index.populate(column_offsets.as_map(), "context_id");

    assert_eq!(index.index.len(), 1);
    let ctx1_zones = &index.index["signup_uid"]["ctx1"];
    let ctx2_zones = &index.index["signup_uid"]["ctx2"];
    assert_eq!(ctx1_zones, &[0]);
    assert_eq!(ctx2_zones, &[1]);
}
