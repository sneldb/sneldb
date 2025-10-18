use crate::command::types::CompareOp;
use crate::engine::core::zone::enum_zone_pruner::EnumZonePruner;
use crate::test_helpers::factories::EnumBitmapIndexFactory;

#[test]
fn prunes_eq_variant() {
    let index = EnumBitmapIndexFactory::new()
        .with_variants(&["free", "pro", "premium", "enterprise"])
        .with_rows_per_zone(2)
        .with_zone_variant_bits(0, 0, &[0]) // zone 0: free at row 0
        .with_zone_variant_bits(0, 1, &[1]) // zone 0: pro at row 1
        .with_zone_variant_bits(1, 2, &[0]) // zone 1: premium at row 0
        .with_zone_variant_bits(1, 3, &[1]) // zone 1: enterprise at row 1
        .build();
    let pruner = EnumZonePruner {
        segment_id: "001",
        ebm: &index,
    };

    // variant_id for "pro" is 1
    let zones = pruner.prune(&CompareOp::Eq, 1);
    assert_eq!(zones.len(), 1);
    assert_eq!(zones[0].zone_id, 0);
    assert_eq!(zones[0].segment_id, "001");
}

#[test]
fn prunes_neq_variant() {
    let index = EnumBitmapIndexFactory::new()
        .with_variants(&["free", "pro", "premium", "enterprise"])
        .with_rows_per_zone(2)
        .with_zone_variant_bits(0, 0, &[0])
        .with_zone_variant_bits(0, 1, &[1])
        .with_zone_variant_bits(1, 2, &[0])
        .with_zone_variant_bits(1, 3, &[1])
        .build();
    let pruner = EnumZonePruner {
        segment_id: "001",
        ebm: &index,
    };

    // variant_id for "pro" is 1 -> zones with other variants should be included (both z0 and z1)
    let zones = pruner.prune(&CompareOp::Neq, 1);
    assert_eq!(zones.len(), 2);
    assert!(zones.iter().any(|z| z.zone_id == 0));
    assert!(zones.iter().any(|z| z.zone_id == 1));
}
