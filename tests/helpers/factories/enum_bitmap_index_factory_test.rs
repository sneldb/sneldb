use crate::test_helpers::factories::EnumBitmapIndexFactory;

#[test]
fn builds_with_defaults() {
    let index = EnumBitmapIndexFactory::new().build();

    assert_eq!(index.variants, vec!["pro", "basic"]);
    assert_eq!(index.rows_per_zone, 8);
    assert!(index.zone_bitmaps.is_empty());
    assert!(!index.has_any(0, 0));
}

#[test]
fn sets_bits_for_specific_variant_in_zone() {
    let rows = 10u16;
    let index = EnumBitmapIndexFactory::new()
        .with_variants(&["pro", "basic", "trial"])
        .with_rows_per_zone(rows)
        .with_zone_variant_bits(0, 0, &[0, 3, 9])
        .build();

    // Expect 2 bytes per bitmap for 10 rows
    let expected_len = ((rows as usize) + 7) / 8;
    let bitsets = index.zone_bitmaps.get(&0).expect("zone 0 missing");
    assert_eq!(bitsets.len(), 3);
    assert_eq!(bitsets[0].len(), expected_len);

    // Bits set: 0 and 3 in first byte => 0b00001001 = 0x09, and 9 in second byte => 0b00000010 = 0x02
    assert_eq!(bitsets[0], vec![0x09, 0x02]);
    assert!(index.has_any(0, 0));
    assert!(!index.has_any(1, 0));
}

#[test]
fn supports_multiple_variants_and_zones() {
    let index = EnumBitmapIndexFactory::new()
        .with_variants(&["pro", "basic", "trial"]) // 3 variants
        .with_rows_per_zone(8)
        .with_zone_variant_bits(1, 1, &[1]) // zone 1, variant 1 -> row 1
        .with_zone_variant_bits(1, 2, &[2, 3]) // zone 1, variant 2 -> rows 2,3
        .with_zone_variant_bits(2, 0, &[0, 7]) // zone 2, variant 0 -> rows 0,7
        .build();

    // Zone 1
    let z1 = index.zone_bitmaps.get(&1).expect("zone 1 missing");
    assert_eq!(z1.len(), 3);
    // variant 1 has bit 1 => 0b00000010
    assert_eq!(z1[1][0], 0b00000010);
    // variant 2 has bits 2 and 3 => 0b00001100
    assert_eq!(z1[2][0], 0b00001100);
    assert!(index.has_any(1, 1));
    assert!(index.has_any(1, 2));

    // Zone 2
    let z2 = index.zone_bitmaps.get(&2).expect("zone 2 missing");
    assert_eq!(z2.len(), 3);
    // variant 0 has bits 0 and 7 => 0b10000001
    assert_eq!(z2[0][0], 0b10000001);
    assert!(index.has_any(2, 0));
}
