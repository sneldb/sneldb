use crate::engine::core::column::column_values::ColumnValues;
use crate::test_helpers::factories::DecompressedBlockFactory;

#[test]
fn factory_builds_block_with_values_and_ranges() {
    let (block, ranges) = DecompressedBlockFactory::create_with_ranges(&["a", "beta"]);
    let cv = ColumnValues::new(block, ranges);
    assert_eq!(cv.len(), 2);
    assert_eq!(cv.get_str_at(0), Some("a"));
    assert_eq!(cv.get_str_at(1), Some("beta"));
}

#[test]
fn builder_style_appends_values_and_creates_block() {
    let block = DecompressedBlockFactory::new()
        .with_values(&["x"]) // [len][x]
        .append_raw_bytes(&[0x02, 0x00, b'y', b'z']) // raw encoded "yz"
        .create();

    // Build ranges matching the two encoded values for verification
    // bytes layout: [01 00 'x'][02 00 'y' 'z']
    let ranges = vec![(2, 1), (5, 2)];
    let cv = ColumnValues::new(block, ranges);

    assert_eq!(cv.get_str_at(0), Some("x"));
    assert_eq!(cv.get_str_at(1), Some("yz"));
}


