use crate::engine::core::column::reader::null_bitmap::NullBitmap;

#[test]
fn null_bitmap_checks_bits() {
    let nb = NullBitmap::new(Some(&[0b0001_0010]));
    assert_eq!(nb.is_null(0), false);
    assert_eq!(nb.is_null(1), true);
    assert_eq!(nb.is_null(4), true);
    assert_eq!(nb.is_null(5), false);
}

#[test]
fn null_bitmap_none_always_false() {
    let nb = NullBitmap::new(None);
    for i in 0..8 {
        assert_eq!(nb.is_null(i), false);
    }
}


