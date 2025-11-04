use super::bitmap::NullBitmap;

#[test]
fn size_for_calculates_correct_size() {
    assert_eq!(NullBitmap::size_for(0, 0), 0);
    assert_eq!(NullBitmap::size_for(1, 1), 1); // 1 bit, rounded up to 1 byte
    assert_eq!(NullBitmap::size_for(1, 8), 1); // 8 bits, exactly 1 byte
    assert_eq!(NullBitmap::size_for(1, 9), 2); // 9 bits, rounded up to 2 bytes
    assert_eq!(NullBitmap::size_for(2, 4), 1); // 8 bits, exactly 1 byte
    assert_eq!(NullBitmap::size_for(2, 5), 2); // 10 bits, rounded up to 2 bytes
    assert_eq!(NullBitmap::size_for(100, 10), 125); // 1000 bits, 125 bytes
}

#[test]
fn set_bit_sets_correct_bit() {
    let mut bitmap = vec![0u8; 2]; // 16 bits

    // Set bit 0 (first bit of first byte)
    NullBitmap::set_bit(&mut bitmap, 0);
    assert_eq!(bitmap[0], 0b00000001);
    assert_eq!(bitmap[1], 0b00000000);

    // Set bit 7 (last bit of first byte)
    NullBitmap::set_bit(&mut bitmap, 7);
    assert_eq!(bitmap[0], 0b10000001);
    assert_eq!(bitmap[1], 0b00000000);

    // Set bit 8 (first bit of second byte)
    NullBitmap::set_bit(&mut bitmap, 8);
    assert_eq!(bitmap[0], 0b10000001);
    assert_eq!(bitmap[1], 0b00000001);

    // Set bit 15 (last bit of second byte)
    NullBitmap::set_bit(&mut bitmap, 15);
    assert_eq!(bitmap[0], 0b10000001);
    assert_eq!(bitmap[1], 0b10000001);
}

#[test]
fn set_bit_can_set_multiple_bits() {
    let mut bitmap = vec![0u8; 1]; // 8 bits

    NullBitmap::set_bit(&mut bitmap, 0);
    NullBitmap::set_bit(&mut bitmap, 2);
    NullBitmap::set_bit(&mut bitmap, 4);
    NullBitmap::set_bit(&mut bitmap, 6);

    assert_eq!(bitmap[0], 0b01010101);
}

#[test]
fn is_null_checks_correct_bit() {
    let mut bitmap = vec![0u8; 2]; // 16 bits

    // All bits should be false initially
    for i in 0..16 {
        assert!(!NullBitmap::is_null(&bitmap, i));
    }

    // Set some bits
    NullBitmap::set_bit(&mut bitmap, 0);
    NullBitmap::set_bit(&mut bitmap, 5);
    NullBitmap::set_bit(&mut bitmap, 9);
    NullBitmap::set_bit(&mut bitmap, 15);

    // Check set bits
    assert!(NullBitmap::is_null(&bitmap, 0));
    assert!(NullBitmap::is_null(&bitmap, 5));
    assert!(NullBitmap::is_null(&bitmap, 9));
    assert!(NullBitmap::is_null(&bitmap, 15));

    // Check unset bits
    assert!(!NullBitmap::is_null(&bitmap, 1));
    assert!(!NullBitmap::is_null(&bitmap, 4));
    assert!(!NullBitmap::is_null(&bitmap, 8));
    assert!(!NullBitmap::is_null(&bitmap, 14));
}

#[test]
fn set_and_check_roundtrip() {
    let row_count = 10;
    let column_count = 5;
    let total_bits = row_count * column_count;
    let size = NullBitmap::size_for(row_count, column_count);
    let mut bitmap = vec![0u8; size];

    // Set all bits
    for i in 0..total_bits {
        NullBitmap::set_bit(&mut bitmap, i);
        assert!(NullBitmap::is_null(&bitmap, i));
    }

    // Verify all are set
    for i in 0..total_bits {
        assert!(NullBitmap::is_null(&bitmap, i));
    }
}

#[test]
fn set_bit_handles_large_indices() {
    let mut bitmap = vec![0u8; 100];

    // Set bits at various positions
    NullBitmap::set_bit(&mut bitmap, 0);
    NullBitmap::set_bit(&mut bitmap, 100);
    NullBitmap::set_bit(&mut bitmap, 500);
    NullBitmap::set_bit(&mut bitmap, 799); // Last bit of 100 bytes

    assert!(NullBitmap::is_null(&bitmap, 0));
    assert!(NullBitmap::is_null(&bitmap, 100));
    assert!(NullBitmap::is_null(&bitmap, 500));
    assert!(NullBitmap::is_null(&bitmap, 799));
}
