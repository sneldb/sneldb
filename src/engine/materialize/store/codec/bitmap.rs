pub struct NullBitmap;

impl NullBitmap {
    pub fn set_bit(bitmap: &mut [u8], index: usize) {
        let byte = index / 8;
        let bit = index % 8;
        bitmap[byte] |= 1 << bit;
    }

    pub fn is_null(bitmap: &[u8], index: usize) -> bool {
        let byte = index / 8;
        let bit = index % 8;
        (bitmap[byte] & (1 << bit)) != 0
    }

    pub fn size_for(row_count: usize, column_count: usize) -> usize {
        ((row_count * column_count) + 7) / 8
    }
}
