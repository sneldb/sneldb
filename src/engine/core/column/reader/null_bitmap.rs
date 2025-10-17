pub struct NullBitmap<'a> {
    bits: Option<&'a [u8]>,
}

impl<'a> NullBitmap<'a> {
    pub fn new(bits: Option<&'a [u8]>) -> Self { Self { bits } }
    #[inline]
    pub fn is_null(&self, idx: usize) -> bool {
        if let Some(b) = self.bits { (b[idx / 8] & (1 << (idx % 8))) != 0 } else { false }
    }
}

