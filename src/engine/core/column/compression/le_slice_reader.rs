pub const SIZE_U32: usize = 4;
pub const SIZE_U64: usize = 8;

pub struct LeSliceReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> LeSliceReader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    pub fn has_bytes(&self, n: usize) -> bool {
        self.remaining() >= n
    }

    pub fn read_u32(&mut self) -> Option<u32> {
        if !self.has_bytes(SIZE_U32) {
            return None;
        }
        let val = u32::from_le_bytes(self.buf[self.pos..self.pos + SIZE_U32].try_into().ok()?);
        self.pos += SIZE_U32;
        Some(val)
    }

    pub fn read_u64(&mut self) -> Option<u64> {
        if !self.has_bytes(SIZE_U64) {
            return None;
        }
        let val = u64::from_le_bytes(self.buf[self.pos..self.pos + SIZE_U64].try_into().ok()?);
        self.pos += SIZE_U64;
        Some(val)
    }
}
