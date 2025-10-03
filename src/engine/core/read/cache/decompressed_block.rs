use std::sync::Arc;

#[derive(Debug)]
pub struct DecompressedBlock {
    data: Arc<Vec<u8>>,
    pub size: usize,
}

impl DecompressedBlock {
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        let size = bytes.len();
        Self {
            data: Arc::new(bytes),
            size,
        }
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.data[..]
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.size
    }
}
