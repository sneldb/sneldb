use std::sync::Arc;

#[derive(Debug)]
pub struct DecompressedBlock {
    pub bytes: Arc<Vec<u8>>,
    pub size: usize,
}

impl DecompressedBlock {
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        let size = bytes.len();
        Self {
            bytes: Arc::new(bytes),
            size,
        }
    }
}
