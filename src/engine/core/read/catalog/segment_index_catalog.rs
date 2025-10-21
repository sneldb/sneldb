use super::IndexKind;
use crate::shared::storage_header::{BinaryHeader, FileKind};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentIndexCatalog {
    pub uid: String,
    pub segment_id: String,
    /// field name -> kinds available for this field within this segment
    pub field_kinds: HashMap<String, IndexKind>,
    /// global (per-uid) kinds: zone index, RLTE, etc.
    pub global_kinds: IndexKind,
}

impl SegmentIndexCatalog {
    pub fn new(uid: String, segment_id: String) -> Self {
        Self {
            uid,
            segment_id,
            field_kinds: HashMap::new(),
            global_kinds: IndexKind::from_bits_truncate(0),
        }
    }

    pub fn set_field_kind(&mut self, field: &str, kinds: IndexKind) {
        self.field_kinds.insert(field.to_string(), kinds);
    }

    pub fn add_global_kind(&mut self, kind: IndexKind) {
        self.global_kinds |= kind;
    }

    pub fn save(&self, path: &Path) -> std::io::Result<()> {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        BinaryHeader::new(FileKind::IndexCatalog.magic(), 1, 0).write_to(&mut f)?;
        let bytes = bincode::serialize(self)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        f.write_all(&bytes)?;
        Ok(())
    }

    pub fn load(path: &Path) -> std::io::Result<Self> {
        let mut f = OpenOptions::new().read(true).open(path)?;
        let hdr = BinaryHeader::read_from(&mut f)?;
        if hdr.magic != FileKind::IndexCatalog.magic() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid magic",
            ));
        }
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        bincode::deserialize(&buf).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    }
}
