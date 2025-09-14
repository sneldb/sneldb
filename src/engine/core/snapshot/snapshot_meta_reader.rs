use std::fs::File;
use std::io::Read;
use std::path::Path;
use tracing::{debug, info, warn};

use crate::engine::core::snapshot::snapshot_meta::SnapshotMeta;
use crate::engine::errors::StoreError;
use crate::shared::storage_header::{BinaryHeader, FileKind};

pub struct SnapshotMetaReader<'a> {
    path: &'a Path,
}

impl<'a> SnapshotMetaReader<'a> {
    pub fn new(path: &'a Path) -> Self {
        Self { path }
    }

    pub fn read_all(&self) -> Result<Vec<SnapshotMeta>, StoreError> {
        let mut file = File::open(self.path)?;
        let meta_len = file.metadata()?.len();
        if meta_len == 0 {
            return Ok(Vec::new());
        }

        info!(target: "snapshot_meta_reader::read_all", path = %self.path.display(), size = meta_len, "Reading snapshot meta file");

        let header = BinaryHeader::read_from(&mut file)?;
        if header.magic != FileKind::EventSnapshotMeta.magic() {
            return Err(StoreError::FlushFailed(
                "invalid magic for snapshot meta".to_string(),
            ));
        }

        let mut u32buf = [0u8; 4];
        file.read_exact(&mut u32buf)?;
        let total = u32::from_le_bytes(u32buf) as usize;

        let mut metas = Vec::with_capacity(total);
        for idx in 0..total {
            let mut len_buf = [0u8; 4];
            if let Err(e) = file.read_exact(&mut len_buf) {
                warn!(target: "snapshot_meta_reader::read_all", index = idx, err = %e, "Unexpected EOF while reading length");
                break;
            }
            let len = u32::from_le_bytes(len_buf) as usize;
            let mut buf = vec![0u8; len];
            if let Err(e) = file.read_exact(&mut buf) {
                warn!(target: "snapshot_meta_reader::read_all", index = idx, err = %e, "Unexpected EOF while reading payload");
                break;
            }
            let meta: SnapshotMeta =
                serde_json::from_slice(&buf).map_err(|e| StoreError::FlushFailed(e.to_string()))?;
            debug!(target: "snapshot_meta_reader::read_all", index = idx, "Read meta record");
            metas.push(meta);
        }

        Ok(metas)
    }
}
