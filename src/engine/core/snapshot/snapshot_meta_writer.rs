use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::Path;
use tracing::{debug, info};

use crate::engine::core::snapshot::snapshot_meta::SnapshotMeta;
use crate::engine::errors::StoreError;
use crate::shared::storage_header::{BinaryHeader, FileKind};

pub struct SnapshotMetaWriter<'a> {
    path: &'a Path,
}

impl<'a> SnapshotMetaWriter<'a> {
    pub fn new(path: &'a Path) -> Self {
        Self { path }
    }

    pub fn write_all(&self, metas: &[SnapshotMeta]) -> Result<(), StoreError> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(self.path)?;
        let mut writer = BufWriter::new(file);

        info!(target: "snapshot_meta_writer::write_all", path = %self.path.display(), count = metas.len(), "Writing snapshot meta file");

        BinaryHeader::new(FileKind::EventSnapshotMeta.magic(), 1, 0).write_to(&mut writer)?;
        let count: u32 = metas.len() as u32;
        writer.write_all(&count.to_le_bytes())?;

        for (idx, m) in metas.iter().enumerate() {
            let bytes =
                serde_json::to_vec(m).map_err(|e| StoreError::FlushFailed(e.to_string()))?;
            let len = bytes.len() as u32;
            writer.write_all(&len.to_le_bytes())?;
            writer.write_all(&bytes)?;
            debug!(target: "snapshot_meta_writer::write_all", index = idx, bytes = len, "Wrote meta record");
        }

        writer.flush()?;
        Ok(())
    }
}
