use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::Path;
use tracing::{debug, info};

use crate::engine::core::event::event::Event;
use crate::engine::errors::StoreError;
use crate::shared::storage_header::{BinaryHeader, FileKind};
use serde_json;

/// Writer for `.snp` snapshot files that store an array of `Event` records.
/// Format:
///   [BinaryHeader]
///   u32 num_events
///   repeated { u32 len, [len bytes of bincode(Event)] }
pub struct SnapshotWriter<'a> {
    path: &'a Path,
}

impl<'a> SnapshotWriter<'a> {
    pub fn new(path: &'a Path) -> Self {
        Self { path }
    }

    pub fn write_all(&self, events: &[Event]) -> Result<(), StoreError> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(self.path)?;
        let mut writer = BufWriter::new(file);

        info!(target: "snapshot_writer::write_all", path = %self.path.display(), count = events.len(), "Writing snapshot file");

        BinaryHeader::new(FileKind::EventSnapshot.magic(), 1, 0).write_to(&mut writer)?;

        let count: u32 = events.len() as u32;
        writer.write_all(&count.to_le_bytes())?;

        for (idx, ev) in events.iter().enumerate() {
            let bytes =
                serde_json::to_vec(ev).map_err(|e| StoreError::FlushFailed(e.to_string()))?;
            let len = bytes.len() as u32;
            writer.write_all(&len.to_le_bytes())?;
            writer.write_all(&bytes)?;
            debug!(target: "snapshot_writer::write_all", index = idx, bytes = len, "Wrote event record");
        }

        writer.flush()?;
        Ok(())
    }
}
