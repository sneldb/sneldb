use crate::engine::core::ColumnOffsets;
use crate::engine::errors::{ColumnLoadError, QueryExecutionError};
use crate::shared::storage_header::{BinaryHeader, FileKind};
use memmap2::{Mmap, MmapOptions};
use std::fs::File;
use std::path::{Path, PathBuf};
use tracing::{debug, error, info};

#[derive(Debug)]
pub struct ColumnReader {
    uid: String,
    field: String,
    segment_dir: PathBuf,
    zone_mmap: Option<Mmap>,
    data_mmap: Option<Mmap>,
}

impl ColumnReader {
    pub fn new(uid: String, field: String, segment_dir: PathBuf) -> Self {
        Self {
            uid,
            field,
            segment_dir,
            zone_mmap: None,
            data_mmap: None,
        }
    }

    pub fn load_for_zone(
        segment_dir: &Path,
        segment_id: &str,
        uid: &str,
        field: &str,
        zone_id: u32,
    ) -> Result<Vec<String>, QueryExecutionError> {
        info!(
            target: "col_reader::zone_load",
            %segment_id,
            %uid,
            %field,
            zone_id,
            "Loading column values for specific zone"
        );

        let offsets = ColumnOffsets::load(segment_dir, segment_id, uid, field)
            .map_err(|e| QueryExecutionError::OffsetLoad(e.to_string()))?;

        if let Some(offset_vec) = offsets.get(&zone_id) {
            if offset_vec.len() >= 2 {
                return Self::load(segment_dir, segment_id, uid, field, offset_vec);
            } else {
                debug!(
                    target: "col_reader::zone_load",
                    zone_id,
                    count = offset_vec.len(),
                    "Zone has too few offsets"
                );
            }
        }

        Ok(Vec::new())
    }

    pub fn load(
        segment_dir: &Path,
        segment_id: &str,
        uid: &str,
        field: &str,
        offsets: &[u64],
    ) -> Result<Vec<String>, QueryExecutionError> {
        let col_path = segment_dir.join(format!("{}_{}.col", uid, field));
        info!(
            target: "col_reader::load",
            path = %col_path.display(),
            %segment_id,
            %uid,
            %field,
            count = offsets.len(),
            "Loading column data"
        );

        // Strict header validation
        let mut f = File::open(&col_path)
            .map_err(|e| QueryExecutionError::ColLoad(ColumnLoadError::Io(e)))?;
        let header = BinaryHeader::read_from(&mut f)
            .map_err(|e| QueryExecutionError::ColRead(format!("Header read failed: {}", e)))?;
        if header.magic != FileKind::SegmentColumn.magic() {
            return Err(QueryExecutionError::ColRead(
                "invalid magic for .col".into(),
            ));
        }

        let file = File::open(&col_path)
            .map_err(|e| QueryExecutionError::ColLoad(ColumnLoadError::Io(e)))?;
        let mmap = unsafe {
            MmapOptions::new()
                .map(&file)
                .map_err(|e| QueryExecutionError::ColLoad(ColumnLoadError::Io(e)))?
        };

        let mut values = Vec::with_capacity(offsets.len());

        for &start in offsets {
            let base = start as usize;
            if base + 2 > mmap.len() {
                error!(
                    target: "col_reader::error",
                    offset = base,
                    file_len = mmap.len(),
                    "Offset out of bounds"
                );
                return Err(QueryExecutionError::ColRead("Offset out of bounds".into()));
            }

            let len = u16::from_le_bytes(mmap[base..base + 2].try_into().unwrap()) as usize;
            let end = base + 2 + len;
            if end > mmap.len() {
                error!(
                    target: "col_reader::error",
                    base,
                    len,
                    end,
                    file_len = mmap.len(),
                    "Value length out of bounds"
                );
                return Err(QueryExecutionError::ColRead(
                    "Value length out of bounds".into(),
                ));
            }

            let val_bytes = &mmap[base + 2..end];
            match String::from_utf8(val_bytes.to_vec()) {
                Ok(val) => {
                    debug!(
                        target: "col_reader::value",
                        offset = base,
                        length = len,
                        value = %val,
                        "Read string value"
                    );
                    values.push(val);
                }
                Err(e) => {
                    error!(
                        target: "col_reader::error",
                        offset = base,
                        length = len,
                        "Invalid UTF-8: {e}"
                    );
                    return Err(QueryExecutionError::ColRead(format!(
                        "Invalid UTF-8: {}",
                        e
                    )));
                }
            }
        }

        Ok(values)
    }
}
