use memmap2::{Mmap, MmapOptions};
use std::fs::File;
use std::path::Path;

use crate::engine::core::column::compression::ZoneBlockEntry;
use crate::engine::errors::{ColumnLoadError, QueryExecutionError};
use crate::shared::storage_header::{BinaryHeader, FileKind};

pub fn map_column_file(
    segment_dir: &Path,
    uid: &str,
    field: &str,
) -> Result<Mmap, QueryExecutionError> {
    let col_path = segment_dir.join(format!("{}_{}.col", uid, field));
    let mut f =
        File::open(&col_path).map_err(|e| QueryExecutionError::ColLoad(ColumnLoadError::Io(e)))?;
    let header = BinaryHeader::read_from(&mut f)
        .map_err(|e| QueryExecutionError::ColRead(format!("header read failed: {e}")))?;
    if header.magic != FileKind::SegmentColumn.magic() {
        return Err(QueryExecutionError::ColRead(
            "invalid magic for .col".into(),
        ));
    }
    let file =
        File::open(&col_path).map_err(|e| QueryExecutionError::ColLoad(ColumnLoadError::Io(e)))?;
    let mmap = unsafe {
        MmapOptions::new()
            .map(&file)
            .map_err(|e| QueryExecutionError::ColLoad(ColumnLoadError::Io(e)))?
    };
    Ok(mmap)
}

#[inline]
pub fn compressed_range(
    entry: &ZoneBlockEntry,
    mmap_len: usize,
) -> Result<(usize, usize), QueryExecutionError> {
    let start = entry.block_start as usize;
    let end = start + entry.comp_len as usize;
    if end > mmap_len {
        return Err(QueryExecutionError::ColRead(format!(
            "compressed block OOB: end={} > file_len={}",
            end, mmap_len
        )));
    }
    Ok((start, end))
}
