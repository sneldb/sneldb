use std::path::Path;
use std::sync::Arc;

use memmap2::{Mmap, MmapOptions};

use crate::engine::core::column::compression::CompressedColumnIndex;
use crate::shared::storage_header::{BinaryHeader, FileKind};

#[derive(Debug)]
pub struct ColumnHandle {
    pub col_mmap: Arc<Mmap>,
    pub zfc_index: Arc<CompressedColumnIndex>,
    pub col_path: std::path::PathBuf,
}

impl ColumnHandle {
    pub fn open(segment_dir: &Path, uid: &str, field: &str) -> std::io::Result<Self> {
        let zfc_path = segment_dir.join(format!("{}_{}.zfc", uid, field));
        let zfc_index = CompressedColumnIndex::load_from_path(&zfc_path)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let col_path = segment_dir.join(format!("{}_{}.col", uid, field));
        {
            let mut f = std::fs::File::open(&col_path)?;
            let header = BinaryHeader::read_from(&mut f)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            if header.magic != FileKind::SegmentColumn.magic() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "invalid magic for .col",
                ));
            }
        }
        let file = std::fs::File::open(&col_path)?;
        let mmap = unsafe { MmapOptions::new().map(&file)? };

        Ok(Self {
            col_mmap: Arc::new(mmap),
            zfc_index: Arc::new(zfc_index),
            col_path,
        })
    }
}
