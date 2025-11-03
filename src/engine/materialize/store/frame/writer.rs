use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use crc32fast::Hasher as Crc32Hasher;

use crate::engine::materialize::high_water::HighWaterMark;
use crate::engine::materialize::MaterializationError;
use crate::shared::storage_header::{BinaryHeader, FileKind};

use super::header::{FrameHeader, FRAME_VERSION};
use super::metadata::StoredFrameMeta;
use super::super::codec::EncodedFrame;

pub struct FrameWriter<'a> {
    frame_dir: &'a Path,
}

impl<'a> FrameWriter<'a> {
    pub fn new(frame_dir: &'a Path) -> Self {
        Self { frame_dir }
    }

    pub fn write(
        &self,
        index: u64,
        frame: &EncodedFrame,
    ) -> Result<StoredFrameMeta, MaterializationError> {
        let file_name = format!("{:06}.mat", index);
        let frame_path = self.frame_dir.join(&file_name);

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&frame_path)?;

        BinaryHeader::new(FileKind::MaterializedFrame.magic(), FRAME_VERSION as u16, 0)
            .write_to(&mut file)
            .map_err(|e| MaterializationError::Header(e.to_string()))?;

        let mut crc = Crc32Hasher::new();
        crc.update(&frame.compressed);
        let checksum = crc.finalize();

        let header = FrameHeader {
            schema_hash: frame.schema_hash,
            row_count: frame.row_count,
            column_count: frame.schema.len() as u32,
            min_timestamp: frame.min_timestamp,
            max_timestamp: frame.max_timestamp,
            max_event_id: frame.max_event_id,
            uncompressed_len: frame.uncompressed_len,
            compressed_len: frame.compressed.len() as u32,
            null_bitmap_len: frame.null_bitmap_len,
            checksum,
        };
        header.write_to(&mut file)?;

        file.write_all(&frame.compressed)?;
        file.flush()?;
        file.sync_all()?;

        Ok(StoredFrameMeta {
            file_name,
            schema: frame.schema.clone(),
            schema_hash: frame.schema_hash,
            row_count: frame.row_count,
            min_timestamp: frame.min_timestamp,
            max_timestamp: frame.max_timestamp,
            max_event_id: frame.max_event_id,
            compressed_len: frame.compressed.len() as u32,
            uncompressed_len: frame.uncompressed_len,
            null_bitmap_len: frame.null_bitmap_len,
            high_water_mark: HighWaterMark::new(frame.max_timestamp, frame.max_event_id),
        })
    }
}

