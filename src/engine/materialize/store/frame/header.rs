use std::io::{Read, Write};

use crate::engine::materialize::MaterializationError;

pub const FRAME_VERSION: u32 = 1;
pub const FRAME_VERSION_V2: u32 = 2; // Version 2: columnar storage with metadata

#[derive(Debug, Clone)]
pub struct FrameHeader {
    pub schema_hash: u64,
    pub row_count: u32,
    pub column_count: u32,
    pub min_timestamp: u64,
    pub max_timestamp: u64,
    pub max_event_id: u64,
    pub uncompressed_len: u32,
    pub compressed_len: u32,
    pub null_bitmap_len: u32,
    pub checksum: u32,
}

impl FrameHeader {
    pub fn write_to<W: Write>(&self, mut writer: W) -> Result<(), MaterializationError> {
        writer.write_all(&self.schema_hash.to_le_bytes())?;
        writer.write_all(&self.row_count.to_le_bytes())?;
        writer.write_all(&self.column_count.to_le_bytes())?;
        writer.write_all(&self.min_timestamp.to_le_bytes())?;
        writer.write_all(&self.max_timestamp.to_le_bytes())?;
        writer.write_all(&self.max_event_id.to_le_bytes())?;
        writer.write_all(&self.uncompressed_len.to_le_bytes())?;
        writer.write_all(&self.compressed_len.to_le_bytes())?;
        writer.write_all(&self.null_bitmap_len.to_le_bytes())?;
        writer.write_all(&self.checksum.to_le_bytes())?;
        Ok(())
    }

    pub fn read_from<R: Read>(mut reader: R) -> Result<Self, MaterializationError> {
        let mut buf8 = [0u8; 8];
        let mut buf4 = [0u8; 4];

        reader.read_exact(&mut buf8)?;
        let schema_hash = u64::from_le_bytes(buf8);

        reader.read_exact(&mut buf4)?;
        let row_count = u32::from_le_bytes(buf4);

        reader.read_exact(&mut buf4)?;
        let column_count = u32::from_le_bytes(buf4);

        reader.read_exact(&mut buf8)?;
        let min_timestamp = u64::from_le_bytes(buf8);
        reader.read_exact(&mut buf8)?;
        let max_timestamp = u64::from_le_bytes(buf8);
        reader.read_exact(&mut buf8)?;
        let max_event_id = u64::from_le_bytes(buf8);

        reader.read_exact(&mut buf4)?;
        let uncompressed_len = u32::from_le_bytes(buf4);
        reader.read_exact(&mut buf4)?;
        let compressed_len = u32::from_le_bytes(buf4);
        reader.read_exact(&mut buf4)?;
        let null_bitmap_len = u32::from_le_bytes(buf4);
        reader.read_exact(&mut buf4)?;
        let checksum = u32::from_le_bytes(buf4);

        Ok(Self {
            schema_hash,
            row_count,
            column_count,
            min_timestamp,
            max_timestamp,
            max_event_id,
            uncompressed_len,
            compressed_len,
            null_bitmap_len,
            checksum,
        })
    }
}
