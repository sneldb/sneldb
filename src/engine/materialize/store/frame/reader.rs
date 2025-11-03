use std::fs::File;
use std::io::Read;
use std::path::Path;

use crc32fast::Hasher as Crc32Hasher;

use crate::engine::materialize::MaterializationError;
use crate::shared::storage_header::{BinaryHeader, FileKind};

use super::data::FrameData;
use super::header::{FrameHeader, FRAME_VERSION};
use super::metadata::StoredFrameMeta;

pub struct FrameReader<'a> {
    frame_dir: &'a Path,
}

impl<'a> FrameReader<'a> {
    pub fn new(frame_dir: &'a Path) -> Self {
        Self { frame_dir }
    }

    pub fn read(&self, meta: &StoredFrameMeta) -> Result<FrameData, MaterializationError> {
        let frame_path = self.frame_dir.join(&meta.file_name);
        let mut file = File::open(&frame_path)?;

        let header = self.read_and_validate_header(&mut file, meta.schema_hash)?;

        let mut compressed = vec![0u8; header.compressed_len as usize];
        file.read_exact(&mut compressed)?;

        let mut crc = Crc32Hasher::new();
        crc.update(&compressed);
        let checksum = crc.finalize();
        if checksum != header.checksum {
            return Err(MaterializationError::Corrupt(format!(
                "Checksum mismatch for frame {}",
                meta.file_name
            )));
        }

        Ok(FrameData { header, compressed })
    }

    fn read_and_validate_header(
        &self,
        file: &mut File,
        expected_hash: u64,
    ) -> Result<FrameHeader, MaterializationError> {
        let header = BinaryHeader::read_from(&mut *file)?;
        if header.magic != FileKind::MaterializedFrame.magic() {
            return Err(MaterializationError::Header("Invalid frame magic".into()));
        }
        if header.version != FRAME_VERSION as u16 {
            return Err(MaterializationError::Header(format!(
                "Unsupported frame version {}",
                header.version
            )));
        }

        let frame_header = FrameHeader::read_from(&mut *file)?;
        if frame_header.schema_hash != expected_hash {
            return Err(MaterializationError::Corrupt(
                "Schema hash mismatch while reading frame".into(),
            ));
        }
        Ok(frame_header)
    }
}

