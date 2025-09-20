use crate::engine::errors::StoreError;
use crate::shared::storage_header::{BinaryHeader, FileKind, open_and_header_offset};
use memmap2::MmapOptions;
use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::engine::core::column::compression::{LeSliceReader, SIZE_U32, SIZE_U64};
const MIN_ENTRY_SIZE: usize = SIZE_U32 + SIZE_U64 + SIZE_U32 + SIZE_U32 + SIZE_U32;

#[derive(Clone, Debug)]
pub struct ZoneBlockEntry {
    pub zone_id: u32,
    pub block_start: u64,
    pub comp_len: u32,
    pub uncomp_len: u32,
    pub num_rows: u32,
    pub in_block_offsets: Vec<u32>,
}

#[derive(Default)]
pub struct CompressedColumnIndex {
    pub entries: HashMap<u32, ZoneBlockEntry>,
}

impl CompressedColumnIndex {
    pub fn path_for(uid: &str, field: &str, segment_dir: &Path) -> PathBuf {
        segment_dir.join(format!("{}_{}.zfc", uid, field))
    }

    pub fn write_to_path(&self, path: &Path) -> Result<(), StoreError> {
        let file = std::fs::File::create(path)?;
        let mut writer = std::io::BufWriter::new(file);
        BinaryHeader::new(FileKind::ZoneCompressedOffsets.magic(), 1, 0).write_to(&mut writer)?;

        let mut entries: Vec<_> = self.entries.values().cloned().collect();
        entries.sort_by_key(|e| e.zone_id);

        for e in entries {
            writer.write_all(&e.zone_id.to_le_bytes())?;
            writer.write_all(&e.block_start.to_le_bytes())?;
            writer.write_all(&e.comp_len.to_le_bytes())?;
            writer.write_all(&e.uncomp_len.to_le_bytes())?;
            writer.write_all(&e.num_rows.to_le_bytes())?;
            for off in &e.in_block_offsets {
                writer.write_all(&off.to_le_bytes())?;
            }
        }
        writer.flush()?;
        Ok(())
    }

    pub fn load_from_path(path: &Path) -> Result<Self, StoreError> {
        let (file, header_offset) =
            open_and_header_offset(path, FileKind::ZoneCompressedOffsets.magic())?;
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        let slice = &mmap[header_offset..];
        let mut reader = LeSliceReader::new(slice);
        let mut entries = HashMap::new();
        loop {
            if reader.remaining() < MIN_ENTRY_SIZE {
                break;
            }
            let zone_id = match reader.read_u32() {
                Some(v) => v,
                None => break,
            };
            let block_start = match reader.read_u64() {
                Some(v) => v,
                None => break,
            };
            let comp_len = match reader.read_u32() {
                Some(v) => v,
                None => break,
            };
            let uncomp_len = match reader.read_u32() {
                Some(v) => v,
                None => break,
            };
            let num_rows = match reader.read_u32() {
                Some(v) => v,
                None => break,
            };

            let needed = (num_rows as usize) * SIZE_U32;
            if !reader.has_bytes(needed) {
                break;
            }

            let mut in_block_offsets = Vec::with_capacity(num_rows as usize);
            for _ in 0..num_rows {
                if let Some(off) = reader.read_u32() {
                    in_block_offsets.push(off);
                } else {
                    break;
                }
            }

            entries.insert(
                zone_id,
                ZoneBlockEntry {
                    zone_id,
                    block_start,
                    comp_len,
                    uncomp_len,
                    num_rows,
                    in_block_offsets,
                },
            );
        }
        Ok(Self { entries })
    }
}
