use crate::engine::errors::StoreError;
use crate::shared::storage_header::{BinaryHeader, FileKind, open_and_header_offset};
use memmap2::MmapOptions;
use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};

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
        let mut pos = header_offset;
        let mut entries = HashMap::new();
        while pos + 4 + 8 + 4 + 4 + 4 <= mmap.len() {
            let zone_id = u32::from_le_bytes(mmap[pos..pos + 4].try_into().unwrap());
            pos += 4;
            let block_start = u64::from_le_bytes(mmap[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let comp_len = u32::from_le_bytes(mmap[pos..pos + 4].try_into().unwrap());
            pos += 4;
            let uncomp_len = u32::from_le_bytes(mmap[pos..pos + 4].try_into().unwrap());
            pos += 4;
            let num_rows = u32::from_le_bytes(mmap[pos..pos + 4].try_into().unwrap());
            pos += 4;
            let need = (num_rows as usize) * 4;
            if pos + need > mmap.len() {
                break;
            }
            let mut in_block_offsets = Vec::with_capacity(num_rows as usize);
            for _ in 0..num_rows {
                let off = u32::from_le_bytes(mmap[pos..pos + 4].try_into().unwrap());
                pos += 4;
                in_block_offsets.push(off);
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

pub fn zfc_path_for_key_in_jobs(
    key: &(String, String),
    write_jobs: &Vec<crate::engine::core::WriteJob>,
) -> PathBuf {
    let col = column_path_for_key_in_jobs(key, write_jobs);
    let mut s = col.to_string_lossy().to_string();
    if s.ends_with(".col") {
        s.truncate(s.len() - 4);
    }
    s.push_str(".zfc");
    PathBuf::from(s)
}

pub fn column_path_for_key_in_jobs(
    key: &(String, String),
    write_jobs: &Vec<crate::engine::core::WriteJob>,
) -> PathBuf {
    for job in write_jobs {
        if &job.key == key {
            return job.path.clone();
        }
    }
    PathBuf::from(format!("{}_{}.col", key.0, key.1))
}

