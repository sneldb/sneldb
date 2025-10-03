use crate::engine::core::CandidateZone;
use crate::engine::errors::StoreError;
use crate::shared::storage_header::{BinaryHeader, FileKind};
use memmap2::Mmap;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use tracing::{debug, error};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ZoneIndex {
    pub index: BTreeMap<String, BTreeMap<String, Vec<u32>>>,
}

struct MmapReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> MmapReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn read_u32(&mut self) -> Result<u32, StoreError> {
        if self.pos + 4 > self.data.len() {
            error!(target: "sneldb::index", pos = self.pos, len = self.data.len(), "read_u32 out of bounds");
            return Err(StoreError::FlushFailed(
                "Unexpected end of file".to_string(),
            ));
        }
        let value = u32::from_le_bytes(self.data[self.pos..self.pos + 4].try_into().unwrap());
        self.pos += 4;
        Ok(value)
    }

    fn read_string(&mut self) -> Result<String, StoreError> {
        let len = self.read_u32()? as usize;
        if len > 1024 * 1024 {
            error!(target: "sneldb::index", len, "String length too large");
            return Err(StoreError::FlushFailed(format!(
                "String length too large: {}",
                len
            )));
        }
        if self.pos + len > self.data.len() {
            error!(target: "sneldb::index", pos = self.pos, len, data_len = self.data.len(), "Unexpected end of file while reading string");
            return Err(StoreError::FlushFailed(
                "Unexpected end of file".to_string(),
            ));
        }
        let bytes = &self.data[self.pos..self.pos + len];
        self.pos += len;
        String::from_utf8(bytes.to_vec()).map_err(|e| {
            error!(target: "sneldb::index", error = %e, "Invalid UTF-8 in string");
            StoreError::FlushFailed(e.to_string())
        })
    }

    fn read_zones(&mut self) -> Result<Vec<u32>, StoreError> {
        let count = self.read_u32()? as usize;
        if count > 1_000_000 {
            error!(target: "sneldb::index", count, "zone_count too large");
            return Err(StoreError::FlushFailed(format!(
                "zone_count too large: {}",
                count
            )));
        }
        let mut zones = Vec::with_capacity(count);
        for _ in 0..count {
            zones.push(self.read_u32()?);
        }
        Ok(zones)
    }
}

impl ZoneIndex {
    pub fn insert(&mut self, event_type: &str, context_id: &str, id: u32) {
        self.index
            .entry(event_type.to_string())
            .or_default()
            .entry(context_id.to_string())
            .or_default()
            .push(id);
    }

    pub fn write_to_path<P: AsRef<Path>>(&self, path: P) -> Result<(), StoreError> {
        let file = File::create(&path).map_err(|e| {
            error!(target: "sneldb::index", error = %e, path = %path.as_ref().display(), "Failed to create file");
            StoreError::FlushFailed(e.to_string())
        })?;
        let mut writer = BufWriter::new(file);
        let header = BinaryHeader::new(FileKind::ZoneIndex.magic(), 1, 0);
        header.write_to(&mut writer)?;

        for (event_type, context_map) in &self.index {
            let event_type_bytes = event_type.as_bytes();
            writer.write_all(&(event_type_bytes.len() as u32).to_le_bytes())?;
            writer.write_all(event_type_bytes)?;

            writer.write_all(&(context_map.len() as u32).to_le_bytes())?;

            for (context_id, zones) in context_map {
                let context_bytes = context_id.as_bytes();
                writer.write_all(&(context_bytes.len() as u32).to_le_bytes())?;
                writer.write_all(context_bytes)?;

                writer.write_all(&(zones.len() as u32).to_le_bytes())?;
                for id in zones {
                    writer.write_all(&id.to_le_bytes())?;
                }
            }
        }

        Ok(())
    }

    pub fn load_from_path<P: AsRef<Path>>(path: P) -> Result<Self, StoreError> {
        let file = File::open(&path).map_err(|e| {
            error!(target: "sneldb::index", error = %e, path = %path.as_ref().display(), "Failed to open zone index file");
            StoreError::FlushFailed(e.to_string())
        })?;
        let mmap = unsafe {
            Mmap::map(&file).map_err(|e| {
                error!(target: "sneldb::index", error = %e, "Failed to memory-map zone index file");
                StoreError::FlushFailed(e.to_string())
            })?
        };

        if mmap.len() < BinaryHeader::TOTAL_LEN {
            return Err(StoreError::FlushFailed("index header missing".into()));
        }
        let mut tmp = &mmap[..];
        let header = BinaryHeader::read_from(&mut tmp)?;
        if header.magic != FileKind::ZoneIndex.magic() {
            return Err(StoreError::FlushFailed("invalid index magic".into()));
        }
        let mut reader = MmapReader::new(&mmap[BinaryHeader::TOTAL_LEN..]);
        let mut index = BTreeMap::new();

        while reader.pos < reader.data.len() {
            let event_type = reader.read_string()?;
            let context_count = reader.read_u32()?;
            let mut context_map = BTreeMap::new();

            for _ in 0..context_count {
                let context_id = reader.read_string()?;
                let zones = reader.read_zones()?;
                context_map.insert(context_id, zones);
            }

            index.insert(event_type, context_map);
        }

        Ok(ZoneIndex { index })
    }

    pub fn find_candidate_zones(
        &self,
        event_type: &str,
        context_id: Option<&str>,
        segment_id: &str,
    ) -> Vec<CandidateZone> {
        // Collect zone ids into a vector, then sort+dedup to avoid HashSet churn
        let mut zone_ids: Vec<u32> = Vec::new();

        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(target: "sneldb::index", event_type, ?context_id, segment_id, "Looking up candidate zones");
        }

        if let Some(context_map) = self.index.get(event_type) {
            match context_id {
                Some(ctx_id) => {
                    if let Some(row_ids) = context_map.get(ctx_id) {
                        zone_ids.reserve(row_ids.len());
                        zone_ids.extend(row_ids.iter().copied());
                    }
                }
                None => {
                    // Reserve once using a conservative estimate to reduce reallocations
                    let est: usize = context_map.values().map(|v| v.len()).sum();
                    zone_ids.reserve(est);
                    for (_, row_ids) in context_map {
                        zone_ids.extend(row_ids.iter().copied());
                    }
                }
            }
        }

        // Sort and dedup in-place
        zone_ids.sort_unstable();
        zone_ids.dedup();

        // Construct CandidateZone instances only for unique zone ids
        let mut out = Vec::with_capacity(zone_ids.len());
        for zone_id in zone_ids {
            out.push(CandidateZone::new(zone_id, segment_id.to_string()));
        }

        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(target: "sneldb::index", found = out.len(), "Candidate zones found");
        }
        out
    }

    pub fn get(&self, uid: &str) -> Option<&BTreeMap<String, Vec<u32>>> {
        self.index.get(uid)
    }
}
