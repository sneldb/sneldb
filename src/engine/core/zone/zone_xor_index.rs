use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Error, ErrorKind, Write};
use std::path::{Path, PathBuf};

use memmap2::MmapOptions;
use serde_json::Value;
use tracing::{debug, info, warn};
use xorf::{BinaryFuse8, Filter};

use crate::engine::core::ZonePlan;
use crate::shared::storage_header::{BinaryHeader, FileKind, open_and_header_offset};

#[derive(Debug, Clone)]
pub struct ZoneXorFilterIndex {
    pub field: String,
    pub uid: String,
    /// zone_id -> BinaryFuse8 filter
    pub filters: HashMap<u32, BinaryFuse8>,
}

impl ZoneXorFilterIndex {
    pub fn new(uid: impl Into<String>, field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            uid: uid.into(),
            filters: HashMap::new(),
        }
    }

    pub fn put_zone_filter(&mut self, zone_id: u32, filter: BinaryFuse8) {
        self.filters.insert(zone_id, filter);
    }

    pub fn contains_in_zone(&self, zone_id: u32, value: &Value) -> bool {
        let Some(filter) = self.filters.get(&zone_id) else {
            return false;
        };
        match value_to_string(value) {
            Some(s) => {
                let h = crate::shared::hash::stable_hash64(&s);
                filter.contains(&h)
            }
            None => false,
        }
    }

    pub fn zones_maybe_containing(&self, value: &Value) -> Vec<u32> {
        let Some(s) = value_to_string(value) else {
            return Vec::new();
        };
        let h = crate::shared::hash::stable_hash64(&s);
        self.filters
            .iter()
            .filter_map(|(z, f)| if f.contains(&h) { Some(*z) } else { None })
            .collect()
    }

    pub fn file_path(segment_dir: &Path, uid: &str, field: &str) -> PathBuf {
        segment_dir.join(format!("{}_{}.zxf", uid, field))
    }

    /// Build per-zone XOR filters for a single field from zone plans
    pub fn build_for_field(
        uid: &str,
        field: &str,
        zones: &[ZonePlan],
    ) -> Option<ZoneXorFilterIndex> {
        let mut index = ZoneXorFilterIndex::new(uid.to_string(), field.to_string());

        for zone in zones {
            // Collect unique values for this field in this zone
            let mut values: Vec<String> = Vec::new();
            for event in &zone.events {
                if let Some(v) = event.payload.get(field) {
                    if let Some(s) = value_to_string(v) {
                        values.push(s);
                    }
                }
            }

            if values.is_empty() {
                continue;
            }

            values.sort();
            values.dedup();
            let hashes = values
                .into_iter()
                .map(|s| crate::shared::hash::stable_hash64(&s))
                .collect::<Vec<u64>>();
            match BinaryFuse8::try_from_iterator(hashes.into_iter()) {
                Ok(filter) => index.put_zone_filter(zone.id, filter),
                Err(e) => {
                    warn!(target: "sneldb::zxf", zone_id = zone.id, field, "Failed to build BinaryFuse8: {:?}", e);
                }
            }
        }

        if index.filters.is_empty() {
            None
        } else {
            Some(index)
        }
    }

    /// Save as: header | u32 zone_count | [entries...]
    /// Each entry: u32 zone_id | u32 blob_len | blob_bytes
    pub fn save(&self, path: &Path) -> std::io::Result<()> {
        debug!(target: "sneldb::zxf", path = %path.display(), uid = %self.uid, field = %self.field, zone_count = self.filters.len(), "Saving .zxf");

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        let header = BinaryHeader::new(FileKind::ZoneXorFilter.magic(), 1, 0);
        header.write_to(&mut file)?;

        file.write_all(&(self.filters.len() as u32).to_le_bytes())?;
        for (zone_id, filter) in &self.filters {
            let blob = bincode::serialize(filter)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            file.write_all(&zone_id.to_le_bytes())?;
            file.write_all(&(blob.len() as u32).to_le_bytes())?;
            file.write_all(&blob)?;
        }
        Ok(())
    }

    pub fn load(path: &Path) -> std::io::Result<Self> {
        let (file, offset) = open_and_header_offset(path, FileKind::ZoneXorFilter.magic())?;
        let mmap = unsafe { MmapOptions::new().map(&file)? };

        let mut pos = offset as usize;
        if mmap.len() < pos + 4 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "Incomplete .zxf header: missing count",
            ));
        }
        let count = u32::from_le_bytes(mmap[pos..pos + 4].try_into().unwrap());
        pos += 4;

        // We do not store uid/field inside the file; derive from filename if needed
        let (uid, field) = parse_uid_field_from_filename(path);
        let mut filters: HashMap<u32, BinaryFuse8> = HashMap::with_capacity(count as usize);
        for _ in 0..count {
            if mmap.len() < pos + 8 {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    "Incomplete .zxf entry header",
                ));
            }
            let zone_id = u32::from_le_bytes(mmap[pos..pos + 4].try_into().unwrap());
            pos += 4;
            let len = u32::from_le_bytes(mmap[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            if mmap.len() < pos + len {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    "Incomplete .zxf entry payload",
                ));
            }
            let buf = &mmap[pos..pos + len];
            pos += len;

            let filter: BinaryFuse8 =
                bincode::deserialize(buf).map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
            filters.insert(zone_id, filter);
        }

        Ok(ZoneXorFilterIndex {
            field,
            uid,
            filters,
        })
    }
}

fn parse_uid_field_from_filename(path: &Path) -> (String, String) {
    let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
    // Expect format: <uid>_<field>.zxf; fall back to empty strings
    if let Some(stripped) = name.strip_suffix(".zxf") {
        if let Some((uid, field)) = stripped.split_once('_') {
            return (uid.to_string(), field.to_string());
        }
    }
    (String::new(), String::new())
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => n
            .as_i64()
            .map(|i| i.to_string())
            .or_else(|| n.as_f64().map(|f| f.to_string()))
            .or_else(|| Some(n.to_string())),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

// Removed local hasher; use crate::shared::hash::stable_hash64

/// Builds and writes one .zxf per field for the given zone plans
pub fn build_all_zxf(zone_plans: &[ZonePlan], segment_dir: &Path) -> std::io::Result<()> {
    // Collect per-field per-zone values: iterate fields by scanning events in each zone
    // To limit cost, we build an index per (uid, field) across all zones
    if zone_plans.is_empty() {
        return Ok(());
    }
    let uid = zone_plans[0].uid.clone();

    // Discover fields from events
    let mut all_fields: Vec<String> = Vec::new();
    for z in zone_plans {
        for e in &z.events {
            for (k, _) in e.payload.as_object().into_iter().flat_map(|m| m.iter()) {
                if !all_fields.contains(k) {
                    all_fields.push(k.clone());
                }
            }
        }
    }

    for field in all_fields {
        if let Some(index) = ZoneXorFilterIndex::build_for_field(&uid, &field, zone_plans) {
            let path = ZoneXorFilterIndex::file_path(segment_dir, &uid, &field);
            index.save(&path)?;
            info!(target: "sneldb::zxf", uid = %uid, field = %field, path = %path.display(), zone_count = index.filters.len(), "Wrote .zxf index");
        }
    }

    Ok(())
}
