use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Error, ErrorKind, Write};
use std::path::{Path, PathBuf};

use memmap2::MmapOptions;
use tracing::{debug, info, warn};
use xorf::{BinaryFuse8, Filter};

use crate::engine::core::ZonePlan;
use crate::engine::types::ScalarValue;
use crate::shared::hash::stable_hash64;
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

    pub fn contains_in_zone(&self, zone_id: u32, value: &ScalarValue) -> bool {
        let Some(filter) = self.filters.get(&zone_id) else {
            return false;
        };
        match value_to_string(value) {
            Some(s) => {
                let h = stable_hash64(&s);
                filter.contains(&h)
            }
            None => false,
        }
    }

    pub fn zones_maybe_containing(&self, value: &ScalarValue) -> Vec<u32> {
        let Some(s) = value_to_string(value) else {
            return Vec::new();
        };
        let h = stable_hash64(&s);
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
            use std::sync::Arc;
            let field_arc: Arc<str> = Arc::from(field);
            let mut values: Vec<String> = Vec::new();
            for event in &zone.events {
                if let Some(v) = event.payload.get(field_arc.as_ref()) {
                    if let Some(s) = value_to_string(v) {
                        values.push(s);
                    }
                }
            }

            if values.is_empty() {
                continue;
            }

            // Deduplicate values before hashing
            let mut unique_values: Vec<String> = values;
            unique_values.sort();
            unique_values.dedup();

            // Hash values and deduplicate hashes (handle hash collisions)
            let mut unique_hashes: std::collections::HashSet<u64> =
                std::collections::HashSet::with_capacity(unique_values.len());
            for value in &unique_values {
                unique_hashes.insert(stable_hash64(value));
            }

            // Convert HashSet to Vec for iterator (order doesn't matter)
            let hashes_vec: Vec<u64> = unique_hashes.into_iter().collect();
            let hashes_count = hashes_vec.len();
            match BinaryFuse8::try_from_iterator(hashes_vec.into_iter()) {
                Ok(filter) => index.put_zone_filter(zone.id, filter),
                Err(e) => {
                    // BinaryFuse8 can fail to construct even with valid, deduplicated hashes
                    // due to algorithmic limitations with certain hash distributions.
                    // This is non-fatal - we just skip building the filter for this zone.
                    if tracing::enabled!(tracing::Level::WARN) {
                        warn!(
                            target: "sneldb::zxf",
                            zone_id = zone.id,
                            field,
                            unique_values_count = unique_values.len(),
                            unique_hashes_count = hashes_count,
                            error = ?e,
                            "Failed to build BinaryFuse8 filter - skipping zone filter (this is non-fatal)"
                        );
                    }
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
        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(target: "sneldb::zxf", path = %path.display(), uid = %self.uid, field = %self.field, zone_count = self.filters.len(), "Saving .zxf");
        }

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

fn value_to_string(value: &ScalarValue) -> Option<String> {
    match value {
        ScalarValue::Utf8(s) => Some(s.clone()),
        ScalarValue::Int64(i) => Some(i.to_string()),
        ScalarValue::Timestamp(ts) => Some(ts.to_string()),
        ScalarValue::Float64(f) => Some(f.to_string()),
        ScalarValue::Boolean(b) => Some(b.to_string()),
        _ => None,
    }
}

// Removed local hasher; use crate::shared::hash::stable_hash64

/// Builds .zxf only for fields present in `allowed_fields`.
pub fn build_all_zxf_filtered(
    zone_plans: &[ZonePlan],
    segment_dir: &Path,
    allowed_fields: &std::collections::HashSet<String>,
) -> std::io::Result<()> {
    if zone_plans.is_empty() {
        return Ok(());
    }
    let uid = zone_plans[0].uid.clone();
    // Use HashSet for O(1) lookups instead of O(n) Vec::contains()
    let mut all_fields: std::collections::HashSet<String> = std::collections::HashSet::new();
    for z in zone_plans {
        for e in &z.events {
            for k in e.payload.keys() {
                all_fields.insert(k.as_ref().to_string());
            }
        }
    }
    for field in all_fields {
        if !allowed_fields.contains(&field) {
            continue;
        }
        if let Some(index) = ZoneXorFilterIndex::build_for_field(&uid, &field, zone_plans) {
            let path = ZoneXorFilterIndex::file_path(segment_dir, &uid, &field);
            index.save(&path)?;
            if tracing::enabled!(tracing::Level::INFO) {
                info!(target: "sneldb::zxf", uid = %uid, field = %field, path = %path.display(), zone_count = index.filters.len(), "Wrote .zxf index");
            }
        }
    }
    Ok(())
}
