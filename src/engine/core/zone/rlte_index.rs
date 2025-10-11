use std::collections::HashMap;
use std::io::Write;
use std::path::Path;

use crate::engine::core::ZonePlan;
use crate::shared::storage_header::{BinaryHeader, FileKind};

/// Rank-Ladder Tail Envelope per-zone, per-field.
/// Stores geometric-rank samples of descending values for fast LB/UB estimation.
#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct RlteIndex {
    /// field_name -> zone_id -> ladder values (descending)
    pub ladders: HashMap<String, HashMap<u32, Vec<String>>>,
}

impl RlteIndex {
    pub fn new() -> Self {
        Self {
            ladders: HashMap::new(),
        }
    }

    /// Build RLTE for numeric and string-comparable fields from zone events.
    /// For now we include core fields: event_type, timestamp, and all payload fields. Excludes context_id.
    pub fn build_from_zones(zones: &[ZonePlan]) -> Self {
        let mut idx = Self::new();
        if zones.is_empty() {
            return idx;
        }
        // Determine rows per zone from first zone
        let rows_per_zone = zones[0].end_index - zones[0].start_index + 1;

        for zone in zones {
            // Collect all fields present in events (exclude context_id; it's sorted in LSM)
            let mut all_fields: std::collections::HashSet<String> =
                std::collections::HashSet::new();
            all_fields.insert("event_type".to_string());
            all_fields.insert("timestamp".to_string());
            for e in &zone.events {
                for f in e.collect_all_fields() {
                    if f != "context_id" {
                        all_fields.insert(f);
                    }
                }
            }

            for field in all_fields {
                // Gather values as sortable strings (zero-padded for numeric fields)
                let mut vals: Vec<String> = Vec::with_capacity(rows_per_zone);
                for e in &zone.events {
                    vals.push(e.get_field_value_sortable(&field));
                }

                // Sort descending lexicographically; numeric values are zero-padded so lexicographic ordering works correctly.
                vals.sort_by(|a, b| b.cmp(a));

                let ladder = Self::build_ladder(&vals);
                idx.ladders
                    .entry(field.clone())
                    .or_default()
                    .insert(zone.id, ladder);
            }
        }
        idx
    }

    /// Build geometric-rank ladder r in {1,2,4,8,...}
    fn build_ladder(sorted_desc: &[String]) -> Vec<String> {
        let mut r = 1usize;
        let mut out = Vec::new();
        while r <= sorted_desc.len() {
            out.push(sorted_desc[r - 1].clone());
            r <<= 1;
        }
        out
    }

    /// Persist RLTE for a uid as `{uid}.rlte` with header and a simple bincode payload.
    pub fn save(&self, uid: &str, segment_dir: &Path) -> std::io::Result<()> {
        let path = segment_dir.join(format!("{}.rlte", uid));
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&path)?;
        BinaryHeader::new(FileKind::ZoneRlte.magic(), 1, 0).write_to(&mut f)?;
        let bytes = bincode::serialize(self)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        f.write_all(&bytes)?;
        Ok(())
    }

    /// Load RLTE file for uid.
    pub fn load(uid: &str, segment_dir: &Path) -> std::io::Result<Self> {
        let path = segment_dir.join(format!("{}.rlte", uid));
        let mut reader = std::fs::File::open(&path)?;
        let header = BinaryHeader::read_from(&mut reader)?;
        if header.magic != FileKind::ZoneRlte.magic() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid magic for .rlte",
            ));
        }
        let bytes = std::fs::read(path)?;
        // Skip header when decoding
        let data = &bytes[BinaryHeader::TOTAL_LEN..];
        bincode::deserialize(data).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    }
}
