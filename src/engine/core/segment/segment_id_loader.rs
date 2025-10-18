use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use tracing::{debug, info, warn};

/// Loads and manages segment IDs from the segment directory.
pub struct SegmentIdLoader {
    segment_base_dir: PathBuf,
}

impl SegmentIdLoader {
    /// Creates a new loader for the given segment directory.
    pub fn new(segment_base_dir: PathBuf) -> Self {
        Self { segment_base_dir }
    }

    /// Loads all segment IDs from the base directory, sorted in ascending order.
    pub fn load(&self) -> Vec<String> {
        let mut ids = Vec::new();

        match fs::read_dir(&self.segment_base_dir) {
            Ok(entries) => {
                for entry in entries.flatten() {
                    let file_name = entry.file_name();
                    if let Some(name) = file_name.to_str() {
                        if name.chars().all(|c| c.is_ascii_digit()) {
                            debug!(target: "segment_id_loader::load", name, "Found numeric segment directory");
                            ids.push(name.to_string());
                        }
                    }
                }
            }
            Err(e) => {
                warn!(
                    target: "segment_id_loader::load",
                    segment_base_dir = ?self.segment_base_dir,
                    error = ?e,
                    "Failed to read segment directory"
                );
            }
        }

        ids.sort();
        info!(target: "segment_id_loader::load", count = ids.len(), "Loaded segment IDs");
        ids
    }

    /// Determines the next available segment ID (max + 1) based on the given list.
    pub fn next_id(segment_ids: &Arc<RwLock<Vec<String>>>) -> u64 {
        let ids = segment_ids.read().unwrap();
        // Allocate in L0 range only: [0..=9999]
        let max_l0 = ids
            .iter()
            .filter_map(|id| id.parse::<u64>().ok())
            .filter(|&n| n < 10_000)
            .max();
        let next = max_l0.map_or(0, |max| max + 1);

        debug!(target: "segment_id_loader::next_id", next_id = next, "Calculated next segment ID");
        next
    }
}
