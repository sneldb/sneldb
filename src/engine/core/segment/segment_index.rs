use crate::engine::errors::StoreError;
use crate::shared::config::CONFIG;
use crate::shared::storage_header::{BinaryHeader, FileKind};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use tracing::{debug, info, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentEntry {
    pub level: u32,
    pub label: String,
    pub counter: u32,
    pub uids: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SegmentIndex {
    pub entries: Vec<SegmentEntry>,
    pub path: PathBuf,
}

impl SegmentIndex {
    /// Loads an existing index or initializes a new empty one if missing.
    pub async fn load(shard_dir: &Path) -> Result<Self, StoreError> {
        let path = shard_dir.join("segments.idx");
        if path.exists() {
            let mut file = fs::File::open(&path)?;
            let header = BinaryHeader::read_from(&mut file)?;
            if header.magic != FileKind::ShardSegmentIndex.magic() {
                return Err(StoreError::FlushFailed(
                    "invalid magic for segments.idx".into(),
                ));
            }
            let reader = BufReader::new(file);
            let entries: Vec<SegmentEntry> = bincode::deserialize_from(reader)?;
            info!(target: "segment_index::load", ?path, count = entries.len(), "Loaded segment index");
            Ok(Self { entries, path })
        } else {
            warn!(target: "segment_index::load", ?path, "No existing segment index found, initializing new");
            Ok(Self {
                entries: Vec::new(),
                path,
            })
        }
    }

    /// Appends a new entry and saves the index.
    pub async fn append(&mut self, entry: SegmentEntry) -> Result<(), StoreError> {
        debug!(
            target: "segment_index::append",
            level = entry.level,
            label = %entry.label,
            counter = entry.counter,
            uids = ?entry.uids,
            "Appending new segment entry"
        );
        self.entries.push(entry);
        self.save(self.path.parent().unwrap()).await
    }

    /// Returns all entries containing the given UID.
    pub fn list_for_uid(&self, uid: &str) -> Vec<&SegmentEntry> {
        let matches: Vec<&SegmentEntry> = self
            .entries
            .iter()
            .filter(|e| e.uids.contains(&uid.to_string()))
            .collect();
        debug!(target: "segment_index::list_for_uid", uid = %uid, matches = matches.len(), "Listing segments for UID");
        matches
    }

    /// Returns all segment labels.
    pub fn all_labels(&self) -> Vec<String> {
        let labels: Vec<String> = self.entries.iter().map(|e| e.label.clone()).collect();
        debug!(target: "segment_index::all_labels", count = labels.len(), "Collected all segment labels");
        labels
    }

    /// Persists the current index to disk.
    pub async fn save(&self, shard_dir: &Path) -> Result<(), StoreError> {
        let path = shard_dir.join("segments.idx");
        let file = File::create(&path)?;
        let mut writer = BufWriter::new(file);
        let header = BinaryHeader::new(FileKind::ShardSegmentIndex.magic(), 1, 0);
        header.write_to(&mut writer)?;
        bincode::serialize_into(&mut writer, &self.entries)?;
        info!(target: "segment_index::save", ?path, count = self.entries.len(), "Saved segment index");
        Ok(())
    }

    /// Returns the highest current level (e.g., 0, 1, 2, ...)
    pub fn current_level(&self) -> u32 {
        let level = self.entries.iter().map(|e| e.level).max().unwrap_or(0);
        debug!(target: "segment_index::current_level", current_level = level, "Computed current segment level");
        level
    }

    /// Determines whether any level exceeds the compaction threshold.
    pub fn needs_compaction(&self) -> bool {
        let mut counts: HashMap<u32, usize> = HashMap::new();
        for entry in &self.entries {
            *counts.entry(entry.level).or_default() += 1;
        }

        let threshold = CONFIG.engine.compaction_threshold;
        let needs = counts.iter().any(|(_, &count)| count >= threshold);

        debug!(
            target: "segment_index::needs_compaction",
            threshold,
            ?counts,
            needs_compaction = needs,
            "Checked compaction necessity"
        );
        needs
    }
}
