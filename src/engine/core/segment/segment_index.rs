use super::segment_id::{LEVEL_SPAN, SegmentId};
use crate::engine::errors::StoreError;
use crate::shared::storage_header::{BinaryHeader, FileKind};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use tracing::{debug, info, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentEntry {
    pub id: u32,
    pub uids: Vec<String>,
}

impl SegmentEntry {
    #[inline]
    pub fn level(&self) -> u32 {
        SegmentId::from(self.id).level()
    }

    #[inline]
    pub fn offset_in_level(&self) -> u32 {
        self.id % LEVEL_SPAN
    }

    #[inline]
    pub fn label(&self) -> String {
        SegmentId::from(self.id).dir_name()
    }
}

#[derive(Debug, Default, Clone)]
struct SegmentIndexTree {
    by_level: BTreeMap<u32, BTreeMap<u32, SegmentEntry>>,
}

impl SegmentIndexTree {
    fn insert(&mut self, entry: SegmentEntry) {
        let level = entry.level();
        let offset = entry.offset_in_level();
        self.by_level
            .entry(level)
            .or_default()
            .insert(offset, entry);
    }

    fn remove_many<I>(&mut self, level: u32, offsets: I) -> Vec<SegmentEntry>
    where
        I: IntoIterator<Item = u32>,
    {
        if let Some(level_map) = self.by_level.get_mut(&level) {
            let mut removed = Vec::new();
            for offset in offsets {
                if let Some(entry) = level_map.remove(&offset) {
                    removed.push(entry);
                }
            }
            if level_map.is_empty() {
                self.by_level.remove(&level);
            }
            removed
        } else {
            Vec::new()
        }
    }

    fn iter_level(&self, level: u32) -> impl Iterator<Item = &SegmentEntry> {
        self.by_level
            .get(&level)
            .into_iter()
            .flat_map(|by_offset| by_offset.values())
    }

    fn iter_all(&self) -> impl Iterator<Item = &SegmentEntry> {
        self.by_level
            .values()
            .flat_map(|by_offset| by_offset.values())
    }

    fn len(&self) -> usize {
        self.by_level
            .values()
            .map(|level_map| level_map.len())
            .sum()
    }

    fn is_empty(&self) -> bool {
        self.by_level.is_empty()
    }

    fn current_level(&self) -> u32 {
        self.by_level.keys().copied().max().unwrap_or(0)
    }

    fn flatten(&self) -> Vec<SegmentEntry> {
        self.by_level
            .iter()
            .flat_map(|(_level, level_map)| level_map.values().cloned())
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct SegmentIndex {
    path: PathBuf,
    tree: SegmentIndexTree,
}

#[derive(Debug, Clone, Default)]
pub struct LevelStats {
    pub level: u32,
    pub count: usize,
}

impl SegmentIndex {
    pub fn path(&self) -> &Path {
        &self.path
    }

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
            let mut tree = SegmentIndexTree::default();
            for entry in entries {
                tree.insert(entry);
            }
            Ok(Self { path, tree })
        } else {
            warn!(target: "segment_index::load", ?path, "No existing segment index found, initializing new");
            Ok(Self {
                path,
                tree: SegmentIndexTree::default(),
            })
        }
    }

    pub fn insert_entry(&mut self, entry: SegmentEntry) {
        debug!(target: "segment_index::insert_entry", id = entry.id, level = entry.level(), uids = ?entry.uids, "Inserting segment entry");
        self.tree.insert(entry);
    }

    pub fn remove_many<I>(&mut self, level: u32, offsets: I) -> Vec<SegmentEntry>
    where
        I: IntoIterator<Item = u32>,
    {
        let removed = self.tree.remove_many(level, offsets);
        if !removed.is_empty() {
            debug!(target: "segment_index::remove_many", level, removed = removed.len(), "Removed segment entries at level");
        }
        removed
    }

    pub fn remove_labels<'a, I>(&mut self, labels: I) -> Vec<SegmentEntry>
    where
        I: IntoIterator<Item = &'a str>,
    {
        let mut by_level: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
        for label in labels {
            if let Some(seg) = SegmentId::from_str(label) {
                by_level
                    .entry(seg.level())
                    .or_default()
                    .push(seg.id % LEVEL_SPAN);
            }
        }
        let mut removed = Vec::new();
        for (level, offsets) in by_level {
            removed.extend(self.remove_many(level, offsets));
        }
        removed
    }

    pub fn entries_for_level(&self, level: u32) -> impl Iterator<Item = &SegmentEntry> {
        self.tree.iter_level(level)
    }

    pub fn iter_all(&self) -> impl Iterator<Item = &SegmentEntry> {
        self.tree.iter_all()
    }

    pub fn level_stats(&self) -> Vec<LevelStats> {
        self.tree
            .by_level
            .iter()
            .map(|(level, entries)| LevelStats {
                level: *level,
                count: entries.len(),
            })
            .collect()
    }

    pub fn list_for_uid(&self, uid: &str) -> Vec<&SegmentEntry> {
        let matches: Vec<&SegmentEntry> = self
            .iter_all()
            .filter(|entry| entry.uids.iter().any(|u| u == uid))
            .collect();
        debug!(target: "segment_index::list_for_uid", uid = %uid, matches = matches.len(), "Listing segments for UID");
        matches
    }

    pub fn all_labels(&self) -> Vec<String> {
        let labels: Vec<String> = self.iter_all().map(|entry| entry.label()).collect();
        debug!(target: "segment_index::all_labels", count = labels.len(), "Collected all segment labels");
        labels
    }

    pub fn len(&self) -> usize {
        self.tree.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tree.is_empty()
    }

    /// Persists the current index to disk.
    pub async fn save(&self, shard_dir: &Path) -> Result<(), StoreError> {
        let path = shard_dir.join("segments.idx");
        let file = File::create(&path)?;
        let mut writer = BufWriter::new(file);
        let header = BinaryHeader::new(FileKind::ShardSegmentIndex.magic(), 1, 0);
        header.write_to(&mut writer)?;
        let entries = self.tree.flatten();
        bincode::serialize_into(&mut writer, &entries)?;
        info!(target: "segment_index::save", ?path, count = entries.len(), "Saved segment index");
        Ok(())
    }

    pub async fn persist(&self) -> Result<(), StoreError> {
        if let Some(parent) = self.path.parent() {
            self.save(parent).await
        } else {
            Err(StoreError::FlushFailed(
                "Segment index path has no parent directory".into(),
            ))
        }
    }

    pub fn current_level(&self) -> u32 {
        let level = self.tree.current_level();
        debug!(target: "segment_index::current_level", current_level = level, "Computed current segment level");
        level
    }
}

#[cfg(test)]
impl SegmentIndex {
    pub fn from_entries(path: PathBuf, entries: Vec<SegmentEntry>) -> Self {
        let mut tree = SegmentIndexTree::default();
        for entry in entries {
            tree.insert(entry);
        }
        Self { path, tree }
    }
}
