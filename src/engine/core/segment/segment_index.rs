use super::segment_id::{LEVEL_SPAN, SegmentId};
use crate::engine::errors::StoreError;
use crate::shared::storage_header::{BinaryHeader, FileKind};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use tracing::{debug, error, info, warn};

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

    fn remove_uid(&mut self, level: u32, offset: u32, uid: &str) -> Option<SegmentEntry> {
        let mut removed_entry = None;
        let should_remove_level = {
            let level_map = match self.by_level.get_mut(&level) {
                Some(map) => map,
                None => {
                    tracing::warn!(
                        target: "segment_index::remove_uid",
                        level = level,
                        offset = offset,
                        uid = %uid,
                        "Level not found in index tree"
                    );
                    return None;
                }
            };

            let mut remove_offset = false;
            if let Some(entry) = level_map.get_mut(&offset) {
                let before_uids = entry.uids.clone();
                let before_count = entry.uids.len();
                entry.uids.retain(|existing| existing != uid);
                let after_count = entry.uids.len();

                tracing::warn!(
                    target: "segment_index::remove_uid",
                    level = level,
                    offset = offset,
                    uid = %uid,
                    uids_before = ?before_uids,
                    uids_after = ?entry.uids,
                    before_count = before_count,
                    after_count = after_count,
                    "Removed UID from segment entry"
                );

                if before_count != after_count && entry.uids.is_empty() {
                    remove_offset = true;
                    tracing::warn!(
                        target: "segment_index::remove_uid",
                        level = level,
                        offset = offset,
                        uid = %uid,
                        "Segment entry is now empty, will be removed"
                    );
                }
            } else {
                tracing::warn!(
                    target: "segment_index::remove_uid",
                    level = level,
                    offset = offset,
                    uid = %uid,
                    "Offset not found in level map"
                );
                return None;
            }

            if remove_offset {
                removed_entry = level_map.remove(&offset);
            }
            level_map.is_empty()
        };

        if should_remove_level {
            self.by_level.remove(&level);
            tracing::warn!(
                target: "segment_index::remove_uid",
                level = level,
                uid = %uid,
                "Removed empty level from index tree"
            );
        }

        removed_entry
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
    /// Handles crash recovery: if index is corrupted, attempts to recover from backup or rebuild from disk.
    pub async fn load(shard_dir: &Path) -> Result<Self, StoreError> {
        let path = shard_dir.join("segments.idx");
        let tmp_path = shard_dir.join("segments.idx.tmp");

        // Clean up any leftover temporary files from crashed writes
        if tmp_path.exists() {
            warn!(target: "segment_index::load", ?tmp_path, "Found leftover temporary index file, removing");
            let _ = std::fs::remove_file(&tmp_path);
        }

        if path.exists() {
            // Try to load the index
            match Self::try_load_index(&path).await {
                Ok(index) => Ok(index),
                Err(e) => {
                    error!(
                        target: "segment_index::load",
                        ?path,
                        error = %e,
                        "Failed to load segment index, attempting recovery"
                    );
                    // Attempt recovery: rebuild index from actual segment directories on disk
                    Self::recover_from_disk(shard_dir).await
                }
            }
        } else {
            warn!(target: "segment_index::load", ?path, "No existing segment index found, attempting recovery from disk");
            // When index is missing, try to recover from disk
            Self::recover_from_disk(shard_dir).await
        }
    }

    /// Attempts to load the index file, returning error if corrupted.
    async fn try_load_index(path: &Path) -> Result<Self, StoreError> {
        let mut file = fs::File::open(path)?;
        let header = BinaryHeader::read_from(&mut file)?;
        if header.magic != FileKind::ShardSegmentIndex.magic() {
            return Err(StoreError::FlushFailed(
                "invalid magic for segments.idx".into(),
            ));
        }
        let reader = BufReader::new(file);
        let entries: Vec<SegmentEntry> = bincode::deserialize_from(reader).map_err(|e| {
            StoreError::FlushFailed(format!("Failed to deserialize segment index: {}", e))
        })?;
        info!(target: "segment_index::load", ?path, count = entries.len(), "Loaded segment index");
        let mut tree = SegmentIndexTree::default();
        for entry in entries {
            tree.insert(entry);
        }
        Ok(Self {
            path: path.to_path_buf(),
            tree,
        })
    }

    /// Recovers segment index by scanning disk for actual segment directories.
    /// This is a fallback when the index file is corrupted or missing.
    async fn recover_from_disk(shard_dir: &Path) -> Result<Self, StoreError> {
        warn!(
            target: "segment_index::recover_from_disk",
            ?shard_dir,
            "Recovering segment index by scanning disk"
        );

        use std::fs;
        let mut tree = SegmentIndexTree::default();
        let mut recovered_count = 0;

        // Scan shard directory for segment directories (format: 00000, 10000, 20000, etc.)
        if let Ok(entries) = fs::read_dir(shard_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    if let Some(dir_name) = path.file_name().and_then(|n| n.to_str()) {
                        // Check if it looks like a segment directory (5 digits)
                        if dir_name.len() == 5 && dir_name.chars().all(|c| c.is_ascii_digit()) {
                            if let Ok(segment_id) = dir_name.parse::<u32>() {
                                // Try to discover UIDs by scanning for .zones files
                                let mut uids = Vec::new();
                                if let Ok(zone_files) = fs::read_dir(&path) {
                                    for zone_file in zone_files.flatten() {
                                        if let Some(name) = zone_file.file_name().to_str() {
                                            if name.ends_with(".zones") {
                                                if let Some(uid) = name.strip_suffix(".zones") {
                                                    uids.push(uid.to_string());
                                                }
                                            }
                                        }
                                    }
                                }

                                if !uids.is_empty() {
                                    tree.insert(SegmentEntry {
                                        id: segment_id,
                                        uids,
                                    });
                                    recovered_count += 1;
                                }
                            }
                        }
                    }
                }
            }
        }

        let recovered_index = Self {
            path: shard_dir.join("segments.idx"),
            tree,
        };

        // Save the recovered index
        if recovered_count > 0 {
            if let Err(e) = recovered_index.save(shard_dir).await {
                warn!(
                    target: "segment_index::recover_from_disk",
                    ?shard_dir,
                    error = %e,
                    "Failed to save recovered index"
                );
            }
        }

        info!(
            target: "segment_index::recover_from_disk",
            ?shard_dir,
            recovered_count,
            "Recovered segment index from disk"
        );

        Ok(recovered_index)
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

    pub fn retire_uid_from_labels<'a, I>(&mut self, uid: &str, labels: I) -> Vec<SegmentEntry>
    where
        I: IntoIterator<Item = &'a str>,
    {
        let mut drained = Vec::new();
        let labels_vec: Vec<&str> = labels.into_iter().collect();

        tracing::warn!(
            target: "segment_index::retire_uid_from_labels",
            uid = %uid,
            labels_to_process = labels_vec.len(),
            labels = ?labels_vec,
            "Starting UID retirement from labels"
        );

        for label in &labels_vec {
            if let Some(seg) = SegmentId::from_str(label) {
                let level = seg.level();
                let offset = seg.id % LEVEL_SPAN;

                // Check state before removal
                let before_state = self
                    .tree
                    .by_level
                    .get(&level)
                    .and_then(|level_map| level_map.get(&offset))
                    .map(|e| e.uids.clone());

                if let Some(entry) = self.tree.remove_uid(level, offset, uid) {
                    tracing::warn!(
                        target: "segment_index::retire_uid_from_labels",
                        uid = %uid,
                        label = %label,
                        level = level,
                        offset = offset,
                        uids_before = ?before_state,
                        "Segment fully drained after UID retirement"
                    );
                    drained.push(entry);
                } else if let Some(uids_before) = before_state {
                    tracing::warn!(
                        target: "segment_index::retire_uid_from_labels",
                        uid = %uid,
                        label = %label,
                        level = level,
                        offset = offset,
                        uids_before = ?uids_before,
                        "UID retired but segment still has other UIDs"
                    );
                } else {
                    tracing::warn!(
                        target: "segment_index::retire_uid_from_labels",
                        uid = %uid,
                        label = %label,
                        level = level,
                        offset = offset,
                        "Segment not found in index"
                    );
                }
            } else {
                tracing::warn!(
                    target: "segment_index::retire_uid_from_labels",
                    uid = %uid,
                    label = %label,
                    "Failed to parse segment label"
                );
            }
        }

        if !drained.is_empty() {
            debug!(
                target: "segment_index::retire_uid_from_labels",
                uid = %uid,
                drained = drained.len(),
                "Fully drained segment entries after UID retirement"
            );
        }

        tracing::warn!(
            target: "segment_index::retire_uid_from_labels",
            uid = %uid,
            total_drained = drained.len(),
            drained_labels = ?drained.iter().map(|e| e.label()).collect::<Vec<_>>(),
            "Completed UID retirement"
        );

        drained
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

    /// Persists the current index to disk atomically using write-then-rename pattern.
    /// This ensures crash safety: if the write fails mid-way, the old index remains intact.
    pub async fn save(&self, shard_dir: &Path) -> Result<(), StoreError> {
        let path = shard_dir.join("segments.idx");
        let mut tmp_path = path.clone();
        tmp_path.set_extension("idx.tmp");

        // Write to temporary file first
        let file = File::create(&tmp_path)?;
        let mut writer = BufWriter::new(file);
        let header = BinaryHeader::new(FileKind::ShardSegmentIndex.magic(), 1, 0);
        header.write_to(&mut writer)?;
        let entries = self.tree.flatten();
        bincode::serialize_into(&mut writer, &entries)?;

        // Flush and sync to ensure data is on disk before atomic rename
        writer.flush()?;
        writer.get_ref().sync_all()?;
        drop(writer); // Ensure file handle is closed before rename

        // Atomic rename: on most filesystems, this is an atomic operation
        std::fs::rename(&tmp_path, &path)?;

        // Sync parent directory to ensure rename is persisted
        if let Some(parent) = path.parent() {
            if let Ok(parent_file) = File::open(parent) {
                let _ = parent_file.sync_all();
            }
        }

        info!(target: "segment_index::save", ?path, count = entries.len(), "Saved segment index atomically");
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
