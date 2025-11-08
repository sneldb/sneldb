use super::segment_batch::SegmentBatch;
use crate::engine::core::read::cache::{
    GlobalColumnBlockCache, GlobalColumnHandleCache, GlobalIndexCatalogCache, GlobalZoneIndexCache,
    GlobalZoneSurfCache,
};
use crate::engine::core::segment::segment_id::SegmentId;
use crate::engine::core::{SegmentEntry, SegmentIndex};
use crate::engine::errors::StoreError;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, warn};

pub trait SegmentCache: Send + Sync {
    fn invalidate_segment(&self, segment_label: &str);
}

struct ColumnCacheAdapter(&'static GlobalColumnHandleCache);

impl SegmentCache for ColumnCacheAdapter {
    fn invalidate_segment(&self, segment_label: &str) {
        self.0.invalidate_segment(segment_label);
    }
}

struct ZoneSurfCacheAdapter(&'static GlobalZoneSurfCache);

impl SegmentCache for ZoneSurfCacheAdapter {
    fn invalidate_segment(&self, segment_label: &str) {
        self.0.invalidate_segment(segment_label);
    }
}

struct ZoneIndexCacheAdapter(&'static GlobalZoneIndexCache);

impl SegmentCache for ZoneIndexCacheAdapter {
    fn invalidate_segment(&self, segment_label: &str) {
        self.0.invalidate_segment(segment_label);
    }
}

struct IndexCatalogCacheAdapter(&'static GlobalIndexCatalogCache);

impl SegmentCache for IndexCatalogCacheAdapter {
    fn invalidate_segment(&self, segment_label: &str) {
        self.0.invalidate_segment(segment_label);
    }
}

struct ColumnBlockCacheAdapter(&'static GlobalColumnBlockCache);

impl SegmentCache for ColumnBlockCacheAdapter {
    fn invalidate_segment(&self, segment_label: &str) {
        self.0.invalidate_segment(segment_label);
    }
}

pub struct CompactionHandover {
    shard_id: u32,
    shard_dir: PathBuf,
    segment_ids: Arc<RwLock<Vec<String>>>,
    flush_lock: Arc<tokio::sync::Mutex<()>>,
    column_cache: Arc<dyn SegmentCache>,
    zone_surf_cache: Arc<dyn SegmentCache>,
    zone_index_cache: Arc<dyn SegmentCache>,
    index_catalog_cache: Arc<dyn SegmentCache>,
    column_block_cache: Arc<dyn SegmentCache>,
}

impl CompactionHandover {
    pub fn new(
        shard_id: u32,
        shard_dir: PathBuf,
        segment_ids: Arc<RwLock<Vec<String>>>,
        flush_lock: Arc<tokio::sync::Mutex<()>>,
    ) -> Self {
        Self {
            shard_id,
            shard_dir,
            segment_ids,
            flush_lock,
            column_cache: Arc::new(ColumnCacheAdapter(GlobalColumnHandleCache::instance())),
            zone_surf_cache: Arc::new(ZoneSurfCacheAdapter(GlobalZoneSurfCache::instance())),
            zone_index_cache: Arc::new(ZoneIndexCacheAdapter(GlobalZoneIndexCache::instance())),
            index_catalog_cache: Arc::new(IndexCatalogCacheAdapter(
                GlobalIndexCatalogCache::instance(),
            )),
            column_block_cache: Arc::new(ColumnBlockCacheAdapter(
                GlobalColumnBlockCache::instance(),
            )),
        }
    }

    #[cfg(test)]
    pub fn with_caches(
        shard_id: u32,
        shard_dir: PathBuf,
        segment_ids: Arc<RwLock<Vec<String>>>,
        flush_lock: Arc<tokio::sync::Mutex<()>>,
        column_cache: Arc<dyn SegmentCache>,
        zone_surf_cache: Arc<dyn SegmentCache>,
        zone_index_cache: Arc<dyn SegmentCache>,
        index_catalog_cache: Arc<dyn SegmentCache>,
        column_block_cache: Arc<dyn SegmentCache>,
    ) -> Self {
        Self {
            shard_id,
            shard_dir,
            segment_ids,
            flush_lock,
            column_cache,
            zone_surf_cache,
            zone_index_cache,
            index_catalog_cache,
            column_block_cache,
        }
    }

    /// Commits a batch of UIDs compacted from the same input segments.
    /// Returns the list of drained segment labels that can be deleted later.
    pub async fn commit_batch(
        &self,
        batch: &SegmentBatch,
        new_entries: Vec<SegmentEntry>,
    ) -> Result<Vec<String>, StoreError> {
        let input_labels = batch.input_segment_labels.clone();

        info!(
            target: "compaction_handover::commit_batch",
            shard = self.shard_id,
            input_count = input_labels.len(),
            uid_count = new_entries.len(),
            "Applying batch compaction handover"
        );

        let drained_labels = {
            let _guard = self.flush_lock.lock().await;
            let mut index = SegmentIndex::load(&self.shard_dir).await?;

            // Log segment states before retirement
            let segments_before: Vec<_> = input_labels
                .iter()
                .filter_map(|l| {
                    index
                        .iter_all()
                        .find(|e| e.label() == *l)
                        .map(|e| (l.clone(), e.uids.clone()))
                })
                .collect();

            if tracing::enabled!(tracing::Level::DEBUG) {
                tracing::debug!(
                    target: "compaction_handover::commit_batch",
                    shard = self.shard_id,
                    input_labels = ?input_labels,
                    segments_before_retirement = segments_before.len(),
                    segment_states_before = ?segments_before,
                    total_entries_before = index.len(),
                    "About to retire UIDs from segments"
                );
            }

            // Retire all UIDs from the input segments
            // We need to deduplicate drained segments since the same segment might be returned
            // multiple times if it becomes empty after retiring different UIDs
            let mut all_drained: Vec<crate::engine::core::SegmentEntry> = Vec::new();
            let mut seen_drained_labels: HashSet<String> = HashSet::new();

            if tracing::enabled!(tracing::Level::DEBUG) {
                debug!(
                    target: "compaction_handover::commit_batch",
                    shard = self.shard_id,
                    uid_count = batch.uid_plans.len(),
                    uids_to_retire = ?batch.uid_plans.iter().map(|p| p.uid.clone()).collect::<Vec<_>>(),
                    input_segments = ?input_labels,
                    "Retiring multiple UIDs from shared segments"
                );
            }

            for uid_plan in &batch.uid_plans {
                let drained = index
                    .retire_uid_from_labels(&uid_plan.uid, input_labels.iter().map(|s| s.as_str()));

                if tracing::enabled!(tracing::Level::DEBUG) {
                    debug!(
                        target: "compaction_handover::commit_batch",
                        shard = self.shard_id,
                        uid = %uid_plan.uid,
                        drained_from_this_uid = drained.len(),
                        "Retired UID from segments"
                    );
                }

                // Only add segments that haven't been marked as drained yet
                for entry in drained {
                    let label = entry.label();
                    if !seen_drained_labels.contains(&label) {
                        seen_drained_labels.insert(label.clone());
                        all_drained.push(entry);
                    }
                }
            }

            let drained_labels: Vec<String> =
                all_drained.iter().map(|entry| entry.label()).collect();

            if tracing::enabled!(tracing::Level::DEBUG) {
                debug!(
                    target: "compaction_handover::commit_batch",
                    shard = self.shard_id,
                    total_uids_retired = batch.uid_plans.len(),
                    total_drained_after_all_uids = drained_labels.len(),
                    drained_labels = ?drained_labels,
                    input_segments = ?input_labels,
                    "Completed retiring all UIDs from segments"
                );
            }

            // Log segment states after retirement
            let segments_after: Vec<_> = input_labels
                .iter()
                .filter_map(|l| {
                    index
                        .iter_all()
                        .find(|e| e.label() == *l)
                        .map(|e| (l.clone(), e.uids.clone()))
                })
                .collect();

            if tracing::enabled!(tracing::Level::DEBUG) {
                tracing::debug!(
                    target: "compaction_handover::commit_batch",
                    shard = self.shard_id,
                    drained_count = drained_labels.len(),
                    drained_labels = ?drained_labels,
                    segments_remaining = segments_after.len(),
                    segment_states_after = ?segments_after,
                    total_entries_after_retirement = index.len(),
                    "Retired UIDs from input segments"
                );
            }

            // Verify output segments exist before updating index (crash safety)
            // This ensures we never update the index to reference segments that don't exist
            for entry in &new_entries {
                let output_segment_dir = self.shard_dir.join(SegmentId::from(entry.id).dir_name());
                if !output_segment_dir.exists() {
                    error!(
                        target: "compaction_handover::commit_batch",
                        shard = self.shard_id,
                        output_segment_id = entry.id,
                        output_segment_dir = %output_segment_dir.display(),
                        "Output segment directory does not exist, aborting index update"
                    );
                    return Err(StoreError::FlushFailed(format!(
                        "Output segment directory does not exist: {}",
                        output_segment_dir.display()
                    )));
                }
            }

            // Insert all new entries
            for entry in &new_entries {
                let new_entry_id = entry.id;
                let new_entry_uids = entry.uids.clone();
                index.insert_entry(entry.clone());
                debug!(
                    target: "compaction_handover::commit_batch",
                    shard = self.shard_id,
                    new_entry_id = new_entry_id,
                    new_entry_uids = ?new_entry_uids,
                    "Inserted new entry"
                );
            }

            index.save(&self.shard_dir).await?;

            if tracing::enabled!(tracing::Level::INFO) {
                tracing::info!(
                    target: "compaction_handover::commit_batch",
                    shard = self.shard_id,
                    new_entries_count = new_entries.len(),
                    total_entries_final = index.len(),
                    "Inserted all entries and saved index"
                );
            }

            drained_labels
        };

        // Update segment IDs: remove all drained segments, add all new segments
        let retired_set: HashSet<&str> = drained_labels.iter().map(|s| s.as_str()).collect();
        let mut guard = self.segment_ids.write().unwrap();
        let before = guard.len();
        guard.retain(|label| !retired_set.contains(label.as_str()));
        for entry in &new_entries {
            let new_label = SegmentId::from(entry.id).dir_name();
            guard.push(new_label);
        }
        guard.sort();
        debug!(
            target: "compaction_handover::commit_batch",
            shard = self.shard_id,
            before,
            after = guard.len(),
            new_entries_count = new_entries.len(),
            "Updated shared segment id list"
        );

        self.invalidate_caches(&drained_labels);

        Ok(drained_labels)
    }

    fn invalidate_caches(&self, retired: &[String]) {
        for label in retired {
            self.column_cache.invalidate_segment(label);
            self.zone_surf_cache.invalidate_segment(label);
            self.zone_index_cache.invalidate_segment(label);
            self.index_catalog_cache.invalidate_segment(label);
            self.column_block_cache.invalidate_segment(label);
            debug!(
                target: "compaction_handover::cache",
                shard = self.shard_id,
                %label,
                "Invalidated caches for retired segment"
            );
        }
    }

    pub fn schedule_reclaim(&self, retired: Vec<String>) {
        if retired.is_empty() {
            return;
        }

        let shard_dir = self.shard_dir.clone();
        let shard_id = self.shard_id;
        tokio::spawn(async move {
            match tokio::task::spawn_blocking(move || {
                Self::move_to_reclaim(shard_id, shard_dir, retired)
            })
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    warn!(target: "compaction_handover::reclaim", shard = shard_id, error = %err, "Failed to reclaim retired segments")
                }
                Err(err) => {
                    error!(target: "compaction_handover::reclaim", shard = shard_id, "Failed to join reclaim task: {err}")
                }
            }
        });
    }

    fn move_to_reclaim(
        shard_id: u32,
        shard_dir: PathBuf,
        retired: Vec<String>,
    ) -> std::io::Result<()> {
        use std::fs;

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let reclaim_root = shard_dir.join(".reclaim");
        fs::create_dir_all(&reclaim_root)?;
        let batch_dir = reclaim_root.join(timestamp.to_string());
        fs::create_dir_all(&batch_dir)?;

        for label in &retired {
            let src = shard_dir.join(label);
            if !src.exists() {
                continue;
            }
            let dst = batch_dir.join(label);
            if let Err(err) = fs::rename(&src, &dst) {
                warn!(
                    target: "compaction_handover::reclaim",
                    shard = shard_id,
                    %label,
                    error = %err,
                    "Failed to move retired segment into reclaim directory"
                );
            }
        }

        for label in &retired {
            let path = batch_dir.join(label);
            match fs::remove_dir_all(&path) {
                Ok(_) => {
                    debug!(target: "compaction_handover::reclaim", shard = shard_id, %label, "Deleted reclaimed segment directory")
                }
                Err(err) => {
                    warn!(target: "compaction_handover::reclaim", shard = shard_id, %label, error = %err, "Leaving reclaimed directory for future cleanup")
                }
            }
        }

        match fs::remove_dir(&batch_dir) {
            Ok(_) => {
                debug!(target: "compaction_handover::reclaim", shard = shard_id, "Cleaned empty reclaim batch directory")
            }
            Err(err) if err.kind() == std::io::ErrorKind::DirectoryNotEmpty => {}
            Err(err) => {
                warn!(target: "compaction_handover::reclaim", shard = shard_id, error = %err, "Failed to remove reclaim batch directory")
            }
        }

        Ok(())
    }
}
