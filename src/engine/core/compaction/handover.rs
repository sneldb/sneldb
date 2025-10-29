use super::merge_plan::MergePlan;
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

    pub async fn commit(
        &self,
        plan: &MergePlan,
        new_entry: SegmentEntry,
    ) -> Result<(), StoreError> {
        let retired_labels = plan.input_segment_labels.clone();
        let new_label = SegmentId::from(new_entry.id).dir_name();

        info!(
            target: "compaction_handover::commit",
            shard = self.shard_id,
            level_from = plan.level_from,
            level_to = plan.level_to,
            input_count = retired_labels.len(),
            %new_label,
            "Applying compaction handover"
        );

        {
            let _guard = self.flush_lock.lock().await;
            let mut index = SegmentIndex::load(&self.shard_dir).await?;
            let removed = index.remove_labels(retired_labels.iter().map(|s| s.as_str()));
            debug!(
                target: "compaction_handover::commit",
                shard = self.shard_id,
                removed = removed.len(),
                "Removed retired entries from index"
            );
            index.insert_entry(new_entry);
            index.save(&self.shard_dir).await?;
            debug!(
                target: "compaction_handover::commit",
                shard = self.shard_id,
                len = index.len(),
                "Persisted segment index after compaction"
            );
        }

        self.update_segment_ids(&retired_labels, &new_label);
        self.invalidate_caches(&retired_labels);
        self.schedule_reclaim(retired_labels);

        Ok(())
    }

    fn update_segment_ids(&self, retired: &[String], new_label: &str) {
        let retired_set: HashSet<&str> = retired.iter().map(|s| s.as_str()).collect();
        let mut guard = self.segment_ids.write().unwrap();
        let before = guard.len();
        guard.retain(|label| !retired_set.contains(label.as_str()));
        guard.push(new_label.to_string());
        guard.sort();
        debug!(
            target: "compaction_handover::segment_ids",
            shard = self.shard_id,
            before,
            after = guard.len(),
            %new_label,
            "Updated shared segment id list"
        );
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

    fn schedule_reclaim(&self, retired: Vec<String>) {
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
