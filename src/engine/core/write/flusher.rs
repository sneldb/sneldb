use crate::engine::core::{Event, MemTable, SegmentIndexBuilder, ZonePlanner, ZoneWriter};
use crate::engine::errors::StoreError;
use crate::engine::schema::registry::SchemaRegistry;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, info, instrument, trace};

pub struct Flusher {
    memtable: MemTable,
    segment_id: u64,
    segment_dir: PathBuf,
    registry: Arc<RwLock<SchemaRegistry>>,
    flush_coordination_lock: Arc<Mutex<()>>,
}

impl Flusher {
    pub fn new(
        memtable: MemTable,
        segment_id: u64,
        segment_dir: &Path,
        registry: Arc<RwLock<SchemaRegistry>>,
        flush_coordination_lock: Arc<Mutex<()>>,
    ) -> Self {
        Self {
            memtable,
            segment_id,
            segment_dir: segment_dir.to_path_buf(),
            registry,
            flush_coordination_lock,
        }
    }

    #[instrument(skip(self), fields(segment_id = self.segment_id))]
    pub async fn flush(self) -> Result<(), StoreError> {
        // Extract fields we need after moving out memtable
        let segment_id = self.segment_id;
        let segment_dir = self.segment_dir.clone();
        let registry = self.registry.clone();

        fs::create_dir_all(&segment_dir)?;

        // Move events out of the MemTable without cloning
        let table = self.memtable.take(); // BTreeMap<String, Vec<Event>> grouped by context_id

        // Re-group by event_type, moving Events into buckets
        let mut by_event_type: HashMap<String, Vec<Event>> = HashMap::new();
        let mut total_count: usize = 0;
        for (_ctx, mut bucket) in table.into_iter() {
            total_count += bucket.len();
            for ev in bucket.drain(..) {
                by_event_type
                    .entry(ev.event_type.clone())
                    .or_default()
                    .push(ev);
            }
        }

        debug!(
            target: "sneldb::flush",
            count = total_count,
            segment_id = segment_id,
            "Collected events from MemTable"
        );

        info!(
            target: "sneldb::flush",
            segment_id = segment_id,
            event_types = by_event_type.len(),
            "Grouped events by event_type"
        );

        for (event_type, events) in by_event_type.iter() {
            debug!(
                target: "sneldb::flush",
                segment_id = segment_id,
                event_type,
                count = events.len(),
                "Flushing entries for event_type"
            );
            Self::flush_one_type_inner(
                segment_id,
                &segment_dir,
                Arc::clone(&registry),
                event_type,
                events,
            )
            .await?;
        }

        let uids = Self::resolve_uids_with(&registry, by_event_type.keys()).await?;
        SegmentIndexBuilder {
            segment_id,
            segment_dir: &segment_dir,
            event_type_uids: uids,
            flush_coordination_lock: Arc::clone(&self.flush_coordination_lock),
        }
        .add_segment_entry(None)
        .await?;

        info!(
            target: "sneldb::flush",
            segment_id = segment_id,
            "Flush completed successfully"
        );
        Ok(())
    }

    #[instrument(skip(events, registry, segment_dir), fields(event_type, count = events.len()))]
    async fn flush_one_type_inner(
        segment_id: u64,
        segment_dir: &PathBuf,
        registry: Arc<RwLock<SchemaRegistry>>,
        event_type: &str,
        events: &[Event],
    ) -> Result<(), StoreError> {
        let uid = registry
            .read()
            .await
            .get_uid(event_type)
            .ok_or_else(|| StoreError::NoUidForEventType(event_type.to_owned()))?;

        trace!(
            target: "sneldb::flush",
            segment_id = segment_id,
            event_type,
            uid,
            "Planning zones"
        );

        let planner = ZonePlanner::new(&uid, segment_id);
        let zone_plans = planner.plan(events)?;

        trace!(
            target: "sneldb::flush",
            segment_id = segment_id,
            event_type,
            zones = zone_plans.len(),
            "Writing zones to disk"
        );

        let writer = ZoneWriter::new(&uid, segment_dir, registry.clone());
        writer.write_all(&zone_plans).await?;

        Ok(())
    }

    async fn resolve_uids_with<'a, I>(
        registry: &Arc<RwLock<SchemaRegistry>>,
        event_types: I,
    ) -> Result<Vec<String>, StoreError>
    where
        I: IntoIterator<Item = &'a String>,
    {
        let registry = registry.read().await;
        event_types
            .into_iter()
            .map(|et| {
                registry
                    .get_uid(et)
                    .ok_or_else(|| StoreError::NoUidForEventType(et.clone()))
            })
            .collect()
    }
}
