use crate::engine::core::{Event, MemTable, SegmentIndexBuilder, ZonePlanner, ZoneWriter};
use crate::engine::errors::StoreError;
use crate::engine::schema::registry::SchemaRegistry;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, instrument, trace};

pub struct Flusher {
    memtable: MemTable,
    segment_id: u64,
    segment_dir: PathBuf,
    registry: Arc<RwLock<SchemaRegistry>>,
}

impl Flusher {
    pub fn new(
        memtable: MemTable,
        segment_id: u64,
        segment_dir: &Path,
        registry: Arc<RwLock<SchemaRegistry>>,
    ) -> Self {
        Self {
            memtable,
            segment_id,
            segment_dir: segment_dir.to_path_buf(),
            registry,
        }
    }

    #[instrument(skip(self), fields(segment_id = self.segment_id))]
    pub async fn flush(&self) -> Result<(), StoreError> {
        fs::create_dir_all(&self.segment_dir)?;

        let all_events: Vec<Event> = self.memtable.iter().cloned().collect();
        debug!(
            target: "sneldb::flush",
            count = all_events.len(),
            segment_id = self.segment_id,
            "Collected events from MemTable"
        );

        let grouped = Event::group_by(&all_events, "event_type");
        info!(
            target: "sneldb::flush",
            segment_id = self.segment_id,
            event_types = grouped.len(),
            "Grouped events by event_type"
        );

        for (event_type, entries) in &grouped {
            debug!(
                target: "sneldb::flush",
                segment_id = self.segment_id,
                event_type,
                count = entries.len(),
                "Flushing entries for event_type"
            );
            self.flush_one_type(event_type, entries).await?;
        }

        let uids = self.resolve_uids(grouped.keys()).await?;
        SegmentIndexBuilder {
            segment_id: self.segment_id,
            segment_dir: &self.segment_dir,
            event_type_uids: uids,
        }
        .add_segment_entry(None)
        .await?;

        info!(
            target: "sneldb::flush",
            segment_id = self.segment_id,
            "Flush completed successfully"
        );
        Ok(())
    }

    #[instrument(skip(self, events), fields(event_type, count = events.len()))]
    async fn flush_one_type(&self, event_type: &str, events: &[&Event]) -> Result<(), StoreError> {
        let uid = self
            .registry
            .read()
            .await
            .get_uid(event_type)
            .ok_or_else(|| StoreError::NoUidForEventType(event_type.to_owned()))?;

        trace!(
            target: "sneldb::flush",
            segment_id = self.segment_id,
            event_type,
            uid,
            "Planning zones"
        );

        let planner = ZonePlanner::new(&uid, self.segment_id);
        let zone_plans = planner.plan(&events.iter().cloned().cloned().collect::<Vec<_>>())?;

        trace!(
            target: "sneldb::flush",
            segment_id = self.segment_id,
            event_type,
            zones = zone_plans.len(),
            "Writing zones to disk"
        );

        let writer = ZoneWriter::new(&uid, &self.segment_dir, self.registry.clone());
        writer.write_all(&zone_plans).await?;

        Ok(())
    }

    async fn resolve_uids<'a, I>(&self, event_types: I) -> Result<Vec<String>, StoreError>
    where
        I: IntoIterator<Item = &'a String>,
    {
        let registry = self.registry.read().await;
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
