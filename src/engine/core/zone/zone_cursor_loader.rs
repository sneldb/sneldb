use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::engine::core::{ColumnReader, ZoneCursor, ZoneMeta};
use crate::engine::errors::{QueryExecutionError, ZoneMetaError};
use crate::engine::schema::SchemaRegistry;
use tokio::sync::RwLock;
use tracing::{debug, error, info};

/// Loads ZoneCursors for a given UID across multiple segments
pub struct ZoneCursorLoader {
    uid: String,
    segment_ids: Vec<String>,
    registry: Arc<RwLock<SchemaRegistry>>,
    base_dir: PathBuf,
}

impl ZoneCursorLoader {
    pub fn new(
        uid: String,
        segment_ids: Vec<String>,
        registry: Arc<RwLock<SchemaRegistry>>,
        base_dir: PathBuf,
    ) -> Self {
        Self {
            uid,
            segment_ids,
            registry,
            base_dir,
        }
    }

    pub async fn load_all(&self) -> Result<Vec<ZoneCursor>, QueryExecutionError> {
        let schema = {
            let reg = self.registry.read().await;
            reg.get_schema_by_uid(&self.uid)
                .ok_or_else(|| {
                    if tracing::enabled!(tracing::Level::ERROR) {
                        error!(
                            target: "sneldb::cursor_loader",
                            uid = self.uid,
                            "Schema not found"
                        );
                    }
                    QueryExecutionError::SchemaNotFound(format!("No schema for UID {}", self.uid))
                })?
                .clone()
        };

        if tracing::enabled!(tracing::Level::INFO) {
            info!(
                target: "sneldb::cursor_loader",
                uid = self.uid,
                fields = ?schema.fields().collect::<Vec<_>>(),
                "Loaded schema for UID"
            );
        }

        let mut all_cursors = vec![];

        for segment_id in &self.segment_ids {
            let segment_dir = self.base_dir.join(format!("segment-{}", segment_id));
            let zones_path = segment_dir.join(format!("{}.zones", self.uid));

            if tracing::enabled!(tracing::Level::DEBUG) {
                debug!(
                    target: "sneldb::cursor_loader",
                    uid = self.uid,
                    segment_id = segment_id,
                    path = ?zones_path,
                    "Attempting to load zone metadata"
                );
            }

            let zone_metas =
                ZoneMeta::load(&zones_path).map_err(|e| ZoneMetaError::Other(e.to_string()))?;

            for zone in &zone_metas {
                let context_ids = ColumnReader::load_for_zone(
                    &segment_dir,
                    segment_id,
                    &self.uid,
                    "context_id",
                    zone.zone_id,
                )?;

                let timestamps = ColumnReader::load_for_zone(
                    &segment_dir,
                    segment_id,
                    &self.uid,
                    "timestamp",
                    zone.zone_id,
                )?;

                let event_types = ColumnReader::load_for_zone(
                    &segment_dir,
                    segment_id,
                    &self.uid,
                    "event_type",
                    zone.zone_id,
                )?;

                let mut payload_fields = HashMap::new();
                for field in schema.fields() {
                    let values = ColumnReader::load_for_zone(
                        &segment_dir,
                        segment_id,
                        &self.uid,
                        field,
                        zone.zone_id,
                    )?;
                    payload_fields.insert(field.clone(), values);
                }

                if tracing::enabled!(tracing::Level::DEBUG) {
                    debug!(
                        target: "sneldb::cursor_loader",
                        uid = self.uid,
                        segment_id = segment_id,
                        zone_id = zone.zone_id,
                        fields = ?schema.fields().collect::<Vec<_>>(),
                        "Constructed ZoneCursor"
                    );
                }

                let parsed_segment_id = match segment_id.parse::<u64>() {
                    Ok(id) => id,
                    Err(_) => {
                        if tracing::enabled!(tracing::Level::ERROR) {
                            error!(
                                target: "sneldb::cursor_loader",
                                segment_id = segment_id,
                                "Failed to parse segment_id to u64"
                            );
                        }
                        return Err(QueryExecutionError::InvalidSegmentId(segment_id.clone()));
                    }
                };

                all_cursors.push(ZoneCursor {
                    segment_id: parsed_segment_id,
                    zone_id: zone.zone_id,
                    context_ids,
                    timestamps,
                    event_types,
                    payload_fields,
                    pos: 0,
                });
            }
        }

        if tracing::enabled!(tracing::Level::INFO) {
            info!(
                target: "sneldb::cursor_loader",
                uid = self.uid,
                count = all_cursors.len(),
                "Loaded all ZoneCursors"
            );
        }

        Ok(all_cursors)
    }
}
