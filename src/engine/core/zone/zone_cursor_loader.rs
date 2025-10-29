use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::engine::core::column::format::PhysicalType;
use crate::engine::core::column::type_catalog::ColumnTypeCatalog;
use crate::engine::core::{ColumnKey, ColumnReader, EventId, ZoneCursor, ZoneMeta};
use crate::engine::errors::{QueryExecutionError, ZoneMetaError};
use crate::engine::schema::SchemaRegistry;
use crate::engine::schema::types::FieldType;
use serde_json::Value;
use tokio::sync::RwLock;
use tracing::{debug, error, info};

/// Loads ZoneCursors for a given UID across multiple segments
pub struct ZoneCursorLoader {
    uid: String,
    segment_ids: Vec<String>,
    registry: Arc<RwLock<SchemaRegistry>>,
    base_dir: PathBuf,
}

#[derive(Debug)]
pub struct LoadedZoneCursors {
    pub cursors: Vec<ZoneCursor>,
    pub type_catalog: ColumnTypeCatalog,
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

    pub async fn load_all(&self) -> Result<LoadedZoneCursors, QueryExecutionError> {
        let (schema, event_type_name) = {
            let reg = self.registry.read().await;
            let schema_ref = reg.get_schema_by_uid(&self.uid).ok_or_else(|| {
                if tracing::enabled!(tracing::Level::ERROR) {
                    error!(
                        target: "sneldb::cursor_loader",
                        uid = self.uid,
                        "Schema not found"
                    );
                }
                QueryExecutionError::SchemaNotFound(format!("No schema for UID {}", self.uid))
            })?;
            let event_type = reg.get_event_type_by_uid(&self.uid).ok_or_else(|| {
                QueryExecutionError::SchemaNotFound(format!(
                    "No event_type mapping for UID {}",
                    self.uid
                ))
            })?;
            (schema_ref.clone(), event_type)
        };

        let schema_fields: Vec<String> = schema.fields().cloned().collect();

        if tracing::enabled!(tracing::Level::INFO) {
            info!(
                target: "sneldb::cursor_loader",
                uid = self.uid,
                fields = ?schema_fields.as_slice(),
                "Loaded schema for UID"
            );
        }

        let mut all_cursors = Vec::new();
        let mut type_catalog = ColumnTypeCatalog::with_capacity(schema_fields.len() + 4);

        let context_key: ColumnKey = (event_type_name.clone(), "context_id".to_string());
        let timestamp_key: ColumnKey = (event_type_name.clone(), "timestamp".to_string());
        let event_type_key: ColumnKey = (event_type_name.clone(), "event_type".to_string());
        let event_id_key: ColumnKey = (event_type_name.clone(), "event_id".to_string());

        // Prefill type hints from schema before inspecting on-disk payloads so compaction can
        // emit typed column blocks even if the source segments stored values as VarBytes.
        type_catalog.record_if_absent(&context_key, PhysicalType::VarBytes);
        type_catalog.record_if_absent(&timestamp_key, PhysicalType::I64);
        type_catalog.record_if_absent(&event_type_key, PhysicalType::VarBytes);
        for field in &schema_fields {
            if let Some(field_type) = schema.field_type(field) {
                let phys_hint = field_type_to_physical_type(field_type);
                let key: ColumnKey = (event_type_name.clone(), field.clone());
                type_catalog.record_if_absent(&key, phys_hint);
            }
        }

        for segment_id in &self.segment_ids {
            let segment_dir = self.base_dir.join(segment_id);
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
                let context_snapshot = ColumnReader::load_for_zone_snapshot(
                    &segment_dir,
                    segment_id,
                    &self.uid,
                    "context_id",
                    zone.zone_id,
                    None,
                )?;
                let context_phys = context_snapshot.physical_type();
                let context_ids = context_snapshot.into_strings();
                type_catalog.record_if_absent(&context_key, context_phys);

                let timestamp_snapshot = ColumnReader::load_for_zone_snapshot(
                    &segment_dir,
                    segment_id,
                    &self.uid,
                    "timestamp",
                    zone.zone_id,
                    None,
                )?;
                let timestamp_phys = timestamp_snapshot.physical_type();
                let timestamps = timestamp_snapshot.into_strings();
                type_catalog.record_if_absent(&timestamp_key, timestamp_phys);

                let event_id_snapshot = ColumnReader::load_for_zone_snapshot(
                    &segment_dir,
                    segment_id,
                    &self.uid,
                    "event_id",
                    zone.zone_id,
                    None,
                )?;
                let event_id_phys = event_id_snapshot.physical_type();
                let event_id_values = event_id_snapshot.into_values();
                let len = event_id_values.len();
                let mut event_ids = Vec::with_capacity(len);
                for idx in 0..len {
                    let raw = event_id_values
                        .get_u64_at(idx)
                        .or_else(|| event_id_values.get_i64_at(idx).map(|v| v as u64))
                        .or_else(|| {
                            event_id_values
                                .get_str_at(idx)
                                .and_then(|s| s.parse::<u64>().ok())
                        })
                        .unwrap_or_default();
                    event_ids.push(EventId::from(raw));
                }
                type_catalog.record_if_absent(&event_id_key, event_id_phys);

                let event_type_snapshot = ColumnReader::load_for_zone_snapshot(
                    &segment_dir,
                    segment_id,
                    &self.uid,
                    "event_type",
                    zone.zone_id,
                    None,
                )?;
                let event_type_phys = event_type_snapshot.physical_type();
                let event_types = event_type_snapshot.into_strings();
                type_catalog.record_if_absent(&event_type_key, event_type_phys);

                let mut payload_fields = HashMap::new();
                for field in &schema_fields {
                    let snapshot = ColumnReader::load_for_zone_snapshot(
                        &segment_dir,
                        segment_id,
                        &self.uid,
                        field,
                        zone.zone_id,
                        None,
                    )?;
                    let phys = snapshot.physical_type();
                    let key: ColumnKey = (event_type_name.clone(), field.clone());
                    let values: Vec<Value> = snapshot.into_json_values();
                    payload_fields.insert(field.clone(), values);
                    type_catalog.record_if_absent(&key, phys);
                }

                if tracing::enabled!(tracing::Level::DEBUG) {
                    debug!(
                        target: "sneldb::cursor_loader",
                        uid = self.uid,
                        segment_id = segment_id,
                        zone_id = zone.zone_id,
                        fields = ?schema_fields.as_slice(),
                        "Constructed ZoneCursor"
                    );
                }

                all_cursors.push(ZoneCursor {
                    segment_id: parsed_segment_id,
                    zone_id: zone.zone_id,
                    context_ids,
                    timestamps,
                    event_types,
                    event_ids,
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

        Ok(LoadedZoneCursors {
            cursors: all_cursors,
            type_catalog,
        })
    }
}

fn field_type_to_physical_type(field_type: &FieldType) -> PhysicalType {
    match field_type {
        FieldType::I64 | FieldType::Timestamp | FieldType::Date => PhysicalType::I64,
        FieldType::U64 => PhysicalType::U64,
        FieldType::F64 => PhysicalType::F64,
        FieldType::Bool => PhysicalType::Bool,
        FieldType::Optional(inner) => field_type_to_physical_type(inner.as_ref()),
        _ => PhysicalType::VarBytes,
    }
}
