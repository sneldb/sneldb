use std::collections::HashMap;

use crate::engine::core::time::{TemporalCalendarIndex, ZoneTemporalIndex};
use crate::engine::core::zone::zone_plan::ZonePlan;
use crate::engine::errors::StoreError;
use crate::engine::schema::FieldType;
use crate::engine::schema::registry::SchemaRegistry;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct TemporalIndexBuilder<'a> {
    pub uid: &'a str,
    pub segment_dir: &'a Path,
    pub registry: Arc<RwLock<SchemaRegistry>>,
}

impl<'a> TemporalIndexBuilder<'a> {
    pub fn new(uid: &'a str, segment_dir: &'a Path, registry: Arc<RwLock<SchemaRegistry>>) -> Self {
        Self {
            uid,
            segment_dir,
            registry,
        }
    }

    /// Build per-field temporal calendars and per-zone temporal indexes for all temporal fields.
    pub async fn build_for_zone_plans(&self, zone_plans: &[ZonePlan]) -> Result<(), StoreError> {
        let Some(schema) = self
            .registry
            .read()
            .await
            .get(&zone_plans[0].event_type)
            .cloned()
        else {
            return Ok(());
        };

        let mut calendars: HashMap<String, TemporalCalendarIndex> = HashMap::new();
        let temporal_fields: Vec<String> = schema
            .fields
            .iter()
            .filter_map(|(name, ty)| match ty {
                FieldType::Timestamp | FieldType::Date => Some(name.clone()),
                FieldType::Optional(inner)
                    if matches!(**inner, FieldType::Timestamp | FieldType::Date) =>
                {
                    Some(name.clone())
                }
                _ => None,
            })
            .map(|s| s.to_string())
            .collect();

        if temporal_fields.is_empty() {
            return Ok(());
        }

        for zp in zone_plans {
            for field in &temporal_fields {
                let mut ts_vals: Vec<i64> = Vec::new();
                for ev in &zp.events {
                    if let Some(v) = ev.payload.get(field) {
                        if let Some(i) = v.as_i64() {
                            ts_vals.push(i);
                        } else if let Some(u) = v.as_u64() {
                            ts_vals.push(u as i64);
                        }
                    }
                }
                if ts_vals.is_empty() {
                    continue;
                }
                // Build and save ZTI per zone/field
                let zti = ZoneTemporalIndex::from_timestamps(ts_vals.clone(), 1, 64);
                zti.save_for_field(self.uid, field, zp.id, self.segment_dir)
                    .map_err(|e| {
                        StoreError::FlushFailed(format!(
                            "Failed to save field temporal index: {}",
                            e
                        ))
                    })?;

                // Update calendar with min/max per zone
                let min_ts = *ts_vals.iter().min().unwrap_or(&0);
                let max_ts = *ts_vals.iter().max().unwrap_or(&0);
                if min_ts >= 0 && max_ts >= 0 {
                    let entry = calendars
                        .entry(field.clone())
                        .or_insert_with(|| TemporalCalendarIndex::new(field.clone()));
                    entry.add_zone_range(zp.id, min_ts as u64, max_ts as u64);
                }
            }
        }

        // Persist per-field calendars
        for (_field, cal) in calendars.iter() {
            cal.save(self.uid, self.segment_dir).map_err(|e| {
                StoreError::FlushFailed(format!("Failed to save field calendar: {}", e))
            })?;
        }
        Ok(())
    }
}
