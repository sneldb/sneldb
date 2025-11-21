use std::collections::{HashMap, HashSet};

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
        let temporal_field_set: HashSet<String> = temporal_fields.iter().cloned().collect();

        // Accumulate per-field slab entries across all zones
        let mut field_entries: HashMap<String, Vec<(u32, ZoneTemporalIndex)>> = HashMap::new();

        for zp in zone_plans {
            // Precompute per-field timestamps by scanning events once
            let mut ts_per_field: HashMap<String, Vec<i64>> = HashMap::new();
            for ev in &zp.events {
                // Iterate over payload map once; push relevant temporal fields
                for (k, v) in ev.payload.iter() {
                    let k_str = k.as_ref();
                    if !temporal_field_set.contains(k_str) {
                        continue;
                    }
                    if let Some(i) = v.as_i64() {
                        ts_per_field.entry(k_str.to_string()).or_default().push(i);
                    } else if let Some(u) = v.as_u64() {
                        ts_per_field.entry(k_str.to_string()).or_default().push(u as i64);
                    }
                }
            }

            // Always include the fixed event timestamp field as a temporal field
            if !zp.events.is_empty() {
                let mut ts_vals_ts: Vec<i64> = Vec::with_capacity(zp.events.len());
                for ev in &zp.events {
                    ts_vals_ts.push(ev.timestamp as i64);
                }
                // Build ZTI and calendar range for fixed timestamp
                let zti = ZoneTemporalIndex::from_timestamps(ts_vals_ts.clone(), 1, 64);
                field_entries
                    .entry("timestamp".to_string())
                    .or_default()
                    .push((zp.id, zti));

                let min_ts = *ts_vals_ts.iter().min().unwrap_or(&0);
                let max_ts = *ts_vals_ts.iter().max().unwrap_or(&0);
                if min_ts >= 0 && max_ts >= 0 {
                    let entry = calendars
                        .entry("timestamp".to_string())
                        .or_insert_with(|| TemporalCalendarIndex::new("timestamp"));
                    entry.add_zone_range(zp.id, min_ts as u64, max_ts as u64);
                }
            }

            // Build ZTI for each field present in this zone and update calendars
            for (field, ts_vals) in ts_per_field.into_iter() {
                if ts_vals.is_empty() {
                    continue;
                }
                let zti = ZoneTemporalIndex::from_timestamps(ts_vals.clone(), 1, 64);
                field_entries
                    .entry(field.clone())
                    .or_default()
                    .push((zp.id, zti));

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

        // Persist per-field slab files
        for (field, entries) in field_entries.iter() {
            // Build slice of references for slab writer
            let mut refs: Vec<(u32, &ZoneTemporalIndex)> = Vec::with_capacity(entries.len());
            for (zid, zti) in entries.iter() {
                refs.push((*zid, zti));
            }
            ZoneTemporalIndex::save_field_slab(self.uid, field, self.segment_dir, &refs).map_err(
                |e| StoreError::FlushFailed(format!("Failed to save field temporal slab: {}", e)),
            )?;
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
