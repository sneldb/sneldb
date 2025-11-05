use crate::engine::core::{Event, ZoneRow};
use crate::engine::errors::StoreError;
use crate::engine::types::ScalarValue;
use std::collections::{BTreeMap, HashMap, HashSet};
use tracing::{info, trace, warn};

/// Defines a range of rows for a zone.
#[derive(Debug, PartialEq, Clone)]
pub struct ZonePlan {
    pub id: u32,
    pub start_index: usize,
    pub end_index: usize, // inclusive
    pub events: Vec<Event>,
    pub uid: String,
    pub event_type: String,
    pub segment_id: u64,
    /// Maximum created_at timestamp from source zones (for compaction)
    pub created_at: u64,
}

impl ZonePlan {
    /// Divides events into zones of `rows_per_zone`, returning a vector of zone plans.
    pub fn build_all(
        events: &[Event],
        rows_per_zone: usize,
        uid: String,
        segment_id: u64,
    ) -> Result<Vec<ZonePlan>, StoreError> {
        if events.is_empty() {
            if tracing::enabled!(tracing::Level::WARN) {
                warn!(
                    target: "sneldb::flush",
                    uid,
                    segment_id,
                    "ZonePlan::build_all received empty event list"
                );
            }
            return Err(StoreError::EmptyFlush);
        }

        let mut zones = Vec::new();
        let mut start = 0;
        let mut zone_id = 0;
        let event_type = events[0].event_type.clone();

        while start < events.len() {
            let end = (start + rows_per_zone).min(events.len()) - 1;

            if tracing::enabled!(tracing::Level::TRACE) {
                trace!(
                    target: "sneldb::flush",
                    uid,
                    segment_id,
                    zone_id,
                    start,
                    end,
                    "Building zone"
                );
            }

            zones.push(ZonePlan {
                id: zone_id,
                start_index: start,
                end_index: end,
                events: events[start..=end].to_vec(),
                uid: uid.clone(),
                event_type: event_type.clone(),
                segment_id,
                created_at: crate::shared::time::now(),
            });

            zone_id += 1;
            start = end + 1;
        }

        if tracing::enabled!(tracing::Level::INFO) {
            info!(
                target: "sneldb::flush",
                uid,
                segment_id,
                zone_count = zones.len(),
                "Zone planning completed"
            );
        }

        Ok(zones)
    }

    /// Groups the events in this zone by a key extracted with the provided closure.
    pub fn group_by<K, F>(&self, mut key_fn: F) -> HashMap<K, Vec<&Event>>
    where
        K: Eq + std::hash::Hash,
        F: FnMut(&Event) -> K,
    {
        let mut map: HashMap<K, Vec<&Event>> = HashMap::new();
        for event in &self.events {
            map.entry(key_fn(event)).or_default().push(event);
        }
        map
    }

    /// Collects unique values for each field in the zone's events.
    pub fn collect_unique_field_values(&self) -> HashMap<(String, String), HashSet<String>> {
        let mut field_values = HashMap::new();

        // Optimization: collect all field names once for the entire zone
        // This is required for columnar storage - all columns must have same row count
        // Missing fields will be stored as empty strings
        let mut all_field_names = HashSet::new();
        all_field_names.insert("context_id".to_string());
        all_field_names.insert("event_type".to_string());
        all_field_names.insert("timestamp".to_string());
        all_field_names.insert("event_id".to_string());

        // Collect dynamic field names from all events in one pass
        for event in &self.events {
            for key in event.payload.keys() {
                all_field_names.insert(key.clone());
            }
        }

        // Pre-compute HashMap keys to avoid repeated allocations
        let uid = self.uid.clone();
        let mut field_keys: HashMap<String, (String, String)> = HashMap::new();
        for field in &all_field_names {
            let key = (uid.clone(), field.clone());
            field_keys.insert(field.clone(), key.clone());
            field_values.insert(key, HashSet::new());
        }

        // Now iterate through all fields and collect values from all events
        // (including empty strings for missing fields)
        for field in &all_field_names {
            let key = field_keys.get(field).unwrap();
            let values = field_values.get_mut(key).unwrap();

            for event in &self.events {
                let value = event.get_field_value(field);
                values.insert(value);
            }
        }

        field_values
    }

    /// Collects unique values for each field across all zones.
    pub fn collect_field_values(zones: &[ZonePlan]) -> HashMap<(String, String), HashSet<String>> {
        let mut field_values = HashMap::new();

        for zone in zones {
            let zone_field_values = zone.collect_unique_field_values();

            for (key, values) in zone_field_values {
                field_values
                    .entry(key)
                    .or_insert_with(HashSet::new)
                    .extend(values);
            }
        }

        field_values
    }

    pub fn from_rows(
        rows: Vec<ZoneRow>,
        uid: String,
        segment_id: u64,
        zone_id: u32,
        created_at: u64,
    ) -> Result<ZonePlan, StoreError> {
        if rows.is_empty() {
            if tracing::enabled!(tracing::Level::WARN) {
                warn!(
                    target: "sneldb::flush",
                    uid,
                    segment_id,
                    zone_id,
                    "ZonePlan::from_rows received empty input"
                );
            }
            return Err(StoreError::EmptyFlush);
        }

        let event_type = rows[0].event_type.clone();
        let start_index = 0;
        let end_index = rows.len() - 1;

        if tracing::enabled!(tracing::Level::TRACE) {
            trace!(
                target: "sneldb::flush",
                uid,
                segment_id,
                zone_id,
                row_count = rows.len(),
                "Deserializing zone rows into events"
            );
        }

        let events: Vec<Event> = rows
            .into_iter()
            .map(|row| {
                let payload: BTreeMap<String, ScalarValue> = row
                    .payload
                    .into_iter()
                    .map(|(k, v)| (k, ScalarValue::from(v)))
                    .collect();
                Event {
                    context_id: row.context_id,
                    timestamp: row.timestamp.parse::<u64>().unwrap(),
                    event_type: row.event_type.clone(),
                    id: row.event_id,
                    payload,
                }
            })
            .collect();

        Ok(ZonePlan {
            id: zone_id,
            start_index,
            end_index,
            events,
            uid,
            event_type,
            segment_id,
            created_at,
        })
    }
}
