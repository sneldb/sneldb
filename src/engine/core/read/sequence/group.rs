use crate::engine::core::CandidateZone;
use crate::engine::core::filter::condition::{FieldAccessor, PreparedAccessor};
use crate::engine::types::ScalarValue;
use std::collections::HashMap;
use tracing::{debug, info, trace, warn};

/// Represents a row index within a zone.
///
/// This allows us to reference specific rows in columnar data without
/// materializing the entire event.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RowIndex {
    /// Index of the zone in the zones vector
    pub zone_idx: usize,
    /// Index of the row within the zone
    pub row_idx: usize,
}

/// Represents grouped row indices by link field value.
///
/// Contains row indices for each event type, sorted by timestamp.
#[derive(Debug, Clone)]
pub struct GroupedRowIndices {
    /// The link field value that groups these rows
    pub link_value: ScalarValue,
    /// Map from event type to sorted row indices (sorted by timestamp)
    pub rows_by_type: HashMap<String, Vec<RowIndex>>,
}

/// Groups row indices by link field value using columnar data.
///
/// This class encapsulates the logic for extracting link field values from
/// columnar data and grouping rows accordingly. It avoids materializing events
/// by working directly with `CandidateZone` and `ColumnValues`.
///
/// # Performance
///
/// - Works with columnar data (no premature materialization)
/// - Pre-allocates HashMap capacity to avoid resizing
/// - Sorts row indices by timestamp within each group
pub struct ColumnarGrouper {
    /// The field name to use for linking events
    link_field: String,
    /// The field name to use for time-based sorting (default: "timestamp")
    time_field: String,
}

impl ColumnarGrouper {
    /// Creates a new `ColumnarGrouper` for the specified link field.
    ///
    /// # Arguments
    ///
    /// * `link_field` - The field name to use for grouping events (e.g., "user_id")
    /// * `time_field` - The field name to use for time-based sorting (e.g., "created_at", defaults to "timestamp")
    pub fn new(link_field: String, time_field: String) -> Self {
        Self {
            link_field,
            time_field,
        }
    }

    /// Groups row indices by link field value across all zones.
    ///
    /// This method extracts the link field value from each row in the columnar data
    /// and groups row indices by that value. Row indices are sorted by timestamp
    /// within each group.
    ///
    /// # Arguments
    ///
    /// * `zones_by_event_type` - Map from event type to zones containing columnar data
    ///
    /// # Returns
    ///
    /// Map from link field value (as string key) to grouped row indices
    pub fn group_zones_by_link_field(
        &self,
        zones_by_event_type: &HashMap<String, Vec<CandidateZone>>,
    ) -> HashMap<String, GroupedRowIndices> {
        if tracing::enabled!(tracing::Level::INFO) {
            info!(
                target: "sneldb::sequence::group",
                link_field = %self.link_field,
                event_type_count = zones_by_event_type.len(),
                "Starting grouping by link field"
            );
        }

        // Estimate capacity: assume ~10 events per group on average
        let total_events: usize = zones_by_event_type
            .values()
            .map(|zones| {
                zones
                    .iter()
                    .map(|zone| PreparedAccessor::new(&zone.values).event_count())
                    .sum::<usize>()
            })
            .sum();

        let estimated_groups = (total_events / 10).max(1);
        let mut groups: HashMap<String, GroupedRowIndices> =
            HashMap::with_capacity(estimated_groups);

        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                target: "sneldb::sequence::group",
                total_events = total_events,
                estimated_groups = estimated_groups,
                "Estimated group capacity"
            );
        }

        // Process each event type
        for (event_type, zones) in zones_by_event_type {
            if tracing::enabled!(tracing::Level::TRACE) {
                trace!(
                    target: "sneldb::sequence::group",
                    event_type = %event_type,
                    zone_count = zones.len(),
                    "Processing zones for event type"
                );
            }
            self.process_zones_for_event_type(event_type, zones, &mut groups);
        }

        // Sort row indices by timestamp within each group
        self.sort_groups_by_timestamp(&mut groups, zones_by_event_type);

        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                target: "sneldb::sequence::group",
                groups_created = groups.len(),
                total_events = total_events,
                "Grouped events by link field"
            );
        }

        groups
    }

    /// Processes zones for a specific event type and groups rows by link field.
    ///
    /// This is a helper method that extracts link field values and creates row indices
    /// for grouping. It's separated to reduce cyclomatic complexity.
    fn process_zones_for_event_type(
        &self,
        event_type: &str,
        zones: &[CandidateZone],
        groups: &mut HashMap<String, GroupedRowIndices>,
    ) {
        for (zone_idx, zone) in zones.iter().enumerate() {
            let accessor = PreparedAccessor::new(&zone.values);
            let event_count = accessor.event_count();

            if event_count == 0 {
                continue;
            }

            // Extract link field values and timestamps for all rows in this zone
            let mut rows_with_link = 0usize;
            let mut rows_missing_link = 0usize;

            for row_idx in 0..event_count {
                if let Some(link_value) = self.extract_link_value(&accessor, row_idx) {
                    rows_with_link += 1;
                    // Convert ScalarValue to string for HashMap key
                    let link_key = self.scalar_to_key(&link_value);
                    if tracing::enabled!(tracing::Level::DEBUG) {
                        debug!(
                            target: "sneldb::sequence::group",
                            event_type = %event_type,
                            zone_idx = zone_idx,
                            row_idx = row_idx,
                            link_value = ?link_value,
                            link_key = %link_key,
                            "Extracted link field value"
                        );
                    }
                    let row_index = RowIndex { zone_idx, row_idx };

                    let group =
                        groups
                            .entry(link_key.clone())
                            .or_insert_with(|| GroupedRowIndices {
                                link_value: link_value.clone(),
                                rows_by_type: HashMap::new(),
                            });

                    group
                        .rows_by_type
                        .entry(event_type.to_string())
                        .or_insert_with(Vec::new)
                        .push(row_index);
                } else {
                    rows_missing_link += 1;
                }
            }

            if tracing::enabled!(tracing::Level::TRACE) && rows_missing_link > 0 {
                trace!(
                    target: "sneldb::sequence::group",
                    event_type = %event_type,
                    zone_idx = zone_idx,
                    rows_with_link = rows_with_link,
                    rows_missing_link = rows_missing_link,
                    "Processed zone with missing link field values"
                );
            }

            if tracing::enabled!(tracing::Level::WARN)
                && rows_missing_link > 0
                && rows_with_link == 0
            {
                warn!(
                    target: "sneldb::sequence::group",
                    event_type = %event_type,
                    zone_idx = zone_idx,
                    link_field = %self.link_field,
                    "Zone has no rows with link field value"
                );
            }
        }
    }

    /// Sorts row indices by timestamp within each group.
    ///
    /// This is necessary for efficient two-pointer sequence matching.
    /// OPTIMIZATION: Pre-extract timestamps to avoid repeated PreparedAccessor creation during sorting.
    fn sort_groups_by_timestamp(
        &self,
        groups: &mut HashMap<String, GroupedRowIndices>,
        zones_by_event_type: &HashMap<String, Vec<CandidateZone>>,
    ) {
        if tracing::enabled!(tracing::Level::TRACE) {
            trace!(
                target: "sneldb::sequence::group",
                group_count = groups.len(),
                "Sorting row indices by timestamp within groups"
            );
        }

        for group in groups.values_mut() {
            for (event_type, row_indices) in group.rows_by_type.iter_mut() {
                if row_indices.len() <= 1 {
                    // No need to sort single or empty lists
                    continue;
                }

                if let Some(zones) = zones_by_event_type.get(event_type) {
                    // OPTIMIZATION: Pre-extract all timestamps to avoid repeated accessor creation
                    // Create a vector of (timestamp, original_index) pairs
                    let mut timestamped_indices: Vec<(u64, usize)> =
                        Vec::with_capacity(row_indices.len());
                    for (idx, row_index) in row_indices.iter().enumerate() {
                        let ts = self.get_timestamp(zones, row_index);
                        timestamped_indices.push((ts, idx));
                    }

                    // Sort by timestamp
                    timestamped_indices.sort_by_key(|(ts, _)| *ts);

                    // Rebuild row_indices in sorted order
                    let sorted_indices: Vec<RowIndex> = timestamped_indices
                        .into_iter()
                        .map(|(_, idx)| row_indices[idx].clone())
                        .collect();
                    *row_indices = sorted_indices;

                    if tracing::enabled!(tracing::Level::TRACE) {
                        let first_ts = self.get_timestamp(zones, &row_indices[0]);
                        let last_ts =
                            self.get_timestamp(zones, &row_indices[row_indices.len() - 1]);
                        trace!(
                            target: "sneldb::sequence::group",
                            event_type = %event_type,
                            row_count = row_indices.len(),
                            first_timestamp = first_ts,
                            last_timestamp = last_ts,
                            "Sorted row indices for event type"
                        );
                    }
                }
            }
        }
    }

    /// Extracts the link field value from a specific row in columnar data.
    ///
    /// Returns `None` if the field is missing or null.
    fn extract_link_value(
        &self,
        accessor: &PreparedAccessor,
        row_idx: usize,
    ) -> Option<ScalarValue> {
        // Handle special fields
        match self.link_field.as_str() {
            "context_id" => accessor
                .get_str_at("context_id", row_idx)
                .map(|s| ScalarValue::Utf8(s.to_string())),
            "timestamp" => accessor
                .get_i64_at("timestamp", row_idx)
                .map(ScalarValue::Timestamp),
            _ => {
                // Try different types using FieldAccessor trait methods
                // For numeric link fields, try i64 first (most common case)
                if let Some(i64_val) = accessor.get_i64_at(&self.link_field, row_idx) {
                    if tracing::enabled!(tracing::Level::DEBUG) {
                        debug!(
                            target: "sneldb::sequence::group",
                            link_field = %self.link_field,
                            row_idx = row_idx,
                            value = i64_val,
                            "Extracted i64 link field value"
                        );
                    }
                    Some(ScalarValue::Int64(i64_val))
                } else if let Some(u64_val) = accessor.get_u64_at(&self.link_field, row_idx) {
                    if tracing::enabled!(tracing::Level::DEBUG) {
                        debug!(
                            target: "sneldb::sequence::group",
                            link_field = %self.link_field,
                            row_idx = row_idx,
                            value = u64_val,
                            "Extracted u64 link field value"
                        );
                    }
                    Some(ScalarValue::Int64(u64_val as i64))
                } else if let Some(f64_val) = accessor.get_f64_at(&self.link_field, row_idx) {
                    if tracing::enabled!(tracing::Level::DEBUG) {
                        debug!(
                            target: "sneldb::sequence::group",
                            link_field = %self.link_field,
                            row_idx = row_idx,
                            value = f64_val,
                            "Extracted f64 link field value"
                        );
                    }
                    Some(ScalarValue::Float64(f64_val))
                } else if let Some(str_val) = accessor.get_str_at(&self.link_field, row_idx) {
                    if tracing::enabled!(tracing::Level::DEBUG) {
                        debug!(
                            target: "sneldb::sequence::group",
                            link_field = %self.link_field,
                            row_idx = row_idx,
                            value = %str_val,
                            "Extracted string link field value (may need parsing)"
                        );
                    }
                    Some(ScalarValue::Utf8(str_val.to_string()))
                } else {
                    if tracing::enabled!(tracing::Level::DEBUG) {
                        debug!(
                            target: "sneldb::sequence::group",
                            link_field = %self.link_field,
                            row_idx = row_idx,
                            "Failed to extract link field value"
                        );
                    }
                    None
                }
            }
        }
    }

    /// Gets the timestamp for a specific row index.
    ///
    /// This is used for sorting rows within groups.
    /// Uses the configured time_field (default: "timestamp").
    fn get_timestamp(&self, zones: &[CandidateZone], row_index: &RowIndex) -> u64 {
        if let Some(zone) = zones.get(row_index.zone_idx) {
            let accessor = PreparedAccessor::new(&zone.values);
            accessor
                .get_i64_at(&self.time_field, row_index.row_idx)
                .map(|ts| ts as u64)
                .unwrap_or(0)
        } else {
            0
        }
    }

    /// Converts a ScalarValue to a string key for HashMap usage.
    ///
    /// This allows us to use ScalarValue as HashMap keys without implementing Hash.
    fn scalar_to_key(&self, value: &ScalarValue) -> String {
        match value {
            ScalarValue::Null => "null".to_string(),
            ScalarValue::Boolean(b) => format!("bool:{}", b),
            ScalarValue::Int64(i) => format!("i64:{}", i),
            ScalarValue::Float64(f) => format!("f64:{}", f),
            ScalarValue::Timestamp(ts) => format!("ts:{}", ts),
            ScalarValue::Utf8(s) => format!("str:{}", s),
            ScalarValue::Binary(bytes) => {
                use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
                format!("bin:{}", BASE64_STANDARD.encode(bytes))
            }
        }
    }
}
