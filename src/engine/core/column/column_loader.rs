use crate::engine::core::{CandidateZone, ColumnOffsets, ColumnReader};
use std::collections::HashMap;
use std::path::PathBuf;
use tracing::{debug, info};

/// Handles loading column values for a specific zone
pub struct ColumnLoader {
    segment_base_dir: PathBuf,
    uid: String,
}

impl ColumnLoader {
    /// Creates a new ColumnLoader for the given segment and event type
    pub fn new(segment_base_dir: PathBuf, uid: String) -> Self {
        info!(
            target: "col_loader::init",
            ?segment_base_dir,
            %uid,
            "Initializing ColumnLoader"
        );
        Self {
            segment_base_dir,
            uid,
        }
    }

    /// Loads all column values for a zone into a map of column name to values
    pub fn load_all_columns(
        &self,
        zone: &CandidateZone,
        columns: &[String],
    ) -> HashMap<String, Vec<String>> {
        info!(
            target: "col_loader::load",
            zone_id = %zone.zone_id,
            segment_id = %zone.segment_id,
            uid = %self.uid,
            ?columns,
            "Loading columns for zone"
        );

        let offsets = self.collect_zone_offsets(zone, columns);
        debug!(target: "col_loader::offsets", ?offsets, "Resolved offsets per column");

        let mut result = HashMap::new();

        for (column, offsets) in offsets {
            let values = self.read_column_at_offsets(zone, &column, &offsets);
            debug!(target: "col_loader::values", column = %column, values_len = values.len(), "Loaded values for column");
            result.insert(column, values);
        }

        result
    }

    /// Collects offsets for all columns in a zone
    fn collect_zone_offsets(
        &self,
        zone: &CandidateZone,
        columns: &[String],
    ) -> HashMap<String, Vec<u64>> {
        let mut all_offsets = HashMap::new();

        for column in columns {
            debug!(
                target: "col_loader::offsets",
                column = %column,
                "Loading offsets for column"
            );
            let offsets = ColumnOffsets::load(
                &self.segment_base_dir.join(&zone.segment_id),
                &zone.segment_id,
                &self.uid,
                column,
            );

            match offsets {
                Ok(offset_map) => {
                    if let Some(offset_vec) = offset_map.get(&zone.zone_id) {
                        if !offset_vec.is_empty() {
                            all_offsets.insert(column.clone(), offset_vec.clone());
                        }
                    }
                }
                Err(e) => {
                    debug!(
                        target: "col_loader::offsets",
                        column = %column,
                        error = ?e,
                        "Failed to load offsets"
                    );
                }
            }
        }

        all_offsets
    }

    /// Reads values from a column file at the specified offsets
    fn read_column_at_offsets(
        &self,
        zone: &CandidateZone,
        column: &str,
        offsets: &[u64],
    ) -> Vec<String> {
        ColumnReader::load(
            &self.segment_base_dir.join(&zone.segment_id),
            &zone.segment_id,
            &self.uid,
            column,
            offsets,
        )
        .unwrap_or_default()
    }
}
