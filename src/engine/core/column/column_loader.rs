use crate::engine::core::{CandidateZone, ColumnReader};
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

        let mut result = HashMap::new();

        for column in columns {
            let values = self.read_column_for_zone(zone, column);
            debug!(target: "col_loader::values", column = %column, values_len = values.len(), "Loaded values for column");
            result.insert(column.clone(), values);
        }

        result
    }

    /// Reads values for a column using the compressed zone index (.zfc)
    fn read_column_for_zone(&self, zone: &CandidateZone, column: &str) -> Vec<String> {
        let segment_dir = self.segment_base_dir.join(&zone.segment_id);
        ColumnReader::load_for_zone(
            &segment_dir,
            &zone.segment_id,
            &self.uid,
            column,
            zone.zone_id,
        )
        .unwrap_or_default()
    }
}
