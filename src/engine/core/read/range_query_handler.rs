use crate::command::types::CompareOp;
use crate::engine::core::{CandidateZone, FieldXorFilter};
use serde_json::Value;
use tracing::{info, warn};

/// Handles range-based zone pruning using XOR filters
pub struct RangeQueryHandler {
    segment_id: String,
    uid: Option<String>,
    base_dir: Option<std::path::PathBuf>,
}

impl RangeQueryHandler {
    pub fn new(_filter: FieldXorFilter, segment_id: String) -> Self {
        Self {
            segment_id,
            uid: None,
            base_dir: None,
        }
    }

    /// Returns a list of candidate zones for a given range query
    pub fn handle_range_query(
        &self,
        value: &Value,
        operation: &CompareOp,
    ) -> Option<Vec<CandidateZone>> {
        match operation {
            CompareOp::Gt | CompareOp::Gte => {
                self.handle_greater_than(value, matches!(operation, CompareOp::Gte))
            }
            CompareOp::Lt | CompareOp::Lte => {
                self.handle_less_than(value, matches!(operation, CompareOp::Lte))
            }
            _ => {
                warn!(
                    target: "sneldb::query::range",
                    "Unsupported operation in RangeQueryHandler: {:?}",
                    operation
                );
                None
            }
        }
    }

    fn handle_greater_than(&self, value: &Value, _inclusive: bool) -> Option<Vec<CandidateZone>> {
        info!(
            target: "sneldb::query::range",
            "Range query (>) - value: {:?}, segment: {}",
            value, self.segment_id
        );

        Some(match (&self.base_dir, &self.uid) {
            (Some(dir), Some(uid)) => {
                CandidateZone::create_all_zones_for_segment_from_meta(dir, &self.segment_id, uid)
            }
            _ => CandidateZone::create_all_zones_for_segment(&self.segment_id),
        })
    }

    fn handle_less_than(&self, value: &Value, _inclusive: bool) -> Option<Vec<CandidateZone>> {
        info!(
            target: "sneldb::query::range",
            "Range query (<) - value: {:?}, segment: {}",
            value, self.segment_id
        );

        Some(match (&self.base_dir, &self.uid) {
            (Some(dir), Some(uid)) => {
                CandidateZone::create_all_zones_for_segment_from_meta(dir, &self.segment_id, uid)
            }
            _ => CandidateZone::create_all_zones_for_segment(&self.segment_id),
        })
    }
}
