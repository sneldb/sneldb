use crate::command::types::CompareOp;
use crate::engine::core::{CandidateZone, FieldXorFilter, QueryCaches};
use crate::engine::types::ScalarValue;
use tracing::{info, warn};

/// Handles range-based zone pruning using XOR filters
pub struct RangeQueryHandler {
    segment_id: String,
    uid: Option<String>,
    base_dir: Option<std::path::PathBuf>,
    caches: Option<std::sync::Arc<QueryCaches>>,
}

impl RangeQueryHandler {
    pub fn new(_filter: FieldXorFilter, segment_id: String) -> Self {
        Self {
            segment_id,
            uid: None,
            base_dir: None,
            caches: None,
        }
    }

    pub fn with_uid(mut self, uid: String) -> Self {
        self.uid = Some(uid);
        self
    }

    pub fn with_base_dir(mut self, base_dir: std::path::PathBuf) -> Self {
        self.base_dir = Some(base_dir);
        self
    }

    pub fn with_caches(mut self, caches: std::sync::Arc<QueryCaches>) -> Self {
        self.caches = Some(caches);
        self
    }

    /// Returns a list of candidate zones for a given range query
    pub fn handle_range_query(
        &self,
        value: &ScalarValue,
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

    fn handle_greater_than(
        &self,
        value: &ScalarValue,
        _inclusive: bool,
    ) -> Option<Vec<CandidateZone>> {
        info!(
            target: "sneldb::query::range",
            "Range query (>) - value: {:?}, segment: {}",
            value, self.segment_id
        );

        Some(match (&self.base_dir, &self.uid) {
            (Some(dir), Some(uid)) => CandidateZone::create_all_zones_for_segment_from_meta_cached(
                dir,
                &self.segment_id,
                uid,
                self.caches.as_deref(),
            ),
            _ => CandidateZone::create_all_zones_for_segment(&self.segment_id),
        })
    }

    fn handle_less_than(
        &self,
        value: &ScalarValue,
        _inclusive: bool,
    ) -> Option<Vec<CandidateZone>> {
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
