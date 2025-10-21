use crate::command::types::CompareOp;
use crate::engine::core::CandidateZone;
use crate::engine::core::zone::selector::pruner::PruneArgs;
use crate::engine::core::zone::zone_artifacts::ZoneArtifacts;
use tracing::debug;

pub struct XorPruner<'a> {
    pub artifacts: ZoneArtifacts<'a>,
}

impl<'a> XorPruner<'a> {
    /// Use only the zone-level XOR index (.zxf). No fallback.
    pub fn apply_zone_index_only(&self, args: &PruneArgs) -> Option<Vec<CandidateZone>> {
        let (Some(op), Some(value)) = (args.op, args.value) else {
            return None;
        };
        if !matches!(op, CompareOp::Eq) {
            return None;
        }
        if let Ok(zxf) = self
            .artifacts
            .load_zxf(args.segment_id, args.uid, args.column)
        {
            if tracing::enabled!(tracing::Level::DEBUG) {
                debug!(target: "sneldb::query", column = %args.column, segment = %args.segment_id, "Loaded .zxf zone index");
            }
            let zone_ids = zxf.zones_maybe_containing(value);
            if !zone_ids.is_empty() {
                return Some(
                    zone_ids
                        .into_iter()
                        .map(|z| CandidateZone::new(z, args.segment_id.to_string()))
                        .collect(),
                );
            }
            return Some(Vec::new());
        }
        None
    }

    /// Use only the full-field XOR filter (.xf) presence. No fallback.
    pub fn apply_presence_only(&self, args: &PruneArgs) -> Option<Vec<CandidateZone>> {
        let (Some(op), Some(value)) = (args.op, args.value) else {
            return None;
        };
        if !matches!(op, CompareOp::Eq) {
            return None;
        }
        if let Ok(filter) = self
            .artifacts
            .load_xf(args.segment_id, args.uid, args.column)
        {
            if filter.contains_value(value) {
                return Some(CandidateZone::create_all_zones_for_segment_from_meta(
                    self.artifacts.base_dir,
                    args.segment_id,
                    args.uid,
                ));
            }
            return Some(Vec::new());
        }
        None
    }

    // Removed legacy attempt() with probing and fallback
}
