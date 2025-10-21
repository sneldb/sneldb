use crate::command::types::CompareOp;
use crate::engine::core::zone::selector::pruner::PruneArgs;
use crate::engine::core::zone::zone_artifacts::ZoneArtifacts;
use crate::engine::core::CandidateZone;

pub struct RangePruner<'a> {
    pub artifacts: ZoneArtifacts<'a>,
}

impl<'a> RangePruner<'a> {
    /// Use only ZoneSuRF for range pruning. No fallback.
    pub fn apply_surf_only(&self, args: &PruneArgs) -> Option<Vec<CandidateZone>> {
        let (Some(op), Some(value)) = (args.op, args.value) else {
            return None;
        };
        if !matches!(
            op,
            CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte
        ) {
            return None;
        }
        if let Ok(zsf) = self
            .artifacts
            .load_zone_surf(args.segment_id, args.uid, args.column)
        {
            if let Some(bytes) =
                crate::engine::core::filter::surf_encoding::encode_value(value).as_deref()
            {
                let zones = match op {
                    CompareOp::Gt => zsf.zones_overlapping_ge(bytes, false, args.segment_id),
                    CompareOp::Gte => zsf.zones_overlapping_ge(bytes, true, args.segment_id),
                    CompareOp::Lt => zsf.zones_overlapping_le(bytes, false, args.segment_id),
                    CompareOp::Lte => zsf.zones_overlapping_le(bytes, true, args.segment_id),
                    _ => unreachable!(),
                };
                return Some(zones);
            }
        }
        None
    }
}
