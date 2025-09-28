use crate::command::types::CompareOp;
use crate::engine::core::zone::zone_artifacts::ZoneArtifacts;
use crate::engine::core::{CandidateZone, FieldXorFilter, RangeQueryHandler};
use crate::engine::core::zone::selector::pruner::{PruneArgs, ZonePruner};
use tracing::{info, warn};

pub struct RangePruner<'a> {
    pub artifacts: ZoneArtifacts<'a>,
}

impl<'a> RangePruner<'a> {
    pub fn attempt(
        &self,
        segment_id: &str,
        uid: &str,
        column: &str,
        value: &serde_json::Value,
        op: &CompareOp,
    ) -> Option<Vec<CandidateZone>> {
        if !matches!(
            op,
            CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte
        ) {
            return None;
        }

        if let Ok(zsf) = self.artifacts.load_zone_surf(segment_id, uid, column) {
            if let Some(bytes) =
                crate::engine::core::filter::surf_encoding::encode_value(value).as_deref()
            {
                let zones = match op {
                    CompareOp::Gt => zsf.zones_overlapping_ge(bytes, false, segment_id),
                    CompareOp::Gte => zsf.zones_overlapping_ge(bytes, true, segment_id),
                    CompareOp::Lt => zsf.zones_overlapping_le(bytes, false, segment_id),
                    CompareOp::Lte => zsf.zones_overlapping_le(bytes, true, segment_id),
                    _ => unreachable!(),
                };
                if tracing::enabled!(tracing::Level::INFO) {
                    info!(target: "sneldb::query", value = ?value, %segment_id, zones = ?zones, "Zone Surf filter hit");
                }
                return Some(zones);
            }
        }

        if let Some(zones) =
            RangeQueryHandler::new(FieldXorFilter::new(&Vec::new()), segment_id.to_string())
                .handle_range_query(value, op)
        {
            warn!("zones: {:?}", zones);
            return Some(zones);
        }

        None
    }
}

impl<'a> ZonePruner for RangePruner<'a> {
    fn apply(&self, args: &PruneArgs) -> Option<Vec<CandidateZone>> {
        let (Some(op), Some(value)) = (args.op, args.value) else { return None; };
        self.attempt(args.segment_id, args.uid, args.column, value, op)
    }
}

