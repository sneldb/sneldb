use crate::command::types::CompareOp;
use crate::engine::core::CandidateZone;
use crate::engine::core::zone::enum_zone_pruner::EnumZonePruner;
use crate::engine::core::zone::selector::pruner::{PruneArgs, ZonePruner};
use crate::engine::core::zone::zone_artifacts::ZoneArtifacts;

pub struct EnumPruner<'a> {
    pub artifacts: ZoneArtifacts<'a>,
}

impl<'a> EnumPruner<'a> {
    pub fn attempt(
        &self,
        segment_id: &str,
        uid: &str,
        column: &str,
        op: &CompareOp,
        val_str: &str,
    ) -> Option<Vec<CandidateZone>> {
        if !matches!(op, CompareOp::Eq | CompareOp::Neq) {
            return None;
        }
        let index = match self.artifacts.load_ebm(segment_id, uid, column) {
            Ok(idx) => idx,
            Err(_) => return None,
        };

        let Some(variant_id) = index.variants.iter().position(|v| v == val_str) else {
            return None;
        };

        let pruner = EnumZonePruner {
            segment_id,
            ebm: &index,
        };
        let zones = pruner.prune(op, variant_id);
        Some(zones)
    }
}

impl<'a> ZonePruner for EnumPruner<'a> {
    fn apply(&self, args: &PruneArgs) -> Option<Vec<CandidateZone>> {
        let (Some(op), Some(value)) = (args.op, args.value) else {
            return None;
        };
        let Some(val_str) = value.as_str() else {
            return None;
        };
        self.attempt(args.segment_id, args.uid, args.column, op, val_str)
    }
}
