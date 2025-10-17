use crate::command::types::CompareOp;
use crate::engine::core::CandidateZone;
use crate::engine::core::zone::selector::pruner::{PruneArgs, ZonePruner};
use crate::engine::core::zone::zone_artifacts::ZoneArtifacts;
use tracing::debug;

pub struct XorPruner<'a> {
    pub artifacts: ZoneArtifacts<'a>,
}

impl<'a> XorPruner<'a> {
    pub fn attempt(
        &self,
        segment_id: &str,
        uid: &str,
        column: &str,
        value: &serde_json::Value,
        op: &CompareOp,
    ) -> Option<Vec<CandidateZone>> {
        if !matches!(op, CompareOp::Eq) {
            return None;
        }

        // Prefer zone-level XOR index; fall back to full-field XOR filter
        match self.artifacts.load_zxf(segment_id, uid, column) {
            Ok(zxf) => {
                if tracing::enabled!(tracing::Level::DEBUG) {
                    debug!(target: "sneldb::query", %column, %segment_id, "Loaded .zxf zone index");
                }
                let zone_ids = zxf.zones_maybe_containing(value);
                if tracing::enabled!(tracing::Level::DEBUG) {
                    debug!(
                        target: "sneldb::query",
                        %column,
                        value = ?value,
                        zone_count = zone_ids.len(),
                        zones = ?zone_ids,
                        "Zone XOR candidate zones"
                    );
                }
                if !zone_ids.is_empty() {
                    let maybe_zones = zone_ids
                        .into_iter()
                        .map(|z| CandidateZone::new(z, segment_id.to_string()))
                        .collect::<Vec<_>>();
                    Some(maybe_zones)
                } else {
                    // Fall back to full-field XOR filter presence check
                    match self.artifacts.load_xf(segment_id, uid, column) {
                        Ok(filter) => {
                            if filter.contains_value(value) {
                                return Some(
                                    CandidateZone::create_all_zones_for_segment_from_meta(
                                        self.artifacts.base_dir,
                                        segment_id,
                                        uid,
                                    ),
                                );
                            }
                            None
                        }
                        Err(_) => None,
                    }
                }
            }
            Err(_) => {
                // Try full-field XOR filter directly
                match self.artifacts.load_xf(segment_id, uid, column) {
                    Ok(filter) => {
                        if filter.contains_value(value) {
                            return Some(CandidateZone::create_all_zones_for_segment_from_meta(
                                self.artifacts.base_dir,
                                segment_id,
                                uid,
                            ));
                        }
                        None
                    }
                    Err(_) => None,
                }
            }
        }
    }
}

impl<'a> ZonePruner for XorPruner<'a> {
    fn apply(&self, args: &PruneArgs) -> Option<Vec<CandidateZone>> {
        let (Some(op), Some(value)) = (args.op, args.value) else {
            return None;
        };
        self.attempt(args.segment_id, args.uid, args.column, value, op)
    }
}
