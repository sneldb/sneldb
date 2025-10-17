use crate::command::types::CompareOp;
use crate::engine::core::CandidateZone;
use crate::engine::core::zone::enum_bitmap_index::EnumBitmapIndex;
use tracing::debug;

pub struct EnumZonePruner<'a> {
    pub segment_id: &'a str,
    pub ebm: &'a EnumBitmapIndex,
}

impl<'a> EnumZonePruner<'a> {
    pub fn prune(&self, op: &CompareOp, variant_id: usize) -> Vec<CandidateZone> {
        let mut zones: Vec<CandidateZone> = Vec::new();
        for (zone_id, bitsets) in &self.ebm.zone_bitmaps {
            let include = match op {
                CompareOp::Eq => bitsets
                    .get(variant_id)
                    .map(|bytes| bytes.iter().any(|b| *b != 0))
                    .unwrap_or(false),
                CompareOp::Neq => {
                    let mut any = false;
                    for (i, bytes) in bitsets.iter().enumerate() {
                        if i == variant_id {
                            continue;
                        }
                        if bytes.iter().any(|b| *b != 0) {
                            any = true;
                            break;
                        }
                    }
                    any
                }
                _ => false,
            };
            if include {
                zones.push(CandidateZone::new(*zone_id, self.segment_id.to_string()));
            }
        }
        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                target = "sneldb::query",
                pruned = zones.len(),
                "EBM pruning applied"
            );
        }
        zones
    }
}
