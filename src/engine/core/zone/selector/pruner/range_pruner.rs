use crate::command::types::CompareOp;
use crate::engine::core::CandidateZone;
use crate::engine::core::filter::surf_encoding;
use crate::engine::core::zone::selector::pruner::PruneArgs;
use crate::engine::core::zone::zone_artifacts::ZoneArtifacts;

pub struct RangePruner<'a> {
    pub artifacts: ZoneArtifacts<'a>,
}

impl<'a> RangePruner<'a> {
    /// Use only ZoneSuRF for range pruning. No fallback.
    /// Returns None if ZoneSuRF matches >90% of zones (to trigger FullScan fallback and skip ZoneSuRF overhead).
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
        match self
            .artifacts
            .load_zone_surf(args.segment_id, args.uid, args.column)
        {
            Ok(zsf) => {
                let zones_total = zsf.entries.len();
                if zones_total == 0 {
                    if tracing::enabled!(tracing::Level::WARN) {
                        tracing::warn!(
                            target: "sneldb::zone_selector",
                            segment_id = %args.segment_id,
                            column = %args.column,
                            uid = %args.uid,
                            "ZoneSuRF loaded but has 0 zones"
                        );
                    }
                    return None;
                }

                if let Some(bytes) = surf_encoding::encode_value(value).as_deref() {
                    let zones = match op {
                        CompareOp::Gt => zsf.zones_overlapping_ge(bytes, false, args.segment_id),
                        CompareOp::Gte => zsf.zones_overlapping_ge(bytes, true, args.segment_id),
                        CompareOp::Lt => zsf.zones_overlapping_le(bytes, false, args.segment_id),
                        CompareOp::Lte => zsf.zones_overlapping_le(bytes, true, args.segment_id),
                        _ => unreachable!(),
                    };

                    // If ZoneSuRF matches >90% of zones AND there are many zones (>10),
                    // return None to trigger FullScan fallback.
                    // For small zone counts (<=10), we still use ZoneSuRF even if it matches all zones,
                    // as the overhead is minimal and the query might be selective.
                    const MATCH_THRESHOLD: f64 = 0.9;
                    const MIN_ZONES_FOR_THRESHOLD: usize = 10;
                    if zones_total > MIN_ZONES_FOR_THRESHOLD
                        && zones.len() as f64 >= zones_total as f64 * MATCH_THRESHOLD
                    {
                        if tracing::enabled!(tracing::Level::WARN) {
                            tracing::warn!(
                                target: "sneldb::zone_selector",
                                segment_id = %args.segment_id,
                                column = %args.column,
                                uid = %args.uid,
                                zones_matched = zones.len(),
                                zones_total,
                                match_rate = (zones.len() as f64 / zones_total as f64),
                                "ZoneSuRF matched >90% of zones (many zones), falling back to FullScan to skip ZoneSuRF overhead"
                            );
                        }
                        return None;
                    }

                    return Some(zones);
                } else {
                    if tracing::enabled!(tracing::Level::WARN) {
                        tracing::warn!(
                            target: "sneldb::zone_selector",
                            segment_id = %args.segment_id,
                            column = %args.column,
                            uid = %args.uid,
                            "Failed to encode value for ZoneSuRF query"
                        );
                    }
                }
            }
            Err(e) => {
                if tracing::enabled!(tracing::Level::WARN) {
                    tracing::warn!(
                        target: "sneldb::zone_selector",
                        segment_id = %args.segment_id,
                        column = %args.column,
                        uid = %args.uid,
                        error = %e,
                        "Failed to load ZoneSuRF"
                    );
                }
            }
        }
        None
    }
}
