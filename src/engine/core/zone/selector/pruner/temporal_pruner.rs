use crate::command::types::CompareOp;
use crate::engine::core::CandidateZone;
use crate::engine::core::time::calendar_dir::GranularityPref;
use crate::engine::core::zone::selector::pruner::{PruneArgs, ZonePruner};
use crate::engine::core::zone::zone_artifacts::ZoneArtifacts;
use tracing::info;

pub struct TemporalPruner<'a> {
    pub artifacts: ZoneArtifacts<'a>,
}

impl<'a> TemporalPruner<'a> {
    fn pick_from_calendar(&self, segment_id: &str, uid: &str, ts: u64) -> Vec<u32> {
        if let Some(caches) = self.artifacts.caches {
            if let Ok(cal) = caches.get_or_load_calendar(segment_id, uid) {
                let zones = cal.zones_for(ts, GranularityPref::Hour);
                if !zones.is_empty() {
                    return zones.iter().map(|z| z as u32).collect();
                }
                let zones = cal.zones_for(ts, GranularityPref::Day);
                return zones.iter().map(|z| z as u32).collect();
            }
        }
        if let Ok(cal) = self.artifacts.load_calendar(segment_id, uid) {
            let zones = cal.zones_for(ts, GranularityPref::Hour);
            if !zones.is_empty() {
                return zones.iter().map(|z| z as u32).collect();
            }
            let zones = cal.zones_for(ts, GranularityPref::Day);
            return zones.iter().map(|z| z as u32).collect();
        }
        Vec::new()
    }
}

impl<'a> ZonePruner for TemporalPruner<'a> {
    fn apply(&self, args: &PruneArgs) -> Option<Vec<CandidateZone>> {
        let segment_id = args.segment_id;
        let uid = args.uid;
        let column = args.column;
        let value = match args.value {
            Some(v) => v,
            None => return None,
        };
        let op = match args.op {
            Some(o) => o,
            None => return None,
        };
        if column != "timestamp" {
            return None;
        }
        let ts = match value {
            serde_json::Value::Number(n) => n.as_u64().unwrap_or(0),
            serde_json::Value::String(s) => s.parse::<u64>().ok().unwrap_or(0),
            _ => 0,
        };

        match op {
            CompareOp::Eq => {
                let zone_ids = self.pick_from_calendar(segment_id, uid, ts);
                let mut out = Vec::new();
                for zid in zone_ids {
                    // try cache first
                    if let Some(caches) = self.artifacts.caches {
                        if let Ok(zti) = caches.get_or_load_temporal_index(segment_id, uid, zid) {
                            if zti.contains_ts(ts as i64) {
                                out.push(CandidateZone::new(zid, segment_id.to_string()));
                                continue;
                            } else {
                                continue;
                            }
                        }
                    }
                    if let Ok(zti) = self.artifacts.load_temporal_index(segment_id, uid, zid) {
                        if zti.contains_ts(ts as i64) {
                            out.push(CandidateZone::new(zid, segment_id.to_string()));
                        }
                    }
                }
                if !out.is_empty() {
                    if tracing::enabled!(tracing::Level::INFO) {
                        info!(target: "sneldb::query::temporal", %segment_id, count = out.len(), "TemporalPruner hit (Eq)");
                    }
                    Some(out)
                } else {
                    Some(Vec::new())
                }
            }
            CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => {
                let zone_ids = self.pick_from_calendar(segment_id, uid, ts);
                let mut out = Vec::new();
                for zid in zone_ids {
                    if let Some(caches) = self.artifacts.caches {
                        if let Ok(zti) = caches.get_or_load_temporal_index(segment_id, uid, zid) {
                            let overlaps = match op {
                                CompareOp::Gt => zti.max_ts > ts as i64,
                                CompareOp::Gte => zti.max_ts >= ts as i64,
                                CompareOp::Lt => zti.min_ts < ts as i64,
                                CompareOp::Lte => zti.min_ts <= ts as i64,
                                _ => false,
                            };
                            if overlaps {
                                out.push(CandidateZone::new(zid, segment_id.to_string()));
                            }
                            continue;
                        }
                    }
                    if let Ok(zti) = self.artifacts.load_temporal_index(segment_id, uid, zid) {
                        let overlaps = match op {
                            CompareOp::Gt => zti.max_ts > ts as i64,
                            CompareOp::Gte => zti.max_ts >= ts as i64,
                            CompareOp::Lt => zti.min_ts < ts as i64,
                            CompareOp::Lte => zti.min_ts <= ts as i64,
                            _ => false,
                        };
                        if overlaps {
                            out.push(CandidateZone::new(zid, segment_id.to_string()));
                        }
                    }
                }
                Some(out)
            }
            _ => None,
        }
    }
}
