use crate::command::types::CompareOp;
use crate::engine::core::CandidateZone;
use crate::engine::core::time::calendar_dir::GranularityPref;
use crate::engine::core::time::temporal_traits::FieldIndex;
use crate::engine::core::zone::selector::pruner::{PruneArgs, ZonePruner};
use crate::engine::core::zone::zone_artifacts::ZoneArtifacts;
use crate::shared::time::{TimeKind, TimeParser};
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
        // Support any temporal field. If not timestamp, try field-aware calendars/indexes.
        let is_timestamp = column == "timestamp";
        let ts = match value {
            serde_json::Value::Number(n) => n.as_u64().unwrap_or(0),
            serde_json::Value::String(s) => {
                if let Some(parsed) = TimeParser::parse_str_to_epoch_seconds(s, TimeKind::DateTime)
                {
                    parsed.max(0) as u64
                } else {
                    s.parse::<u64>().ok().unwrap_or(0)
                }
            }
            _ => 0,
        };

        match op {
            CompareOp::Eq => {
                // First try legacy timestamp calendar
                let mut zone_ids = if is_timestamp {
                    self.pick_from_calendar(segment_id, uid, ts)
                } else {
                    Vec::new()
                };
                // If empty and a per-field calendar exists, use it
                let mut had_field_calendar = false;
                if zone_ids.is_empty() {
                    if let Some(caches) = self.artifacts.caches {
                        if let Ok(cal) = caches.get_or_load_field_calendar(segment_id, uid, column)
                        {
                            had_field_calendar = true;
                            let zones = cal.zones_intersecting(
                                crate::command::types::CompareOp::Eq,
                                ts as i64,
                            );
                            zone_ids = zones.iter().map(|z| z as u32).collect();
                        }
                    } else if let Ok(cal) =
                        self.artifacts.load_field_calendar(segment_id, uid, column)
                    {
                        had_field_calendar = true;
                        let zones =
                            cal.zones_intersecting(crate::command::types::CompareOp::Eq, ts as i64);
                        zone_ids = zones.iter().map(|z| z as u32).collect();
                    }
                }
                // If not timestamp and no field calendar available, defer to other pruners
                if !is_timestamp && !had_field_calendar {
                    return None;
                }
                let mut out = Vec::new();
                for zid in zone_ids {
                    // try cache first
                    if let Some(caches) = self.artifacts.caches {
                        // Prefer field-aware zti if column is a temporal payload field
                        let zti_result = if is_timestamp {
                            caches.get_or_load_temporal_index(segment_id, uid, zid)
                        } else {
                            caches.get_or_load_field_temporal_index(segment_id, uid, column, zid)
                        };
                        if let Ok(zti) = zti_result {
                            if zti.contains_ts(ts as i64) {
                                out.push(CandidateZone::new(zid, segment_id.to_string()));
                                continue;
                            } else {
                                continue;
                            }
                        }
                    }
                    let zti_result = if is_timestamp {
                        self.artifacts.load_temporal_index(segment_id, uid, zid)
                    } else {
                        self.artifacts
                            .load_field_temporal_index(segment_id, uid, column, zid)
                    };
                    if let Ok(zti) = zti_result {
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
                let mut zone_ids = if is_timestamp {
                    self.pick_from_calendar(segment_id, uid, ts)
                } else {
                    Vec::new()
                };
                let mut had_field_calendar = false;
                if zone_ids.is_empty() {
                    if let Some(caches) = self.artifacts.caches {
                        if let Ok(cal) = caches.get_or_load_field_calendar(segment_id, uid, column)
                        {
                            had_field_calendar = true;
                            let cmp = match op {
                                CompareOp::Gt => CompareOp::Gt,
                                CompareOp::Gte => CompareOp::Gte,
                                CompareOp::Lt => CompareOp::Lt,
                                CompareOp::Lte => CompareOp::Lte,
                                _ => CompareOp::Eq,
                            };
                            let zones = cal.zones_intersecting(cmp, ts as i64);
                            zone_ids = zones.iter().map(|z| z as u32).collect();
                        }
                    } else if let Ok(cal) =
                        self.artifacts.load_field_calendar(segment_id, uid, column)
                    {
                        had_field_calendar = true;
                        let cmp = match op {
                            CompareOp::Gt => CompareOp::Gt,
                            CompareOp::Gte => CompareOp::Gte,
                            CompareOp::Lt => CompareOp::Lt,
                            CompareOp::Lte => CompareOp::Lte,
                            _ => CompareOp::Eq,
                        };
                        let zones = cal.zones_intersecting(cmp, ts as i64);
                        zone_ids = zones.iter().map(|z| z as u32).collect();
                    }
                }
                if !is_timestamp && !had_field_calendar {
                    return None;
                }
                let mut out = Vec::new();
                for zid in zone_ids {
                    if let Some(caches) = self.artifacts.caches {
                        let zti_result = if is_timestamp {
                            caches.get_or_load_temporal_index(segment_id, uid, zid)
                        } else {
                            caches.get_or_load_field_temporal_index(segment_id, uid, column, zid)
                        };
                        if let Ok(zti) = zti_result {
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
                    let zti_result = if is_timestamp {
                        self.artifacts.load_temporal_index(segment_id, uid, zid)
                    } else {
                        self.artifacts
                            .load_field_temporal_index(segment_id, uid, column, zid)
                    };
                    if let Ok(zti) = zti_result {
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
