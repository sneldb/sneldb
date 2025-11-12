use crate::command::types::CompareOp;
use crate::engine::core::CandidateZone;
use crate::engine::core::time::temporal_traits::FieldIndex;
use crate::engine::core::zone::selector::pruner::PruneArgs;
use crate::engine::core::zone::zone_artifacts::ZoneArtifacts;
use crate::engine::types::ScalarValue;
use crate::shared::time::{TimeKind, TimeParser};

pub struct TemporalPruner<'a> {
    pub artifacts: ZoneArtifacts<'a>,
}

impl<'a> TemporalPruner<'a> {
    pub fn apply_temporal_only(&self, args: &PruneArgs) -> Option<Vec<CandidateZone>> {
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
        let is_timestamp = column == "timestamp";
        let ts = match value {
            ScalarValue::Int64(i) => (*i).max(0) as u64,
            ScalarValue::Timestamp(t) => (*t).max(0) as u64,
            ScalarValue::Utf8(s) => {
                if let Some(parsed) = TimeParser::parse_str_to_epoch_seconds(s, TimeKind::DateTime)
                {
                    parsed.max(0) as u64
                } else {
                    s.parse::<u64>().ok().unwrap_or(0)
                }
            }
            // JSON numbers and strings are now Utf8 - parse from string
            ScalarValue::Utf8(s) => {
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
                let mut zone_ids: Vec<u32> = Vec::new();
                if is_timestamp {
                    if let Ok(cal) =
                        self.artifacts
                            .load_field_calendar(segment_id, uid, "timestamp")
                    {
                        let zones = cal.zones_intersecting(CompareOp::Eq, ts as i64);
                        zone_ids = zones.iter().map(|zid| zid as u32).collect();
                    }
                } else if let Ok(cal) = self.artifacts.load_field_calendar(segment_id, uid, column)
                {
                    let zones = cal.zones_intersecting(CompareOp::Eq, ts as i64);
                    zone_ids = zones.iter().map(|zid| zid as u32).collect();
                } else {
                    return None;
                }
                let mut out = Vec::new();
                for zid in zone_ids {
                    let zti_result = if is_timestamp {
                        self.artifacts
                            .load_field_temporal_index(segment_id, uid, "timestamp", zid)
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
                return Some(out);
            }
            CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => {
                let mut zone_ids: Vec<u32> = Vec::new();
                if is_timestamp {
                    if let Ok(cal) =
                        self.artifacts
                            .load_field_calendar(segment_id, uid, "timestamp")
                    {
                        let cmp = match op {
                            CompareOp::Gt => CompareOp::Gt,
                            CompareOp::Gte => CompareOp::Gte,
                            CompareOp::Lt => CompareOp::Lt,
                            CompareOp::Lte => CompareOp::Lte,
                            _ => CompareOp::Eq,
                        };
                        let zones = cal.zones_intersecting(cmp, ts as i64);
                        zone_ids = zones.iter().map(|zid| zid as u32).collect();
                    }
                } else if let Ok(cal) = self.artifacts.load_field_calendar(segment_id, uid, column)
                {
                    let cmp = match op {
                        CompareOp::Gt => CompareOp::Gt,
                        CompareOp::Gte => CompareOp::Gte,
                        CompareOp::Lt => CompareOp::Lt,
                        CompareOp::Lte => CompareOp::Lte,
                        _ => CompareOp::Eq,
                    };
                    let zones = cal.zones_intersecting(cmp, ts as i64);
                    zone_ids = zones.iter().map(|zid| zid as u32).collect();
                } else {
                    return None;
                }
                let mut out = Vec::new();
                for zid in zone_ids {
                    let zti_result = if is_timestamp {
                        self.artifacts
                            .load_field_temporal_index(segment_id, uid, "timestamp", zid)
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
                return Some(out);
            }
            _ => {}
        }
        None
    }
}
