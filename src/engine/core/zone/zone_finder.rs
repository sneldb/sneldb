use crate::command::types::CompareOp;
use crate::engine::core::filter::zone_surf_filter::ZoneSurfFilter;
use crate::engine::core::zone::enum_bitmap_index::EnumBitmapIndex;
use crate::engine::core::zone::enum_zone_pruner::EnumZonePruner;
use crate::engine::core::{
    CandidateZone, FieldXorFilter, FilterPlan, QueryCaches, QueryPlan, RangeQueryHandler, ZoneIndex,
};
use std::path::PathBuf;
use tracing::{debug, error, info, warn};

pub struct ZoneFinder<'a> {
    plan: &'a FilterPlan,
    query_plan: &'a QueryPlan,
    segment_ids: &'a [String],
    base_dir: &'a PathBuf,
    caches: Option<&'a QueryCaches>,
}

impl<'a> ZoneFinder<'a> {
    pub fn new(
        plan: &'a FilterPlan,
        query_plan: &'a QueryPlan,
        segment_ids: &'a [String],
        base_dir: &'a PathBuf,
    ) -> Self {
        Self {
            plan,
            query_plan,
            segment_ids,
            base_dir,
            caches: None,
        }
    }

    pub fn with_caches(mut self, caches: Option<&'a QueryCaches>) -> Self {
        self.caches = caches;
        self
    }

    pub fn find(&self) -> Vec<CandidateZone> {
        debug!(target: "sneldb::query", "Finding candidate zones for filter: {:?}", self.plan);
        let mut out: Vec<CandidateZone> = Vec::new();
        out.reserve(self.segment_ids.len());
        for segment_id in self.segment_ids.iter() {
            let zones = match self.plan.column.as_str() {
                "event_type" => self.find_event_type_zones(segment_id),
                "context_id" => self.find_context_id_zones(segment_id),
                _ => self.find_field_zones(segment_id),
            };
            if tracing::enabled!(tracing::Level::DEBUG) {
                debug!(target: "sneldb::query", segment = %segment_id, column = %self.plan.column, zones = zones.len(), "Segment zones computed");
            }
            out.extend(zones);
        }
        out
    }

    fn find_event_type_zones(&self, segment_id: &str) -> Vec<CandidateZone> {
        let uid = match &self.plan.uid {
            Some(uid) => uid,
            None => {
                warn!(target: "sneldb::query", "Missing UID for event_type in {}", segment_id);
                return vec![];
            }
        };

        let event_type = match self.plan.value.as_ref().and_then(|v| v.as_str()) {
            Some(e) => e,
            None => {
                warn!(target: "sneldb::query", "Missing value for event_type in segment {}", segment_id);
                return vec![];
            }
        };

        let context_id = self
            .query_plan
            .context_id_plan()
            .and_then(|p| p.value.as_ref().and_then(|v| v.as_str()));

        // Try cache first; fall back to disk
        if let Some(caches) = self.caches {
            if let Ok(index) = caches.get_or_load_zone_index(segment_id, uid) {
                info!(
                    target: "cache::zone_index::hit",
                    %segment_id,
                    %uid,
                    "Using cached ZoneIndex"
                );
                return index.find_candidate_zones(event_type, context_id, segment_id);
            }
        }

        let path = self.index_path(segment_id, uid);
        ZoneIndex::load_from_path(&path)
            .map(|index| index.find_candidate_zones(event_type, context_id, segment_id))
            .unwrap_or_else(|err| {
                error!(target: "sneldb::query", "Failed to load index from {:?}: {:?}", path, err);
                vec![]
            })
    }

    fn find_context_id_zones(&self, segment_id: &str) -> Vec<CandidateZone> {
        let uid = match &self.plan.uid {
            Some(uid) => uid,
            None => {
                debug!(target: "sneldb::query", "No UID, returning all zones for segment {}", segment_id);
                return CandidateZone::create_all_zones_for_segment(segment_id);
            }
        };

        let event_type = self
            .query_plan
            .event_type_plan()
            .and_then(|p| p.value.as_ref().and_then(|v| v.as_str()));

        let context_id = self.plan.value.as_ref().and_then(|v| v.as_str());

        match event_type {
            Some(event_type) => {
                // Try cache first; fall back to disk
                if let Some(caches) = self.caches {
                    if let Ok(index) = caches.get_or_load_zone_index(segment_id, uid) {
                        info!(
                            target: "cache::zone_index::hit",
                            %segment_id,
                            %uid,
                            "Using cached ZoneIndex"
                        );
                        return index.find_candidate_zones(event_type, context_id, segment_id);
                    }
                }

                let path = self.index_path(segment_id, uid);
                ZoneIndex::load_from_path(&path)
                    .map(|index| index.find_candidate_zones(event_type, context_id, segment_id))
                    .unwrap_or_else(|err| {
                        error!(target: "sneldb::query", "Failed to load index from {:?}: {:?}", path, err);
                        CandidateZone::create_all_zones_for_segment(segment_id)
                    })
            }
            None => {
                debug!(target: "sneldb::query", "No event_type available for context_id, falling back to all zones");
                CandidateZone::create_all_zones_for_segment(segment_id)
            }
        }
    }

    fn find_field_zones(&self, segment_id: &str) -> Vec<CandidateZone> {
        let uid = match &self.plan.uid {
            Some(uid) => uid,
            None => {
                warn!(target: "sneldb::query", "Missing UID for field column {} in {}", self.plan.column, segment_id);
                return vec![];
            }
        };

        let value = match self.plan.value.as_ref() {
            Some(v) => v,
            None => {
                debug!(target: "sneldb::query", "No value for field column {}, using all zones", self.plan.column);
                return CandidateZone::create_all_zones_for_segment(segment_id);
            }
        };

        if let Some(op) = &self.plan.operation {
            // Prefer zone-level SuRF only for range operations
            if matches!(
                op,
                CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte
            ) {
                let zsurf_path = self.zone_surf_path(segment_id, uid, &self.plan.column);
                if let Ok(zsf) = ZoneSurfFilter::load(&zsurf_path) {
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
                        info!(target: "sneldb::query", "Zone Surf filter found for value {:?} in segment {} zones: {:?}", value, segment_id, zones);
                        return zones;
                    }
                }
                if let Some(zones) =
                    RangeQueryHandler::new(FieldXorFilter::new(&Vec::new()), segment_id.to_string())
                        .handle_range_query(value, op)
                {
                    return zones;
                }
            }
        }

        // Try EBM-based pruning only for enum fields and Eq/Neq.
        // Avoid blocking the Tokio runtime thread; use try_read and skip if unavailable.
        if self
            .query_plan
            .registry
            .try_read()
            .map(|reg| reg.is_enum_field_by_uid(uid, &self.plan.column))
            .unwrap_or(false)
        {
            if let (Some(op), Some(val_str)) = (&self.plan.operation, value.as_str()) {
                if let Some(pruned) =
                    self.try_ebm_prune(segment_id, uid, &self.plan.column, op, val_str)
                {
                    return pruned;
                }
            }
        }

        if let Some(op) = &self.plan.operation {
            // Prefer XOR filter only for Eq operation
            if matches!(op, CompareOp::Eq) {
                let path = self.filter_path(segment_id, uid, &self.plan.column);
                let filter = match FieldXorFilter::load(&path) {
                    Ok(f) => f,
                    Err(err) => {
                        error!(target: "sneldb::query", "Failed to load XOR filter from {:?}: {:?}", path, err);
                        return vec![];
                    }
                };

                if filter.contains_value(value) {
                    debug!(target: "sneldb::query", "XOR filter positive hit for value {:?}", value);
                    return CandidateZone::create_all_zones_for_segment(segment_id);
                }
            }
        }

        debug!(target: "sneldb::query", "XOR filter miss for value {:?}", value);
        vec![]
    }

    fn index_path(&self, segment_id: &str, uid: &str) -> PathBuf {
        self.base_dir.join(segment_id).join(format!("{}.idx", uid))
    }

    fn filter_path(&self, segment_id: &str, uid: &str, column: &str) -> PathBuf {
        self.base_dir
            .join(segment_id)
            .join(format!("{}_{}.xf", uid, column))
    }

    fn zone_surf_path(&self, segment_id: &str, uid: &str, column: &str) -> PathBuf {
        self.base_dir
            .join(segment_id)
            .join(format!("{}_{}.zsrf", uid, column))
    }

    fn ebm_path(&self, segment_id: &str, uid: &str, column: &str) -> PathBuf {
        self.base_dir
            .join(segment_id)
            .join(format!("{}_{}.ebm", uid, column))
    }

    fn try_ebm_prune(
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

        let ebm_path = self.ebm_path(segment_id, uid, column);
        let index = match EnumBitmapIndex::load(&ebm_path) {
            Ok(idx) => idx,
            Err(_) => {
                debug!(target: "sneldb::query", column = %column, path = %ebm_path.display(), "EBM not found or unreadable; falling back to XOR");
                return None;
            }
        };

        let Some(variant_id) = index.variants.iter().position(|v| v == val_str) else {
            debug!(target: "sneldb::query", column = %column, %val_str, "EBM present but variant not found; falling back to XOR");
            return None;
        };

        let pruner = EnumZonePruner {
            segment_id,
            ebm: &index,
        };
        let zones = pruner.prune(op, variant_id);
        Some(CandidateZone::uniq(zones))
    }
}
