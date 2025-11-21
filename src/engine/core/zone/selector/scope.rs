use crate::engine::core::read::event_scope::EventScope;
use crate::engine::core::{CandidateZone, QueryCaches, QueryPlan};
use tracing::debug;

pub fn collect_zones_for_scope(
    plan: &QueryPlan,
    caches: Option<&QueryCaches>,
    segment_id: &str,
    uid_override: Option<&str>,
) -> Vec<CandidateZone> {
    if let Some(uid) = uid_override {
        if !segment_contains_uid(plan, segment_id, uid) {
            return Vec::new();
        }
        return CandidateZone::create_all_zones_for_segment_from_meta_cached(
            &plan.segment_base_dir,
            segment_id,
            uid,
            caches,
        );
    }

    match plan.event_scope() {
        EventScope::Specific { uid: Some(uid), .. } => {
            if !segment_contains_uid(plan, segment_id, uid) {
                return Vec::new();
            }
            CandidateZone::create_all_zones_for_segment_from_meta_cached(
                &plan.segment_base_dir,
                segment_id,
                uid,
                caches,
            )
        }
        EventScope::Specific { uid: None, .. } => {
            CandidateZone::create_all_zones_for_segment(segment_id)
        }
        EventScope::Wildcard { pairs } if !pairs.is_empty() => {
            let mut zones = Vec::new();
            for (_, uid) in pairs {
                if !segment_contains_uid(plan, segment_id, uid) {
                    continue;
                }
                zones.extend(
                    CandidateZone::create_all_zones_for_segment_from_meta_cached(
                        &plan.segment_base_dir,
                        segment_id,
                        uid,
                        caches,
                    )
                    .into_iter(),
                );
            }
            CandidateZone::uniq(zones)
        }
        EventScope::Wildcard { .. } => CandidateZone::create_all_zones_for_segment(segment_id),
    }
}

fn segment_contains_uid(plan: &QueryPlan, segment_id: &str, uid: &str) -> bool {
    if plan.segment_maybe_contains_uid(segment_id, uid) {
        return true;
    }

    if tracing::enabled!(tracing::Level::DEBUG) {
        debug!(
            target: "sneldb::zone_selector",
            %segment_id,
            %uid,
            "Skipping segment because it does not contain the requested uid"
        );
    }
    false
}
