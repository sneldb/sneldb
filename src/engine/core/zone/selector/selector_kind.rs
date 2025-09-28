use crate::engine::core::CandidateZone;

pub trait ZoneSelector {
    fn select_for_segment(&self, segment_id: &str) -> Vec<CandidateZone>;
}
