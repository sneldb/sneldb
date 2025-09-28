use crate::engine::core::CandidateZone;
use crate::engine::core::zone::selector::pruner::PruneArgs;

pub trait ZonePruner {
    fn apply(&self, args: &PruneArgs) -> Option<Vec<CandidateZone>>;
}
