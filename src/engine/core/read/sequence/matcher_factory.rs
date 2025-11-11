use crate::command::types::EventSequence;
use crate::engine::core::read::sequence::matching_strategy::MatchingStrategy;
use crate::engine::core::read::sequence::multi_link_matcher::MultiLinkMatcher;
use crate::engine::core::read::sequence::two_pointer_matcher::TwoPointerMatcher;

/// Factory for creating appropriate matching strategy based on sequence complexity.
pub struct MatcherFactory;

impl MatcherFactory {
    /// Creates a matching strategy appropriate for the sequence.
    ///
    /// - Single link (2 events): Returns TwoPointerMatcher (optimized)
    /// - Multiple links (3+ events): Returns MultiLinkMatcher
    pub fn create_strategy(sequence: EventSequence) -> Box<dyn MatchingStrategy> {
        if sequence.links.len() == 1 {
            Box::new(TwoPointerMatcher::new(sequence))
        } else {
            Box::new(MultiLinkMatcher::new(sequence))
        }
    }
}

