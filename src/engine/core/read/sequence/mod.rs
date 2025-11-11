/// Sequence matching module for FOLLOWED BY, PRECEDED BY, and LINKED BY operators.
///
/// This module implements efficient sequence matching using columnar data processing
/// to avoid premature materialization of events.
///
/// Note: Sequence queries are only available via the streaming execution path.
pub mod group;
pub mod matcher;
pub mod matcher_factory;
pub mod matching_strategy;
pub mod materializer;
pub mod multi_link_matcher;
pub mod two_pointer_matcher;
pub mod utils;
pub mod where_evaluator;

pub use group::ColumnarGrouper;
pub use matcher::SequenceMatcher;
pub use matching_strategy::MatchingStrategy;
pub use materializer::SequenceMaterializer;
pub use where_evaluator::SequenceWhereEvaluator;

#[cfg(test)]
mod group_test;
#[cfg(test)]
mod matcher_test;
#[cfg(test)]
mod materializer_test;
#[cfg(test)]
mod utils_test;
#[cfg(test)]
mod where_evaluator_test;
