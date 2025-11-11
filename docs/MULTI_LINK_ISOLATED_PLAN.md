# Multi-Link Sequence Support: Isolated Implementation Plan

## Design Principles

Following clean code principles and existing codebase patterns:
- **Open/Closed Principle**: Extend without modifying existing code
- **Single Responsibility**: Each matcher handles one matching strategy
- **Strategy Pattern**: Use trait-based polymorphism (like `QueryPlanner`, `BatchDispatch`)
- **Composition**: Delegate to appropriate strategy based on sequence complexity
- **Isolation**: New code in separate modules, minimal changes to existing code

## Architecture Overview

```
SequenceMatcher (existing, minimal changes)
    ↓ delegates to
MatchingStrategy trait
    ↓ implemented by
├── TwoPointerMatcher (existing logic, extracted)
└── MultiLinkMatcher (new, isolated)
```

## File Structure

### New Files (Isolated)

```
src/engine/core/read/sequence/
├── matcher.rs                    # Existing - minimal changes
├── matching_strategy.rs          # NEW - Trait definition
├── two_pointer_matcher.rs        # NEW - Extracted existing logic
├── multi_link_matcher.rs         # NEW - New multi-link implementation
└── matcher_factory.rs            # NEW - Factory to select strategy
```

### Modified Files (Minimal Changes)

```
src/engine/core/read/sequence/
├── mod.rs                        # Export new modules
└── matcher.rs                    # Delegate to strategy (10-20 LOC changes)
```

## Implementation Plan

### Phase 1: Extract Existing Logic (Isolation) - 3-5 days

**Goal**: Extract two-pointer logic into separate module without changing behavior.

#### Step 1.1: Create MatchingStrategy Trait

**File**: `src/engine/core/read/sequence/matching_strategy.rs` (NEW)

```rust
use crate::engine::core::CandidateZone;
use crate::engine::core::read::sequence::group::GroupedRowIndices;
use crate::engine::core::read::sequence::matcher::MatchedSequenceIndices;
use crate::engine::core::read::sequence::where_evaluator::SequenceWhereEvaluator;
use std::collections::HashMap;

/// Trait for sequence matching strategies.
///
/// Different strategies handle different sequence complexities:
/// - TwoPointerMatcher: Optimized for 2-event sequences (single link)
/// - MultiLinkMatcher: Handles N-event sequences (multiple links)
pub trait MatchingStrategy: Send + Sync {
    /// Matches sequences within a single group.
    ///
    /// # Arguments
    /// * `group` - Row indices grouped by link field value
    /// * `zones_by_event_type` - Zones containing columnar data for each event type
    /// * `where_evaluator` - Optional WHERE clause evaluator
    /// * `time_field` - Field name for time-based comparisons
    ///
    /// # Returns
    /// Vector of matched sequence indices
    fn match_in_group(
        &self,
        group: &GroupedRowIndices,
        zones_by_event_type: &HashMap<String, Vec<CandidateZone>>,
        where_evaluator: Option<&SequenceWhereEvaluator>,
        time_field: &str,
    ) -> Vec<MatchedSequenceIndices>;

    /// Validates that a group has all required event types.
    ///
    /// Returns true if group can potentially produce matches, false otherwise.
    fn validate_group(
        &self,
        group: &GroupedRowIndices,
    ) -> bool;
}
```

**Estimated LOC**: ~50 lines

#### Step 1.2: Extract TwoPointerMatcher

**File**: `src/engine/core/read/sequence/two_pointer_matcher.rs` (NEW)

**Action**: Move existing `match_followed_by()` and `match_preceded_by()` logic here.

**Structure**:
```rust
use crate::command::types::{EventSequence, SequenceLink};
use crate::engine::core::read::sequence::matching_strategy::MatchingStrategy;
// ... other imports

/// Matches sequences using two-pointer technique (optimized for 2-event sequences).
///
/// This is the existing optimized algorithm extracted from SequenceMatcher.
pub struct TwoPointerMatcher {
    sequence: EventSequence,
}

impl TwoPointerMatcher {
    pub fn new(sequence: EventSequence) -> Self {
        Self { sequence }
    }

    // Move match_followed_by() here
    // Move match_preceded_by() here
    // Move matches_where_clause() here (for 2 events)
    // Move get_timestamp() here
}

impl MatchingStrategy for TwoPointerMatcher {
    fn match_in_group(...) -> Vec<MatchedSequenceIndices> {
        // Delegate to match_followed_by() or match_preceded_by()
    }

    fn validate_group(...) -> bool {
        // Check for event_type_a and event_type_b
    }
}
```

**Changes**:
- Copy `match_followed_by()` from `matcher.rs` (lines 315-463)
- Copy `match_preceded_by()` from `matcher.rs` (lines 469-606)
- Copy `matches_where_clause()` from `matcher.rs` (lines 612-706)
- Copy `get_timestamp()` from `matcher.rs` (lines 712-743)
- Adapt to work as standalone struct

**Estimated LOC**: ~400 lines (mostly moved, not new)

#### Step 1.3: Create MatcherFactory

**File**: `src/engine/core/read/sequence/matcher_factory.rs` (NEW)

```rust
use crate::command::types::EventSequence;
use crate::engine::core::read::sequence::matching_strategy::MatchingStrategy;
use crate::engine::core::read::sequence::two_pointer_matcher::TwoPointerMatcher;
use crate::engine::core::read::sequence::multi_link_matcher::MultiLinkMatcher;

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
```

**Estimated LOC**: ~30 lines

#### Step 1.4: Update SequenceMatcher (Minimal Changes)

**File**: `src/engine/core/read/sequence/matcher.rs` (MODIFY)

**Changes**:
1. Add field for strategy:
```rust
pub struct SequenceMatcher {
    sequence: EventSequence,
    where_evaluator: Option<SequenceWhereEvaluator>,
    time_field: String,
    strategy: Box<dyn MatchingStrategy>,  // NEW
}
```

2. Update constructor:
```rust
pub fn new(sequence: EventSequence, time_field: String) -> Self {
    let strategy = MatcherFactory::create_strategy(sequence.clone());
    Self {
        sequence,
        where_evaluator: None,
        time_field,
        strategy,
    }
}
```

3. Update `match_in_group()` to delegate:
```rust
fn match_in_group(
    &self,
    group: &GroupedRowIndices,
    zones_by_event_type: &HashMap<String, Vec<CandidateZone>>,
) -> Vec<MatchedSequenceIndices> {
    // Validate group
    if !self.strategy.validate_group(group) {
        return Vec::new();
    }

    // Delegate to strategy
    self.strategy.match_in_group(
        group,
        zones_by_event_type,
        self.where_evaluator.as_ref(),
        &self.time_field,
    )
}
```

4. Update group validation in `match_sequences()`:
```rust
// Replace lines 147-204 with:
if !self.strategy.validate_group(&group) {
    continue;
}
```

**Estimated Changes**: ~30-40 LOC modifications

#### Step 1.5: Update Module Exports

**File**: `src/engine/core/read/sequence/mod.rs` (MODIFY)

```rust
pub mod group;
pub mod matcher;
pub mod matching_strategy;      // NEW
pub mod two_pointer_matcher;    // NEW
pub mod matcher_factory;         // NEW
pub mod materializer;
pub mod utils;
pub mod where_evaluator;

pub use group::ColumnarGrouper;
pub use matcher::SequenceMatcher;
pub use matching_strategy::MatchingStrategy;  // NEW
pub use materializer::SequenceMaterializer;
pub use where_evaluator::SequenceWhereEvaluator;
```

**Estimated Changes**: ~5 LOC

#### Step 1.6: Tests for Extracted Logic

**File**: `src/engine/core/read/sequence/two_pointer_matcher_test.rs` (NEW)

- Copy existing tests from `matcher_test.rs`
- Adapt to test `TwoPointerMatcher` directly
- Ensure behavior unchanged

**Estimated LOC**: ~200 lines (mostly moved)

**Phase 1 Total**: ~715 LOC (mostly moved/extracted, ~100 LOC new)

---

### Phase 2: Implement MultiLinkMatcher (Isolated) - 1-2 weeks

**Goal**: Implement multi-link matching in completely isolated module.

#### Step 2.1: Create MultiLinkMatcher Structure

**File**: `src/engine/core/read/sequence/multi_link_matcher.rs` (NEW)

```rust
use crate::command::types::{EventSequence, SequenceLink};
use crate::engine::core::CandidateZone;
use crate::engine::core::read::sequence::group::{GroupedRowIndices, RowIndex};
use crate::engine::core::read::sequence::matching_strategy::MatchingStrategy;
use crate::engine::core::read::sequence::matcher::MatchedSequenceIndices;
use crate::engine::core::read::sequence::where_evaluator::SequenceWhereEvaluator;
use crate::engine::core::filter::condition::PreparedAccessor;
use std::collections::HashMap;

/// Matches sequences with multiple links using recursive algorithm.
///
/// Handles sequences like: A FOLLOWED BY B FOLLOWED BY C LINKED BY field
pub struct MultiLinkMatcher {
    sequence: EventSequence,
    event_types: Vec<String>,  // Cached for performance
}

impl MultiLinkMatcher {
    pub fn new(sequence: EventSequence) -> Self {
        let event_types = Self::extract_event_types(&sequence);
        Self {
            sequence,
            event_types,
        }
    }

    /// Extracts all event types from sequence.
    fn extract_event_types(sequence: &EventSequence) -> Vec<String> {
        let mut types = vec![sequence.head.event.clone()];
        for (_, target) in &sequence.links {
            types.push(target.event.clone());
        }
        types
    }

    /// Recursive matching algorithm.
    fn match_recursive(
        &self,
        group: &GroupedRowIndices,
        zones_by_event_type: &HashMap<String, Vec<CandidateZone>>,
        where_evaluator: Option<&SequenceWhereEvaluator>,
        time_field: &str,
        current_matches: Vec<(String, RowIndex)>,
        link_idx: usize,
    ) -> Vec<MatchedSequenceIndices> {
        // Implementation here
    }

    /// Evaluates WHERE clause for multiple events.
    fn matches_where_clause_multi(
        &self,
        matched_rows: &[(String, RowIndex)],
        zones_by_event_type: &HashMap<String, Vec<CandidateZone>>,
        where_evaluator: Option<&SequenceWhereEvaluator>,
    ) -> bool {
        // Implementation here
    }

    /// Gets timestamp for a row index.
    fn get_timestamp(
        &self,
        zones: &[CandidateZone],
        row_index: &RowIndex,
        time_field: &str,
    ) -> u64 {
        // Implementation here
    }
}

impl MatchingStrategy for MultiLinkMatcher {
    fn match_in_group(...) -> Vec<MatchedSequenceIndices> {
        self.match_recursive(
            group,
            zones_by_event_type,
            where_evaluator,
            time_field,
            vec![],  // Start with empty matches
            0,       // Start with first link
        )
    }

    fn validate_group(&self, group: &GroupedRowIndices) -> bool {
        // Check all event types are present
        for event_type in &self.event_types {
            if group.rows_by_type.get(event_type)
                .map(|v| v.is_empty())
                .unwrap_or(true) {
                return false;
            }
        }
        true
    }
}
```

**Estimated LOC**: ~300-400 lines

#### Step 2.2: Implement Recursive Matching Logic

**Focus**: Keep cyclomatic complexity low by:
- Extracting helper methods
- Using early returns
- Separating concerns (temporal ordering, WHERE evaluation, etc.)

**Key Methods**:
1. `match_recursive()` - Main recursive logic (~150 LOC)
2. `match_next_link()` - Handle single link step (~80 LOC)
3. `matches_where_clause_multi()` - WHERE evaluation (~50 LOC)
4. `get_timestamp()` - Helper (~20 LOC)
5. `extract_event_types()` - Helper (~10 LOC)

**Estimated LOC**: ~310 lines

#### Step 2.3: Tests for MultiLinkMatcher

**File**: `src/engine/core/read/sequence/multi_link_matcher_test.rs` (NEW)

- Test 3-event sequences
- Test 4+ event sequences
- Test mixed FOLLOWED BY/PRECEDED BY
- Test WHERE clause filtering
- Test edge cases

**Estimated LOC**: ~200-300 lines

**Phase 2 Total**: ~600-700 LOC (all new, isolated)

---

### Phase 3: Integration & Optimization - 1 week

#### Step 3.1: Integration Testing

- Test that existing 2-event queries still work
- Test new multi-link queries
- Performance benchmarks

#### Step 3.2: Optimizations

- Early pruning in `validate_group()`
- Limit propagation through recursion
- WHERE clause filtering before recursion

#### Step 3.3: Documentation

- Update sequence query docs
- Add examples for multi-link sequences

**Phase 3 Total**: ~100 LOC (mostly tests/docs)

---

## Summary: Change Isolation

### Files Modified (Minimal Changes)

| File | Changes | LOC Changed |
|------|--------|-------------|
| `matcher.rs` | Delegate to strategy | ~40 LOC |
| `mod.rs` | Export new modules | ~5 LOC |
| **Total Modified** | | **~45 LOC** |

### Files Created (Isolated)

| File | Purpose | LOC |
|------|---------|-----|
| `matching_strategy.rs` | Trait definition | ~50 |
| `two_pointer_matcher.rs` | Extracted existing logic | ~400 |
| `multi_link_matcher.rs` | New multi-link implementation | ~400 |
| `matcher_factory.rs` | Strategy selection | ~30 |
| `two_pointer_matcher_test.rs` | Tests for extracted logic | ~200 |
| `multi_link_matcher_test.rs` | Tests for new logic | ~300 |
| **Total New** | | **~1,380 LOC** |

### Key Benefits

1. **Zero Risk to Existing Code**: Two-pointer logic extracted, not modified
2. **Clear Separation**: Each matcher in own module
3. **Easy Testing**: Each strategy testable independently
4. **Easy Rollback**: Can disable multi-link by changing factory
5. **Performance**: Two-pointer still optimized path for 2-event sequences
6. **Maintainability**: Low cyclomatic complexity per module
7. **Extensibility**: Easy to add new strategies (e.g., optimized 3-event matcher)

### Testing Strategy

1. **Phase 1**: Ensure extracted two-pointer matcher behaves identically
2. **Phase 2**: Test multi-link matcher in isolation
3. **Phase 3**: Integration tests ensure both work together

### Rollback Plan

If issues arise:
1. Change `MatcherFactory::create_strategy()` to always return `TwoPointerMatcher`
2. Multi-link matcher code remains but unused
3. No changes needed to existing code paths

## Timeline

- **Phase 1** (Extraction): 3-5 days
- **Phase 2** (Multi-link): 1-2 weeks
- **Phase 3** (Integration): 1 week

**Total**: 2.5-3.5 weeks

This approach follows OO principles, minimizes risk, and keeps changes isolated.

