# Multi-Link Sequence Support: Implementation Analysis

## Current State

### What Already Works ✅

1. **Parser**: Fully supports multiple FOLLOWED BY clauses
   - Syntax: `QUERY A FOLLOWED BY B FOLLOWED BY C LINKED BY field`
   - Parses into `EventSequence` with multiple links in `links` Vec

2. **WHERE Evaluator**: Already supports multiple event types
   - `SequenceWhereEvaluator` builds evaluators for each event type
   - Can handle event-prefixed fields (e.g., `page_view.page`, `order_created.status`)
   - Validates field ambiguity across all event types

3. **Materializer**: Already supports multiple events
   - `SequenceMaterializer` iterates over `matched_rows` Vec
   - Can materialize any number of events in a sequence

4. **Grouping**: Already supports multiple event types
   - `ColumnarGrouper` groups rows by link field across all event types
   - Stores rows in `HashMap<String, Vec<RowIndex>>` by event type

### What's Missing ❌

1. **Matcher**: Only handles single link (2 event types)
   - `match_in_group()` checks `if self.sequence.links.len() == 1`
   - Returns empty Vec for multiple links (line 297-307)
   - Two-pointer algorithm only works for 2 event types

2. **Group Validation**: Only checks event_type_a and event_type_b
   - Lines 175-204 only check for presence of A and B
   - Needs to check all event types in sequence

3. **WHERE Clause Evaluation**: Hardcoded for 2 events
   - `matches_where_clause()` takes exactly 2 event types (lines 612-706)
   - Needs to evaluate WHERE clause for N events

## Required Changes

### 1. Matcher: Multi-Link Matching Algorithm

**File**: `src/engine/core/read/sequence/matcher.rs`

**Current**: Two-pointer algorithm for `A FOLLOWED BY B`
- O(n+m) complexity
- Works with sorted indices

**Needed**: Recursive/iterative matching for `A FOLLOWED BY B FOLLOWED BY C ...`
- Options:
  - **Option A**: Recursive matching (cleaner, easier to understand)
  - **Option B**: Iterative with N pointers (more complex, potentially faster)
  - **Option C**: Dynamic programming approach (most complex, handles all cases)

**Recommended**: Option A (Recursive matching)

**Algorithm**:
```rust
fn match_multi_link(
    &self,
    group: &GroupedRowIndices,
    zones_by_event_type: &HashMap<String, Vec<CandidateZone>>,
    event_types: &[String],
    link_types: &[SequenceLink],
    current_matches: Vec<(String, RowIndex)>,
    start_idx: usize,
) -> Vec<MatchedSequenceIndices>
```

**Complexity**:
- For N event types: O(n₁ × n₂ × ... × nₙ) worst case
- Can be optimized with early pruning and WHERE clause filtering

**Estimated LOC**: ~200-300 lines

### 2. Group Validation: Check All Event Types

**File**: `src/engine/core/read/sequence/matcher.rs`

**Current** (lines 147-204):
```rust
let event_type_a = &self.sequence.head.event;
let event_type_b = if let Some((_, target)) = self.sequence.links.first() {
    &target.event
} else {
    return all_matches;
};

// Only checks for A and B
if a_indices.is_none() || a_indices.unwrap().is_empty() {
    continue;
}
if b_indices.is_none() || b_indices.unwrap().is_empty() {
    continue;
}
```

**Needed**: Check all event types in sequence
```rust
// Extract all event types from sequence
let all_event_types = self.extract_event_types();

// Check if group has events of all types
for event_type in &all_event_types {
    if group.rows_by_type.get(event_type).map(|v| v.is_empty()).unwrap_or(true) {
        continue; // Skip group if missing any event type
    }
}
```

**Estimated LOC**: ~30-50 lines

### 3. WHERE Clause Evaluation: Support N Events

**File**: `src/engine/core/read/sequence/matcher.rs`

**Current** (lines 612-706):
```rust
fn matches_where_clause(
    &self,
    event_type_a: &str,
    zones_a: &[CandidateZone],
    row_a: &RowIndex,
    event_type_b: &str,
    zones_b: &[CandidateZone],
    row_b: &RowIndex,
) -> bool
```

**Needed**: Evaluate WHERE clause for N events
```rust
fn matches_where_clause_multi(
    &self,
    matched_rows: &[(String, RowIndex)],
    zones_by_event_type: &HashMap<String, Vec<CandidateZone>>,
) -> bool {
    if let Some(ref evaluator) = self.where_evaluator {
        for (event_type, row_index) in matched_rows {
            let zones = zones_by_event_type.get(event_type)?;
            let zone = &zones[row_index.zone_idx];
            if !evaluator.evaluate_row(event_type, zone, row_index) {
                return false;
            }
        }
    }
    true
}
```

**Estimated LOC**: ~50-80 lines

### 4. Extract Event Types Helper

**File**: `src/engine/core/read/sequence/matcher.rs`

**Needed**: Helper to extract all event types from sequence
```rust
fn extract_event_types(&self) -> Vec<String> {
    let mut event_types = vec![self.sequence.head.event.clone()];
    for (_, target) in &self.sequence.links {
        event_types.push(target.event.clone());
    }
    event_types
}
```

**Estimated LOC**: ~10 lines

## Implementation Plan

### Phase 1: Core Multi-Link Matching (2-3 weeks)

1. **Add helper methods** (~1 day)
   - `extract_event_types()` - Get all event types from sequence
   - `matches_where_clause_multi()` - Evaluate WHERE for N events
   - Update group validation to check all event types

2. **Implement recursive matching** (~1-2 weeks)
   - `match_multi_link_recursive()` - Recursive matching algorithm
   - Handle FOLLOWED BY and PRECEDED BY for each link
   - Maintain temporal ordering across all links
   - Early pruning with WHERE clause filtering

3. **Update match_in_group()** (~1 day)
   - Remove single-link check
   - Call recursive matcher for multi-link sequences
   - Fall back to existing two-pointer for single link (optimization)

4. **Testing** (~3-5 days)
   - Unit tests for 3-event sequences
   - Unit tests for 4+ event sequences
   - Integration tests with WHERE clauses
   - Performance tests

### Phase 2: Optimizations (1-2 weeks)

1. **Early pruning** (~3-5 days)
   - Skip groups missing any event type early
   - Apply WHERE clause filtering before recursive matching
   - Limit propagation through recursion

2. **Performance tuning** (~3-5 days)
   - Profile multi-link matching
   - Optimize hot paths
   - Consider memoization for repeated sub-problems

3. **Edge cases** (~2-3 days)
   - Handle empty groups gracefully
   - Handle sequences with same event type multiple times
   - Handle mixed FOLLOWED BY and PRECEDED BY

### Phase 3: Polish and Documentation (1 week)

1. **Documentation** (~2-3 days)
   - Update sequence query docs
   - Add examples for multi-link sequences
   - Document performance characteristics

2. **Error messages** (~1-2 days)
   - Better error messages for unsupported cases
   - Clear error messages for WHERE clause issues

3. **Final testing** (~2-3 days)
   - Comprehensive test suite
   - Performance benchmarks
   - Edge case coverage

## Code Changes Summary

### Files to Modify

1. **`src/engine/core/read/sequence/matcher.rs`** (Major changes)
   - Add `extract_event_types()` helper
   - Add `matches_where_clause_multi()` method
   - Add `match_multi_link_recursive()` method
   - Update `match_in_group()` to handle multiple links
   - Update group validation logic
   - **Estimated**: +300-400 LOC, -20 LOC (remove single-link check)

2. **`src/engine/core/read/sequence/matcher_test.rs`** (New tests)
   - Add tests for 3-event sequences
   - Add tests for 4+ event sequences
   - Add tests for mixed FOLLOWED BY/PRECEDED BY
   - Add performance tests
   - **Estimated**: +200-300 LOC

### Files That Don't Need Changes ✅

1. **`src/command/parser/commands/query.rs`** - Already supports syntax
2. **`src/engine/core/read/sequence/where_evaluator.rs`** - Already supports multiple event types
3. **`src/engine/core/read/sequence/materializer.rs`** - Already supports multiple events
4. **`src/engine/core/read/sequence/group.rs`** - Already supports multiple event types

## Complexity Analysis

### Time Complexity

- **Current (2 events)**: O(n + m) with two-pointer algorithm
- **Multi-link (N events)**: O(n₁ × n₂ × ... × nₙ) worst case
  - Can be optimized to O(n₁ + n₂ + ... + nₙ) with early pruning
  - WHERE clause filtering reduces search space significantly

### Space Complexity

- **Current**: O(1) extra space (just pointers)
- **Multi-link**: O(N) for recursion stack, O(M) for matched sequences
  - M = number of matched sequences
  - N = number of event types

## Risk Assessment

### Low Risk ✅
- WHERE evaluator already supports multiple event types
- Materializer already supports multiple events
- Parser already supports syntax

### Medium Risk ⚠️
- Performance degradation with many event types
- Recursive algorithm complexity
- Memory usage with large result sets

### Mitigation Strategies
1. **Early pruning**: Skip groups missing any event type
2. **WHERE clause filtering**: Apply filters before recursive matching
3. **Limit propagation**: Pass limit through recursion to stop early
4. **Iterative approach**: Consider iterative algorithm if recursion is too deep

## Estimated Effort

- **Phase 1 (Core Implementation)**: 2-3 weeks
- **Phase 2 (Optimizations)**: 1-2 weeks
- **Phase 3 (Polish)**: 1 week

**Total**: **4-6 weeks** for full multi-link sequence support

## Alternative: Incremental Approach

Instead of full recursive matching, could implement incrementally:

1. **Week 1**: Support exactly 3 event types (A FOLLOWED BY B FOLLOWED BY C)
   - Use nested two-pointer approach
   - Simpler than full recursion
   - Validates approach

2. **Week 2-3**: Generalize to N event types
   - Refactor to recursive approach
   - Reuse 3-event logic

3. **Week 4**: Optimizations and polish

This reduces risk and allows earlier validation.

## Conclusion

**Good news**: Most infrastructure is already in place! The parser, WHERE evaluator, and materializer all support multiple event types. The main work is implementing the multi-link matching algorithm in the matcher.

**Main challenge**: Converting the two-pointer algorithm (optimized for 2 events) to a recursive/iterative algorithm that handles N events while maintaining performance.

**Estimated effort**: 4-6 weeks for full implementation, or 3-4 weeks for incremental approach starting with 3-event support.

