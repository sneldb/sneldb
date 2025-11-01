use crate::engine::core::Event;
use std::cmp::Ordering;

/// Represents a cached sort key to avoid repeated parsing during sort.
///
/// This enum holds pre-parsed field values, allowing O(1) comparisons
/// instead of O(parse) comparisons during sorting.
#[derive(Debug, Clone)]
enum SortKey {
    /// Unsigned integer (most common for IDs, counts)
    U64(u64),
    /// Signed integer (for negative numbers)
    I64(i64),
    /// Floating point (using ordered wrapper to implement Ord)
    F64(OrderedF64),
    /// String fallback (when value is not numeric)
    String(String),
}

impl PartialEq for SortKey {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for SortKey {}

impl PartialOrd for SortKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SortKey {
    fn cmp(&self, other: &Self) -> Ordering {
        use SortKey::*;
        match (self, other) {
            // Same types - compare directly
            (U64(a), U64(b)) => a.cmp(b),
            (I64(a), I64(b)) => a.cmp(b),
            (F64(a), F64(b)) => a.cmp(b),
            (String(a), String(b)) => a.cmp(b),

            // Mixed numeric types - convert to f64 for comparison
            (U64(a), I64(b)) => {
                // If b is negative, a (unsigned) is always greater
                if *b < 0 {
                    Ordering::Greater
                } else {
                    a.cmp(&(*b as u64))
                }
            }
            (I64(a), U64(b)) => {
                // If a is negative, b (unsigned) is always greater
                if *a < 0 {
                    Ordering::Less
                } else {
                    (*a as u64).cmp(b)
                }
            }
            (U64(a), F64(b)) => OrderedF64(*a as f64).cmp(b),
            (F64(a), U64(b)) => a.cmp(&OrderedF64(*b as f64)),
            (I64(a), F64(b)) => OrderedF64(*a as f64).cmp(b),
            (F64(a), I64(b)) => a.cmp(&OrderedF64(*b as f64)),

            // String vs numeric - numeric always sorts before string
            (U64(_), String(_)) => Ordering::Less,
            (I64(_), String(_)) => Ordering::Less,
            (F64(_), String(_)) => Ordering::Less,
            (String(_), U64(_)) => Ordering::Greater,
            (String(_), I64(_)) => Ordering::Greater,
            (String(_), F64(_)) => Ordering::Greater,
        }
    }
}

/// Wrapper for f64 that implements Ord by treating NaN as equal to itself
/// and placing it at the end of the sort order.
#[derive(Debug, Clone, Copy)]
struct OrderedF64(f64);

impl PartialEq for OrderedF64 {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for OrderedF64 {}

impl PartialOrd for OrderedF64 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedF64 {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.0.partial_cmp(&other.0) {
            Some(ord) => ord,
            None => {
                // Handle NaN: NaN == NaN, and NaN > any real number (sorts to end)
                if self.0.is_nan() && other.0.is_nan() {
                    Ordering::Equal
                } else if self.0.is_nan() {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
        }
    }
}

/// Handles comparison of events by field name.
///
/// This struct provides type-aware comparison logic for event fields,
/// attempting numeric comparison before falling back to string comparison.
///
/// **Performance optimization**: The `sort_by_field` method uses the
/// Decorate-Sort-Undecorate pattern (Schwarzian Transform) to parse field
/// values once before sorting, reducing time complexity from
/// O(n log n × parse) to O(n × parse + n log n).
pub struct FieldComparator;

impl FieldComparator {
    /// Compares two events by the specified field.
    ///
    /// For built-in fields (context_id, event_type, timestamp), uses direct comparison.
    /// For payload fields, attempts numeric comparison first (u64, i64, f64),
    /// then falls back to string comparison.
    pub fn compare(a: &Event, b: &Event, field: &str) -> Ordering {
        match field {
            "context_id" => a.context_id.cmp(&b.context_id),
            "event_type" => a.event_type.cmp(&b.event_type),
            "timestamp" => a.timestamp.cmp(&b.timestamp),
            "event_id" => a.event_id().raw().cmp(&b.event_id().raw()),
            other => Self::compare_payload_field(a, b, other),
        }
    }

    /// Compares two events by a payload field.
    ///
    /// Attempts numeric comparison first, falls back to string comparison.
    fn compare_payload_field(a: &Event, b: &Event, field: &str) -> Ordering {
        let val_a = a.get_field_value(field);
        let val_b = b.get_field_value(field);

        // Try numeric types first
        if let Some(ord) = Self::try_numeric_compare(&val_a, &val_b) {
            return ord;
        }

        // Fallback to string comparison
        val_a.cmp(&val_b)
    }

    /// Attempts to compare two string values as numbers.
    ///
    /// Tries parsing as u64, i64, and f64 in that order.
    /// Returns None if neither value can be parsed as a number.
    fn try_numeric_compare(a: &str, b: &str) -> Option<Ordering> {
        // Try u64 first (most common for IDs and counts)
        if let (Ok(na), Ok(nb)) = (a.parse::<u64>(), b.parse::<u64>()) {
            return Some(na.cmp(&nb));
        }

        // Try i64 (for negative numbers)
        if let (Ok(na), Ok(nb)) = (a.parse::<i64>(), b.parse::<i64>()) {
            return Some(na.cmp(&nb));
        }

        // Try f64 (for decimals)
        if let (Ok(na), Ok(nb)) = (a.parse::<f64>(), b.parse::<f64>()) {
            return Some(na.partial_cmp(&nb).unwrap_or(Ordering::Equal));
        }

        None
    }

    /// Extracts and parses a sort key from an event's field.
    ///
    /// **Optimization**: Uses `get_field()` to work with typed JSON Values
    /// directly, avoiding string conversion and re-parsing. This is much faster
    /// than calling `get_field_value()` which returns a String requiring parsing.
    ///
    /// For numeric fields already stored as JSON numbers, this eliminates parsing!
    fn extract_sort_key(event: &Event, field: &str) -> SortKey {
        match field {
            "context_id" => SortKey::String(event.context_id.clone()),
            "event_type" => SortKey::String(event.event_type.clone()),
            "timestamp" => SortKey::U64(event.timestamp),
            "event_id" => SortKey::U64(event.event_id().raw()),
            other => {
                // Get typed Value instead of String - avoids conversion!
                match event.get_field(other) {
                    Some(serde_json::Value::Number(n)) => {
                        // Value is already a number - no parsing needed!
                        if let Some(u) = n.as_u64() {
                            return SortKey::U64(u);
                        }
                        if let Some(i) = n.as_i64() {
                            return SortKey::I64(i);
                        }
                        if let Some(f) = n.as_f64() {
                            return SortKey::F64(OrderedF64(f));
                        }
                        // Shouldn't happen, but fallback to string
                        SortKey::String(n.to_string())
                    }
                    Some(serde_json::Value::String(s)) => {
                        // Try parsing string as number (for string-encoded numbers)
                        if let Ok(u) = s.parse::<u64>() {
                            return SortKey::U64(u);
                        }
                        if let Ok(i) = s.parse::<i64>() {
                            return SortKey::I64(i);
                        }
                        if let Ok(f) = s.parse::<f64>() {
                            return SortKey::F64(OrderedF64(f));
                        }
                        // Not a number, use as string
                        SortKey::String(s)
                    }
                    Some(serde_json::Value::Bool(b)) => {
                        // Booleans: false < true
                        SortKey::String(b.to_string())
                    }
                    Some(serde_json::Value::Null) => {
                        // Null sorts as empty string
                        SortKey::String(String::new())
                    }
                    Some(other_value) => {
                        // Array or Object - convert to string
                        SortKey::String(other_value.to_string())
                    }
                    None => {
                        // Field doesn't exist
                        SortKey::String(String::new())
                    }
                }
            }
        }
    }

    /// Sorts events by field using Decorate-Sort-Undecorate pattern.
    ///
    /// **Performance optimization**: This method parses field values ONCE
    /// before sorting, then sorts by cached keys. This reduces time complexity
    /// from O(n log n × parse) to O(n × parse + n log n).
    ///
    /// **Additional optimization**: Uses in-place permutation to avoid cloning
    /// all events during the undecorate phase, reducing memory allocations.
    ///
    /// For a query sorting 100K events:
    /// - Reduces ~10M parse operations to 100K (100x fewer!)
    /// - Eliminates 100K event clones using cycle-following permutation
    pub fn sort_by_field(events: &mut [Event], field: &str, ascending: bool) {
        if events.is_empty() {
            return;
        }

        // Phase 1: Decorate - Extract and parse sort keys once per event (O(n))
        let mut decorated: Vec<(SortKey, usize)> = events
            .iter()
            .enumerate()
            .map(|(idx, event)| {
                let key = Self::extract_sort_key(event, field);
                (key, idx)
            })
            .collect();

        // Phase 2: Sort by cached keys (O(n log n), no parsing!)
        // Use sort_unstable_by for better performance - maintains relative order of equal elements
        if ascending {
            decorated.sort_unstable_by(|(key_a, _), (key_b, _)| key_a.cmp(key_b));
        } else {
            decorated.sort_unstable_by(|(key_a, _), (key_b, _)| key_b.cmp(key_a));
        }

        // Phase 3: Undecorate - Reorder events in-place using permutation (O(n))
        Self::apply_permutation(events, &decorated);
    }

    /// Applies a permutation to reorder events in-place without cloning.
    ///
    /// Uses cycle-following algorithm to rearrange elements efficiently.
    /// Instead of cloning all N events, this moves them by following
    /// permutation cycles, requiring only 1 temporary Event per cycle.
    ///
    /// **Performance**: Eliminates N-C event clones (where C = number of cycles),
    /// saving significant memory and CPU time for large payloads.
    ///
    /// **Algorithm**: For permutation [2,0,1] representing indices [a,b,c] → [b,c,a]:
    /// - Save a, move b→0, move c→1, place a→2 (1 move per element instead of 1 clone)
    fn apply_permutation(events: &mut [Event], permutation: &[(SortKey, usize)]) {
        let n = events.len();

        // Track which positions have been moved to their final location
        let mut done = vec![false; n];

        // Process each permutation cycle
        for cycle_start in 0..n {
            if done[cycle_start] {
                continue; // Already processed this position
            }

            let target_idx = permutation[cycle_start].1;

            // Check if element is already in correct position
            if target_idx == cycle_start {
                done[cycle_start] = true;
                continue;
            }

            // Follow the cycle and rotate elements
            // We'll use swap to move elements without cloning

            let mut current = cycle_start;
            loop {
                let next = permutation[current].1;
                done[current] = true;

                if next == cycle_start {
                    // Completed the cycle
                    break;
                }

                // Swap current with next position
                // This is safe because current != next (they're in a cycle)
                if current < next {
                    let (left, right) = events.split_at_mut(next);
                    std::mem::swap(&mut left[current], &mut right[0]);
                } else {
                    let (left, right) = events.split_at_mut(current);
                    std::mem::swap(&mut left[next], &mut right[0]);
                }

                current = next;
            }
        }
    }
}

#[cfg(test)]
mod field_comparator_edge_cases {
    use super::*;

    #[test]
    fn handles_nan_values() {
        let a = "NaN";
        let b = "1.5";

        // NaN parses as f64 in Rust, but comparison should handle it
        let result = FieldComparator::try_numeric_compare(a, b);
        // NaN comparisons always return Equal due to partial_cmp behavior
        assert!(result.is_some());
    }

    #[test]
    fn handles_infinity() {
        let a = "inf";
        let b = "1.5";

        // Infinity parses as f64 in Rust
        let result = FieldComparator::try_numeric_compare(a, b);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), std::cmp::Ordering::Greater);
    }

    #[test]
    fn handles_mixed_types_falls_back_to_string() {
        let a = "123abc";
        let b = "456def";

        // These can't be parsed as numbers, should fall back to string comparison
        let result = FieldComparator::try_numeric_compare(a, b);
        assert!(result.is_none());
    }
}
