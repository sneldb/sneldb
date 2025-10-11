use crate::engine::core::Event;
use std::cmp::Ordering;

/// Handles comparison of events by field name.
///
/// This struct provides type-aware comparison logic for event fields,
/// attempting numeric comparison before falling back to string comparison.
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

    /// Helper to sort a slice of events by a field in place.
    pub fn sort_by_field(events: &mut [Event], field: &str, ascending: bool) {
        events.sort_by(|a, b| {
            let ord = Self::compare(a, b, field);
            if ascending { ord } else { ord.reverse() }
        });
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
