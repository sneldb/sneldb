use crate::engine::types::ScalarValue;
use std::cmp::Ordering;

/// Handles comparison of row values by field name.
///
/// This is similar to FieldComparator but works with serialized row data
/// (Vec<ScalarValue>) instead of Event structs. Used for k-way merge operations.
pub struct RowComparator;

impl RowComparator {
    /// Compares two rows by the specified field.
    ///
    /// Row format: [context_id, event_type, timestamp, payload]
    ///
    /// For built-in fields (context_id, event_type, timestamp), uses direct comparison.
    /// For payload fields, attempts numeric comparison first (u64, i64, f64),
    /// then falls back to string comparison.
    pub fn compare(a: &[ScalarValue], b: &[ScalarValue], field: &str) -> Ordering {
        match field {
            "context_id" => Self::compare_string_value(&a[0], &b[0]),
            "event_type" => Self::compare_string_value(&a[1], &b[1]),
            "timestamp" => Self::compare_numeric_value(&a[2], &b[2]),
            other => Self::compare_payload_field(a, b, other),
        }
    }

    /// Compares two ScalarValues as strings.
    fn compare_string_value(a: &ScalarValue, b: &ScalarValue) -> Ordering {
        let sa = a
            .as_str()
            .map(|s| s.to_string())
            .unwrap_or_else(|| a.to_json().to_string());
        let sb = b
            .as_str()
            .map(|s| s.to_string())
            .unwrap_or_else(|| b.to_json().to_string());
        sa.cmp(&sb)
    }

    /// Compares two ScalarValues as numbers, falling back to string.
    fn compare_numeric_value(a: &ScalarValue, b: &ScalarValue) -> Ordering {
        // Try u64 first (most common)
        if let (Some(na), Some(nb)) = (a.as_u64(), b.as_u64()) {
            return na.cmp(&nb);
        }

        // Try i64 (for negative numbers)
        if let (Some(na), Some(nb)) = (a.as_i64(), b.as_i64()) {
            return na.cmp(&nb);
        }

        // Try f64 (for decimals)
        if let (Some(na), Some(nb)) = (a.as_f64(), b.as_f64()) {
            return na.partial_cmp(&nb).unwrap_or(Ordering::Equal);
        }

        // Fallback to string comparison
        let sa = a.to_json().to_string();
        let sb = b.to_json().to_string();
        sa.cmp(&sb)
    }

    /// Compares payload fields from two rows.
    /// Note: Payload is now a Utf8 string (serialized JSON), so we parse it to compare.
    fn compare_payload_field(a: &[ScalarValue], b: &[ScalarValue], field: &str) -> Ordering {
        // Payload is at index 3, now a Utf8 string
        let va = a.get(3).and_then(|v| {
            if let ScalarValue::Utf8(s) = v {
                serde_json::from_str::<serde_json::Value>(s)
                    .ok()
                    .and_then(|json| {
                        json.as_object().and_then(|o| {
                            o.get(field).map(|v| {
                                // Clone the value to avoid lifetime issues
                                ScalarValue::from(v.clone())
                            })
                        })
                    })
            } else {
                None
            }
        });
        let vb = b.get(3).and_then(|v| {
            if let ScalarValue::Utf8(s) = v {
                serde_json::from_str::<serde_json::Value>(s)
                    .ok()
                    .and_then(|json| {
                        json.as_object().and_then(|o| {
                            o.get(field).map(|v| {
                                // Clone the value to avoid lifetime issues
                                ScalarValue::from(v.clone())
                            })
                        })
                    })
            } else {
                None
            }
        });

        match (va, vb) {
            (Some(a_val), Some(b_val)) => Self::compare_numeric_value(&a_val, &b_val),
            (Some(_), None) => Ordering::Greater, // has value > no value
            (None, Some(_)) => Ordering::Less,    // no value < has value
            (None, None) => Ordering::Equal,      // both missing
        }
    }

    /// Helper to sort rows by a field in place.
    pub fn sort_by_field(rows: &mut [Vec<ScalarValue>], field: &str, ascending: bool) {
        // Use sort_unstable_by for better performance - maintains relative order of equal elements
        rows.sort_unstable_by(|a, b| {
            let ord = Self::compare(a, b, field);
            if ascending { ord } else { ord.reverse() }
        });
    }
}
