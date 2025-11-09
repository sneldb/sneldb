/// Utility functions for sequence query processing.
use crate::command::types::Expr;
use crate::engine::types::ScalarValue;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};

/// Converts a ScalarValue to a string representation.
pub(crate) fn scalar_to_string(value: &ScalarValue) -> String {
    match value {
        ScalarValue::Utf8(s) => s.clone(),
        ScalarValue::Int64(i) => i.to_string(),
        ScalarValue::Float64(f) => f.to_string(),
        ScalarValue::Boolean(b) => b.to_string(),
        ScalarValue::Timestamp(ts) => ts.to_string(),
        ScalarValue::Binary(bytes) => BASE64_STANDARD.encode(bytes),
        ScalarValue::Null => "null".to_string(),
    }
}

/// Transforms a WHERE clause expression for a specific event type.
///
/// Removes event prefixes from fields (e.g., "purchase.amount" → "amount" for purchase events)
/// and removes conditions that don't apply to this event type.
///
/// Returns None if all conditions are removed (for other event types).
pub(crate) fn transform_where_clause_for_event_type(
    expr: &Expr,
    target_event_type: &str,
) -> Option<Expr> {
    match expr {
        Expr::Compare { field, op, value } => {
            // Check if field has event prefix (e.g., "purchase.amount")
            if let Some((event_type, field_name)) = parse_event_field(field) {
                if event_type == target_event_type {
                    // This condition applies to the target event type - replace with plain field name
                    Some(Expr::Compare {
                        field: field_name,
                        op: op.clone(),
                        value: value.clone(),
                    })
                } else {
                    // This condition applies to a different event type - remove it
                    None
                }
            } else {
                // Common field (no prefix) - keep as-is
                Some(Expr::Compare {
                    field: field.clone(),
                    op: op.clone(),
                    value: value.clone(),
                })
            }
        }
        Expr::In { field, values } => {
            // Check if field has event prefix (e.g., "purchase.amount")
            if let Some((event_type, field_name)) = parse_event_field(field) {
                if event_type == target_event_type {
                    // This condition applies to the target event type - replace with plain field name
                    Some(Expr::In {
                        field: field_name,
                        values: values.clone(),
                    })
                } else {
                    // This condition applies to a different event type - remove it
                    None
                }
            } else {
                // Common field (no prefix) - keep as-is
                Some(Expr::In {
                    field: field.clone(),
                    values: values.clone(),
                })
            }
        }
        Expr::And(left, right) => {
            let left_transformed = transform_where_clause_for_event_type(left, target_event_type);
            let right_transformed = transform_where_clause_for_event_type(right, target_event_type);

            match (left_transformed, right_transformed) {
                (Some(l), Some(r)) => Some(Expr::And(Box::new(l), Box::new(r))),
                (Some(l), None) => Some(l),
                (None, Some(r)) => Some(r),
                (None, None) => None,
            }
        }
        Expr::Or(left, right) => {
            // Transform both sides
            let left_transformed = transform_where_clause_for_event_type(left, target_event_type);
            let right_transformed = transform_where_clause_for_event_type(right, target_event_type);

            match (left_transformed, right_transformed) {
                (Some(l), Some(r)) => Some(Expr::Or(Box::new(l), Box::new(r))),
                (Some(l), None) => Some(l), // If right side removed, keep left (A OR nothing = A)
                (None, Some(r)) => Some(r), // If left side removed, keep right (nothing OR B = B)
                (None, None) => None,       // If both removed, remove the whole OR
            }
        }
        Expr::Not(inner) => transform_where_clause_for_event_type(inner, target_event_type)
            .map(|e| Expr::Not(Box::new(e))),
    }
}

/// Parses an event-prefixed field name.
///
/// Returns `Some((event_type, field_name))` if the field has an event prefix,
/// or `None` if it's a common field.
///
/// # Examples
///
/// - `"purchase.amount"` → `Some(("purchase", "amount"))`
/// - `"page_view.page"` → `Some(("page_view", "page"))`
/// - `"timestamp"` → `None`
pub(crate) fn parse_event_field(field: &str) -> Option<(String, String)> {
    if let Some(dot_pos) = field.find('.') {
        let event_type = field[..dot_pos].to_string();
        let field_name = field[dot_pos + 1..].to_string();
        Some((event_type, field_name))
    } else {
        None
    }
}
