use crate::engine::core::filter::filter_group::FilterGroup;
use crate::engine::types::ScalarValue;
use serde_json::Value as JsonValue;
use tracing::debug;

/// Handles expansion of IN expressions and OR-of-equality patterns
/// into OR of equality filters for efficient zone collection.
///
/// **Single Responsibility**: Expand IN/OR patterns to enable efficient zone indexing.
pub struct InExpansion;

impl InExpansion {
    /// Expands IN expression to FilterGroup
    /// Returns None for empty IN lists (matches nothing)
    pub fn expand_in(
        field: String,
        values: &[JsonValue],
        event_type_uid: &Option<String>,
    ) -> Option<FilterGroup> {
        if values.is_empty() {
            return None; // Empty IN list matches nothing
        }

        // Single value: create a single equality filter
        if values.len() == 1 {
            return Some(FilterGroup::new_equality_filter(
                field,
                ScalarValue::from(values[0].clone()),
                event_type_uid.clone(),
            ));
        }

        // Multiple values: expand to OR of equality filters
        // This allows efficient zone collection: each equality uses ZoneXorIndex,
        // then zones are unioned by ZoneGroupCollector
        debug!(
            target: "query::filter",
            field = %field,
            values_count = values.len(),
            "Expanding IN expression to OR of equality filters for efficient zone collection"
        );

        let equality_filters =
            FilterGroup::new_equality_filters(field, values, event_type_uid.clone());

        Some(FilterGroup::Or(equality_filters))
    }

    /// Expands OR of equality comparisons to FilterGroup
    /// Used when OR expressions contain multiple equality comparisons on the same field
    pub fn expand_or_equalities(
        field: String,
        values: Vec<JsonValue>,
        event_type_uid: &Option<String>,
    ) -> FilterGroup {
        debug!(
            target: "query::filter",
            field = %field,
            values_count = values.len(),
            "Converting OR of equality comparisons to expanded OR filters for efficient zone collection"
        );

        let equality_filters =
            FilterGroup::new_equality_filters(field, &values, event_type_uid.clone());

        FilterGroup::Or(equality_filters)
    }
}
