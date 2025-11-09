use crate::command::types::CompareOp;
use crate::engine::core::filter::condition::LogicalOp;
use crate::engine::core::read::index_strategy::IndexStrategy;
use crate::engine::types::ScalarValue;
use std::collections::HashSet;
use std::fmt::Write;

/// Priority constants for filter ordering
pub struct FilterPriority;

impl FilterPriority {
    pub const EVENT_TYPE: u32 = 0;
    pub const CONTEXT_ID: u32 = 0;
    pub const TIMESTAMP: u32 = 1;
    pub const DEFAULT: u32 = 2;
    pub const FALLBACK: u32 = 3;

    /// Calculates priority based on field name
    pub fn for_field(field: &str) -> u32 {
        match field {
            "event_type" | "context_id" => Self::EVENT_TYPE,
            "timestamp" => Self::TIMESTAMP,
            _ => Self::DEFAULT,
        }
    }
}

/// Represents a logical grouping of filters, preserving the structure from the WHERE clause.
/// This allows zone collection to correctly combine zones using OR/AND semantics.
///
/// **Single Responsibility**: Preserve logical structure (OR/AND/NOT) from WHERE clause.
/// FilterGroup is the primary abstraction for filters, replacing FilterPlan.
#[derive(Debug, Clone)]
pub enum FilterGroup {
    /// A single filter condition
    Filter {
        column: String,
        operation: Option<CompareOp>,
        value: Option<ScalarValue>,
        priority: u32,
        uid: Option<String>,
        index_strategy: Option<IndexStrategy>,
    },
    /// AND group: all children must match
    And(Vec<FilterGroup>),
    /// OR group: at least one child must match
    Or(Vec<FilterGroup>),
    /// NOT group: the child must not match
    Not(Box<FilterGroup>),
}

/// Creates a unique string key for a filter based on column, operation, and value.
/// Used for deduplicating filters in the tree and caching zones.
/// Optimized to reduce allocations.
pub fn filter_key(column: &str, operation: &Option<CompareOp>, value: &Option<ScalarValue>) -> String {
    // Pre-allocate with estimated capacity
    let mut key = String::with_capacity(column.len() + 30);

    key.push_str(column);
    key.push(':');

    // Operation string
    match operation {
        None => key.push_str("None"),
        Some(CompareOp::Eq) => key.push_str("Eq"),
        Some(CompareOp::Neq) => key.push_str("Neq"),
        Some(CompareOp::Gt) => key.push_str("Gt"),
        Some(CompareOp::Gte) => key.push_str("Gte"),
        Some(CompareOp::Lt) => key.push_str("Lt"),
        Some(CompareOp::Lte) => key.push_str("Lte"),
        Some(CompareOp::In) => key.push_str("In"),
    }
    key.push(':');

    // Value string - optimized to avoid format! allocations where possible
    match value {
        None => key.push_str("None"),
        Some(ScalarValue::Null) => key.push_str("Null"),
        Some(ScalarValue::Boolean(b)) => {
            key.push_str(if *b { "Bool(true)" } else { "Bool(false)" });
        }
        Some(ScalarValue::Int64(i)) => {
            let _ = write!(key, "Int64({})", i);
        }
        Some(ScalarValue::Float64(f)) => {
            if f.is_nan() {
                key.push_str("Float64(NaN)");
            } else {
                let _ = write!(key, "Float64({})", f);
            }
        }
        Some(ScalarValue::Timestamp(t)) => {
            let _ = write!(key, "Timestamp({})", t);
        }
        Some(ScalarValue::Utf8(s)) => {
            key.push_str("Utf8(");
            key.push_str(s);
            key.push(')');
        }
        Some(ScalarValue::Binary(b)) => {
            let _ = write!(key, "Binary({:?})", b);
        }
    }

    key
}

impl FilterGroup {
    /// Creates a new Filter variant with standard defaults
    pub fn new_filter(
        column: String,
        operation: Option<CompareOp>,
        value: Option<ScalarValue>,
        event_type_uid: Option<String>,
    ) -> Self {
        let priority = FilterPriority::for_field(&column);
        Self::Filter {
            column,
            operation,
            value,
            priority,
            uid: event_type_uid,
            index_strategy: None,
        }
    }

    /// Creates an equality filter (common pattern)
    pub fn new_equality_filter(
        column: String,
        value: ScalarValue,
        event_type_uid: Option<String>,
    ) -> Self {
        Self::new_filter(
            column,
            Some(CompareOp::Eq),
            Some(value),
            event_type_uid,
        )
    }

    /// Creates multiple equality filters for IN expansion
    pub fn new_equality_filters(
        column: String,
        values: &[serde_json::Value],
        event_type_uid: Option<String>,
    ) -> Vec<Self> {
        values
            .iter()
            .map(|v| Self::new_equality_filter(
                column.clone(),
                ScalarValue::from(v.clone()),
                event_type_uid.clone(),
            ))
            .collect()
    }

    /// Extracts all individual FilterGroups from this group tree, in execution order.
    /// Each returned FilterGroup is a single Filter (not And/Or/Not).
    pub fn extract_individual_filters(&self) -> Vec<FilterGroup> {
        match self {
            FilterGroup::Filter { .. } => vec![self.clone()],
            FilterGroup::And(children) | FilterGroup::Or(children) => {
                children.iter().flat_map(|c| c.extract_individual_filters()).collect()
            }
            FilterGroup::Not(child) => child.extract_individual_filters(),
        }
    }

    /// Extracts all unique leaf filters (Filter variants) from the tree.
    /// Returns a Vec of unique FilterGroup::Filter instances, deduplicated by (column, operation, value).
    /// This is used for optimizing zone collection by collecting zones once per unique filter.
    pub fn extract_unique_filters(&self) -> Vec<FilterGroup> {
        let mut seen = HashSet::new();
        let mut unique = Vec::new();

        self.extract_unique_filters_recursive(&mut seen, &mut unique);
        unique
    }

    /// Recursive helper for extract_unique_filters
    fn extract_unique_filters_recursive(
        &self,
        seen: &mut HashSet<String>,
        unique: &mut Vec<FilterGroup>,
    ) {
        match self {
            FilterGroup::Filter {
                column,
                operation,
                value,
                ..
            } => {
                let key = filter_key(column, operation, value);
                if seen.insert(key) {
                    unique.push(self.clone());
                }
            }
            FilterGroup::And(children) | FilterGroup::Or(children) => {
                for child in children {
                    child.extract_unique_filters_recursive(seen, unique);
                }
            }
            FilterGroup::Not(child) => {
                child.extract_unique_filters_recursive(seen, unique);
            }
        }
    }

    /// Returns the logical operator for this group
    pub fn operator(&self) -> LogicalOp {
        match self {
            FilterGroup::Filter { .. } => LogicalOp::And, // Single filter is treated as AND
            FilterGroup::And(_) => LogicalOp::And,
            FilterGroup::Or(_) => LogicalOp::Or,
            FilterGroup::Not(_) => LogicalOp::Not,
        }
    }

    /// Checks if this group contains only a single filter (no logical operations)
    pub fn is_single_filter(&self) -> bool {
        matches!(self, FilterGroup::Filter { .. })
    }

    /// Checks if this is an event_type filter
    pub fn is_event_type(&self) -> bool {
        match self {
            FilterGroup::Filter { column, .. } => column == "event_type",
            _ => false,
        }
    }

    /// Checks if this is a context_id filter
    pub fn is_context_id(&self) -> bool {
        match self {
            FilterGroup::Filter { column, .. } => column == "context_id",
            _ => false,
        }
    }

    /// Gets the column name if this is a single filter
    pub fn column(&self) -> Option<&str> {
        match self {
            FilterGroup::Filter { column, .. } => Some(column),
            _ => None,
        }
    }

    /// Gets the operation if this is a single filter
    pub fn operation(&self) -> Option<&CompareOp> {
        match self {
            FilterGroup::Filter { operation, .. } => operation.as_ref(),
            _ => None,
        }
    }

    /// Gets the value if this is a single filter
    pub fn value(&self) -> Option<&ScalarValue> {
        match self {
            FilterGroup::Filter { value, .. } => value.as_ref(),
            _ => None,
        }
    }

    /// Gets mutable access to index_strategy if this is a single filter
    pub fn index_strategy_mut(&mut self) -> Option<&mut Option<IndexStrategy>> {
        match self {
            FilterGroup::Filter { index_strategy, .. } => Some(index_strategy),
            _ => None,
        }
    }

    /// Updates index_strategy in this tree by matching against a list of filters.
    /// This keeps the tree in sync with the flat list after IndexPlanner assigns strategies.
    pub fn sync_index_strategies_from(&mut self, filters: &[FilterGroup]) {
        match self {
            FilterGroup::Filter {
                column,
                operation,
                value,
                index_strategy,
                ..
            } => {
                // If strategy is already set, keep it
                if index_strategy.is_some() {
                    return;
                }
                // Find matching filter in the list
                for fg in filters {
                    if let FilterGroup::Filter {
                        column: fg_col,
                        operation: fg_op,
                        value: fg_val,
                        index_strategy: fg_strategy,
                        ..
                    } = fg {
                        if fg_col == column && fg_op == operation && fg_val == value {
                            *index_strategy = fg_strategy.clone();
                            break;
                        }
                    }
                }
            }
            FilterGroup::And(children) | FilterGroup::Or(children) => {
                for child in children {
                    child.sync_index_strategies_from(filters);
                }
            }
            FilterGroup::Not(child) => {
                child.sync_index_strategies_from(filters);
            }
        }
    }
}
