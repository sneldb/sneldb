/// WHERE clause evaluator for sequence queries.
///
/// Handles event-specific field references (e.g., `page_view.page`) by
/// evaluating conditions on columnar data for specific event types.
use crate::command::types::Expr;
use crate::engine::core::CandidateZone;
use crate::engine::core::filter::condition::PreparedAccessor;
use crate::engine::core::filter::condition_evaluator::ConditionEvaluator;
use crate::engine::core::filter::condition_evaluator_builder::ConditionEvaluatorBuilder;
use crate::engine::core::read::sequence::group::RowIndex;
use crate::engine::core::read::sequence::utils;
use crate::engine::schema::registry::SchemaRegistry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, trace};

/// Evaluates WHERE clauses for sequence queries with event-specific field references.
///
/// This evaluator can handle conditions like:
/// - `page_view.page = "/checkout"` (event-specific)
/// - `order_created.status = "done"` (event-specific)
/// - `timestamp > 1000` (common field)
///
/// It builds separate evaluators for each event type and applies them
/// during sequence matching to filter rows before materialization.
pub struct SequenceWhereEvaluator {
    /// Map from event type to its condition evaluator
    evaluators_by_event_type: HashMap<String, ConditionEvaluator>,
}

impl SequenceWhereEvaluator {
    /// Creates a new SequenceWhereEvaluator from a WHERE clause expression.
    ///
    /// Parses the WHERE clause to extract event-specific conditions and builds
    /// evaluators for each event type.
    ///
    /// # Arguments
    ///
    /// * `where_clause` - The WHERE clause expression (may contain event-prefixed fields)
    /// * `event_types` - List of event types in the sequence
    /// * `registry` - Schema registry to validate field ambiguity
    ///
    /// # Returns
    ///
    /// Returns `Ok(Self)` if successful, or `Err(String)` if there's field ambiguity
    /// (e.g., a common field exists in multiple event types without an event prefix).
    pub async fn new(
        where_clause: Option<&Expr>,
        event_types: &[String],
        registry: &Arc<RwLock<SchemaRegistry>>,
    ) -> Result<Self, String> {
        if tracing::enabled!(tracing::Level::INFO) {
            info!(
                target: "sneldb::sequence::where_evaluator",
                has_where_clause = where_clause.is_some(),
                event_type_count = event_types.len(),
                "Creating sequence WHERE evaluator"
            );
        }

        let mut evaluators_by_event_type = HashMap::new();

        if let Some(expr) = where_clause {
            if tracing::enabled!(tracing::Level::DEBUG) {
                debug!(
                    target: "sneldb::sequence::where_evaluator",
                    "Parsing WHERE clause for event-specific conditions"
                );
            }

            // Validate field ambiguity before transforming
            Self::validate_field_ambiguity(expr, event_types, registry).await?;

            // Transform WHERE clause for each event type, preserving logical structure (OR/NOT/AND)

            if tracing::enabled!(tracing::Level::DEBUG) {
                debug!(
                    target: "sneldb::sequence::where_evaluator",
                    "Transforming WHERE clause for each event type"
                );
            }

            // Build evaluators for each event type
            for event_type in event_types {
                if let Some(transformed_expr) =
                    utils::transform_where_clause_for_event_type(expr, event_type)
                {
                    if tracing::enabled!(tracing::Level::DEBUG) {
                        debug!(
                            target: "sneldb::sequence::where_evaluator",
                            event_type = %event_type,
                            transformed_expr = ?transformed_expr,
                            "Building evaluator for event type with transformed expression"
                        );
                    }

                    let mut builder = ConditionEvaluatorBuilder::new();
                    builder.add_where_clause(&transformed_expr);
                    evaluators_by_event_type.insert(event_type.clone(), builder.into_evaluator());
                } else {
                    if tracing::enabled!(tracing::Level::DEBUG) {
                        debug!(
                            target: "sneldb::sequence::where_evaluator",
                            event_type = %event_type,
                            "No conditions for event type, will pass all rows"
                        );
                    }
                }
            }
        }

        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                target: "sneldb::sequence::where_evaluator",
                evaluators = evaluators_by_event_type.len(),
                "Created sequence WHERE evaluator"
            );
        }

        Ok(Self {
            evaluators_by_event_type,
        })
    }

    /// Validates that common fields (without event prefix) don't create ambiguity.
    ///
    /// If a field without an event prefix exists in multiple event types,
    /// this returns an error requiring the user to use event-prefixed fields.
    ///
    /// # Arguments
    ///
    /// * `expr` - The WHERE clause expression
    /// * `event_types` - List of event types in the sequence
    /// * `registry` - Schema registry to check field existence
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if no ambiguity, or `Err(String)` with error message if ambiguity detected.
    async fn validate_field_ambiguity(
        expr: &Expr,
        event_types: &[String],
        registry: &Arc<RwLock<SchemaRegistry>>,
    ) -> Result<(), String> {
        // Collect all common fields (without event prefix) from the expression
        let mut common_fields = HashSet::new();
        Self::collect_common_fields(expr, &mut common_fields);

        if common_fields.is_empty() {
            // No common fields, no ambiguity possible
            return Ok(());
        }

        // Check each common field against all event types
        let registry_read = registry.read().await;
        for field in &common_fields {
            // Skip core fields that are expected to exist in all event types
            if matches!(
                field.as_str(),
                "timestamp" | "context_id" | "event_type" | "event_id"
            ) {
                continue;
            }

            // Find which event types have this field
            let mut event_types_with_field = Vec::new();
            for event_type in event_types {
                if let Some(schema) = registry_read.get(event_type) {
                    if schema.fields.contains_key(field) {
                        event_types_with_field.push(event_type.clone());
                    }
                }
            }

            // If field exists in multiple event types, it's ambiguous
            if event_types_with_field.len() > 1 {
                return Err(format!(
                    "Ambiguous field '{}' exists in multiple event types: {}. Use event-prefixed fields (e.g., '{}.{}' or '{}.{}') to disambiguate.",
                    field,
                    event_types_with_field.join(", "),
                    event_types_with_field[0],
                    field,
                    event_types_with_field[1],
                    field
                ));
            }
        }

        Ok(())
    }

    /// Recursively collects common fields (without event prefix) from an expression.
    fn collect_common_fields(expr: &Expr, common_fields: &mut HashSet<String>) {
        match expr {
            Expr::Compare { field, .. } => {
                // Only collect fields without event prefix
                if Self::parse_event_field(field).is_none() {
                    common_fields.insert(field.clone());
                }
            }
            Expr::In { field, .. } => {
                // Only collect fields without event prefix
                if Self::parse_event_field(field).is_none() {
                    common_fields.insert(field.clone());
                }
            }
            Expr::And(left, right) => {
                Self::collect_common_fields(left, common_fields);
                Self::collect_common_fields(right, common_fields);
            }
            Expr::Or(left, right) => {
                Self::collect_common_fields(left, common_fields);
                Self::collect_common_fields(right, common_fields);
            }
            Expr::Not(inner) => {
                Self::collect_common_fields(inner, common_fields);
            }
        }
    }

    /// Evaluates WHERE clause for a specific row in a zone.
    ///
    /// Returns true if the row matches the WHERE clause conditions for its event type.
    ///
    /// # Arguments
    ///
    /// * `event_type` - The event type of the row
    /// * `zone` - The zone containing the row
    /// * `row_index` - The row index within the zone
    pub fn evaluate_row(
        &self,
        event_type: &str,
        zone: &CandidateZone,
        row_index: &RowIndex,
    ) -> bool {
        // If no evaluator for this event type, row passes
        let evaluator = match self.evaluators_by_event_type.get(event_type) {
            Some(eval) => eval,
            None => {
                if tracing::enabled!(tracing::Level::TRACE) {
                    trace!(
                        target: "sneldb::sequence::where_evaluator",
                        event_type = %event_type,
                        "No evaluator for event type, row passes"
                    );
                }
                return true;
            }
        };

        // Create accessor for this zone
        let accessor = PreparedAccessor::new(&zone.values);

        // Evaluate conditions at the row index
        let result = evaluator.evaluate_row_at(&accessor, row_index.row_idx);

        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                target: "sneldb::sequence::where_evaluator",
                event_type = %event_type,
                row_idx = row_index.row_idx,
                zone_idx = row_index.zone_idx,
                passes = result,
                zone_columns = ?zone.values.keys().collect::<Vec<_>>(),
                "Evaluated WHERE clause for row"
            );
        }

        result
    }

    /// Parses an event-prefixed field name.
    ///
    /// Returns `Some((event_type, field_name))` if the field has an event prefix,
    /// or `None` if it's a common field.
    ///
    /// # Examples
    ///
    /// - `"page_view.page"` → `Some(("page_view", "page"))`
    /// - `"order_created.status"` → `Some(("order_created", "status"))`
    /// - `"timestamp"` → `None`
    fn parse_event_field(field: &str) -> Option<(String, String)> {
        if let Some(dot_pos) = field.find('.') {
            let event_type = field[..dot_pos].to_string();
            let field_name = field[dot_pos + 1..].to_string();
            Some((event_type, field_name))
        } else {
            None
        }
    }
}
