use crate::command::types::{Command, CompareOp, Expr};
use crate::engine::core::filter::filter_group::{FilterGroup, FilterPriority};
use crate::engine::core::filter::in_expansion::InExpansion;
use crate::engine::schema::FieldType;
use crate::engine::schema::registry::{MiniSchema, SchemaRegistry};
use crate::engine::types::ScalarValue;
use crate::shared::time::{TimeKind, TimeParser};
use serde_json::{Number, Value as JsonValue};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Builds FilterGroup trees from Expr, preserving logical structure.
/// **Single Responsibility**: Convert Expr → FilterGroup tree with optimizations.
pub struct FilterGroupBuilder;

impl FilterGroupBuilder {
    /// Builds all filters from a Command (event_type, context_id, since, where_clause, fallback filters).
    pub async fn build_all(
        command: &Command,
        registry: &Arc<RwLock<SchemaRegistry>>,
    ) -> Vec<FilterGroup> {
        // Collect one unique filter per column here (precedence-aware)
        let mut unique_filters: HashMap<String, FilterGroup> = HashMap::new();
        // Where clause filters can yield multiple filters per column; collect separately
        let mut where_filters: Vec<FilterGroup> = Vec::new();

        if let Command::Query {
            event_type,
            context_id,
            since,
            time_field,
            where_clause,
            ..
        } = command
        {
            let event_type_uid = registry.read().await.get_uid(event_type);

            if tracing::enabled!(tracing::Level::INFO) {
                info!(target: "query::planner", event_type = %event_type, "Planning filters for query");
            }

            // Add event_type filter
            Self::add_event_type(event_type, &event_type_uid, &mut unique_filters);
            // Add context_id filter
            Self::add_context_id(context_id, &event_type_uid, &mut unique_filters);
            // Add time filter
            let tf = time_field.as_deref().unwrap_or("timestamp");
            Self::add_time_filter(tf, since, &event_type_uid, &mut unique_filters);

            let mut where_clause_fields = HashSet::new();
            if let Some(expr) = where_clause {
                if tracing::enabled!(tracing::Level::DEBUG) {
                    debug!(target: "query::planner", ?expr, "Processing where clause");
                }
                // Normalize temporal literals using schema if available
                let normalized_expr = if let Some(schema) = registry.read().await.get(event_type) {
                    Self::normalize_temporal_literals(expr, schema)
                } else {
                    expr.clone()
                };
                Self::extract_fields_from_expr(&normalized_expr, &mut where_clause_fields);
                // Build FilterGroup from WHERE clause
                if let Some(group) = Self::build(&normalized_expr, &event_type_uid) {
                    where_filters.extend(group.extract_individual_filters());
                }
            }

            // Add fallback filters for schema fields not in where clause
            if let Some(schema) = registry.read().await.get(event_type) {
                for field in schema.fields() {
                    if ["context_id", "event_type", "timestamp"].contains(&field.as_str()) {
                        continue;
                    }
                    if !where_clause_fields.contains(field) {
                        if tracing::enabled!(tracing::Level::DEBUG) {
                            debug!(target: "query::fallbacks", field = %field, "Adding fallback filter (no where clause)");
                        }
                        let filter = FilterGroup::Filter {
                            column: field.clone(),
                            operation: None,
                            value: None,
                            priority: FilterPriority::FALLBACK,
                            uid: event_type_uid.clone(),
                            index_strategy: None,
                        };
                        // Insert with precedence (lower priority = higher precedence)
                        unique_filters.entry(field.clone()).or_insert_with(|| filter);
                    }
                }
            }
        }

        let mut filter_groups: Vec<FilterGroup> = unique_filters.into_values().collect();
        filter_groups.extend(where_filters);

        if tracing::enabled!(tracing::Level::INFO) {
            info!(target: "query::planner", total_filters = filter_groups.len(), "Filter planning complete");
        }
        filter_groups
    }

    /// Builds a FilterGroup from an Expr, preserving logical structure.
    pub fn build(
        expr: &Expr,
        event_type_uid: &Option<String>,
    ) -> Option<FilterGroup> {
        match expr {
            Expr::Compare { field, op, value } => {
                Some(FilterGroup::new_filter(
                    field.clone(),
                    Some(op.clone()),
                    Some(ScalarValue::from(value.clone())),
                    event_type_uid.clone(),
                ))
            }
            Expr::In { field, values } => {
                InExpansion::expand_in(field.clone(), values, event_type_uid)
            }
            Expr::And(left, right) => {
                let left_group = Self::build(left, event_type_uid)?;
                let right_group = Self::build(right, event_type_uid)?;
                Some(FilterGroup::And(vec![left_group, right_group]))
            }
            Expr::Or(left, right) => {
                // Optimization: Convert OR of equality comparisons on same field to expanded OR
                // This allows efficient zone collection: each equality uses ZoneXorIndex,
                // then zones are unioned by ZoneGroupCollector
                if let Some((field, values)) = Self::extract_or_equality_values(left, right) {
                    return Some(InExpansion::expand_or_equalities(field, values, event_type_uid));
                }

                // Otherwise, preserve OR structure
                let left_group = Self::build(left, event_type_uid)?;
                let right_group = Self::build(right, event_type_uid)?;

                // Flatten nested OR structures only when they can be flattened
                // (i.e., when all children are equality comparisons on the same field)
                // Otherwise, preserve the nested structure
                let mut children = Vec::new();

                // Handle left group
                let should_flatten_left = matches!(&left_group, FilterGroup::Or(children) if Self::can_flatten_or(children));
                if should_flatten_left {
                    if let FilterGroup::Or(mut left_children) = left_group {
                        children.append(&mut left_children);
                    }
                } else {
                    children.push(left_group);
                }

                // Handle right group
                let should_flatten_right = matches!(&right_group, FilterGroup::Or(children) if Self::can_flatten_or(children));
                if should_flatten_right {
                    if let FilterGroup::Or(mut right_children) = right_group {
                        children.append(&mut right_children);
                    }
                } else {
                    children.push(right_group);
                }

                Some(FilterGroup::Or(children))
            }
            Expr::Not(inner) => {
                let inner_group = Self::build(inner, event_type_uid)?;
                Some(FilterGroup::Not(Box::new(inner_group)))
            }
        }
    }

    /// Checks if an OR group can be flattened (all children are equality comparisons on the same field)
    fn can_flatten_or(children: &[FilterGroup]) -> bool {
        if children.is_empty() {
            return false;
        }

        let mut field_opt: Option<&str> = None;
        for child in children {
            match child {
                FilterGroup::Filter {
                    column,
                    operation: Some(CompareOp::Eq),
                    ..
                } => {
                    if let Some(field) = field_opt {
                        if field != column {
                            return false; // Different fields, can't flatten
                        }
                    } else {
                        field_opt = Some(column);
                    }
                }
                _ => {
                    return false; // Not all children are equality filters, can't flatten
                }
            }
        }
        true
    }

    /// Extracts equality comparison values from an OR expression if both sides are
    /// equality comparisons on the same field. Returns (field, values) if successful.
    fn extract_or_equality_values(
        left: &Expr,
        right: &Expr,
    ) -> Option<(String, Vec<serde_json::Value>)> {
        match (left, right) {
            (
                Expr::Compare {
                    field: field_left,
                    op: op_left,
                    value: value_left,
                },
                Expr::Compare {
                    field: field_right,
                    op: op_right,
                    value: value_right,
                },
            ) if field_left == field_right
                && matches!(op_left, CompareOp::Eq)
                && matches!(op_right, CompareOp::Eq) =>
            {
                Some((field_left.clone(), vec![value_left.clone(), value_right.clone()]))
            }
            // Handle nested OR expressions: (A OR B) OR C
            (Expr::Or(left_inner, right_inner), Expr::Compare { field, op, value })
            | (Expr::Compare { field, op, value }, Expr::Or(left_inner, right_inner))
                if matches!(op, CompareOp::Eq) =>
            {
                Self::extract_or_equality_values(left_inner, right_inner)
                    .and_then(|(field_inner, mut values_inner)| {
                        if field_inner == *field {
                            values_inner.push(value.clone());
                            Some((field.clone(), values_inner))
                        } else {
                            None
                        }
                    })
            }
            _ => None,
        }
    }

    /// Adds an event_type filter with highest priority (0)
    fn add_event_type(
        event_type: &str,
        event_type_uid: &Option<String>,
        unique_filters: &mut HashMap<String, FilterGroup>,
    ) {
        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(target: "query::filter", value = %event_type, "Adding event_type filter");
        }
        let filter = FilterGroup::new_filter(
            "event_type".to_string(),
            Some(CompareOp::Eq),
            Some(ScalarValue::Utf8(event_type.to_string())),
            event_type_uid.clone(),
        );
        unique_filters.insert("event_type".to_string(), filter);
    }

    /// Adds a context_id filter with highest priority (0)
    fn add_context_id(
        context_id: &Option<String>,
        event_type_uid: &Option<String>,
        unique_filters: &mut HashMap<String, FilterGroup>,
    ) {
        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(target: "query::filter", value = ?context_id, "Adding context_id filter");
        }
        let value = context_id.as_ref().map(|id| ScalarValue::Utf8(id.clone()));
        let filter = FilterGroup::new_filter(
            "context_id".to_string(),
            Some(CompareOp::Eq),
            value,
            event_type_uid.clone(),
        );
        unique_filters.insert("context_id".to_string(), filter);
    }

    /// Adds a time filter (GTE) with priority 1
    fn add_time_filter(
        time_field: &str,
        since: &Option<String>,
        event_type_uid: &Option<String>,
        unique_filters: &mut HashMap<String, FilterGroup>,
    ) {
        if let Some(since_val) = since {
            if tracing::enabled!(tracing::Level::DEBUG) {
                debug!(target: "query::filter", field = %time_field, value = %since_val, "Adding time filter (GTE)");
            }
        }
        let value = since.as_ref().map(|s| ScalarValue::Utf8(s.clone()));
        let filter = FilterGroup::new_filter(
            time_field.to_string(),
            Some(CompareOp::Gte),
            value,
            event_type_uid.clone(),
        );
        // Insert with precedence (lower priority = higher precedence)
        unique_filters.entry(time_field.to_string()).or_insert_with(|| filter);
    }

    /// Normalizes temporal literals in an Expr to epoch seconds
    fn normalize_temporal_literals(expr: &Expr, schema: &MiniSchema) -> Expr {
        match expr {
            Expr::Compare { field, op, value } => {
                let is_temporal = schema
                    .field_type(field)
                    .map(|ft| match ft {
                        FieldType::Timestamp | FieldType::Date => true,
                        FieldType::Optional(inner) => matches!(
                            **inner,
                            FieldType::Timestamp | FieldType::Date
                        ),
                        _ => false,
                    })
                    .unwrap_or(false);

                if is_temporal {
                    let scalar_value = ScalarValue::from(value.clone());
                    if let ScalarValue::Utf8(s) = &scalar_value {
                        let kind = match schema.field_type(field) {
                            Some(FieldType::Date) => TimeKind::Date,
                            Some(FieldType::Optional(inner)) => {
                                if matches!(*inner.as_ref(), FieldType::Date) {
                                    TimeKind::Date
                                } else {
                                    TimeKind::DateTime
                                }
                            }
                            _ => TimeKind::DateTime,
                        };
                        if let Some(parsed) = TimeParser::parse_str_to_epoch_seconds(s, kind) {
                            return Expr::Compare {
                                field: field.clone(),
                                op: op.clone(),
                                value: JsonValue::Number(Number::from(parsed)),
                            };
                        }
                    }
                }
                expr.clone()
            }
            Expr::In { field, values } => {
                let is_temporal = schema
                    .field_type(field)
                    .map(|ft| match ft {
                        FieldType::Timestamp | FieldType::Date => true,
                        FieldType::Optional(inner) => matches!(
                            **inner,
                            FieldType::Timestamp | FieldType::Date
                        ),
                        _ => false,
                    })
                    .unwrap_or(false);

                if is_temporal {
                    let kind = match schema.field_type(field) {
                        Some(FieldType::Date) => TimeKind::Date,
                        Some(FieldType::Optional(inner)) => {
                            if matches!(**inner, FieldType::Date) {
                                TimeKind::Date
                            } else {
                                TimeKind::DateTime
                            }
                        }
                        _ => TimeKind::DateTime,
                    };
                    let mut normalized_values = Vec::new();
                    let mut all_normalized = true;

                    for value in values {
                        let scalar_value = ScalarValue::from(value.clone());
                        if let ScalarValue::Utf8(s) = &scalar_value {
                            if let Some(parsed) = TimeParser::parse_str_to_epoch_seconds(s, kind) {
                                normalized_values.push(JsonValue::Number(Number::from(parsed)));
                            } else {
                                all_normalized = false;
                                break;
                            }
                        } else {
                            normalized_values.push(value.clone());
                        }
                    }

                    if all_normalized && !normalized_values.is_empty() {
                        return Expr::In {
                            field: field.clone(),
                            values: normalized_values,
                        };
                    }
                }
                expr.clone()
            }
            Expr::And(l, r) => Expr::And(
                Box::new(Self::normalize_temporal_literals(l, schema)),
                Box::new(Self::normalize_temporal_literals(r, schema)),
            ),
            Expr::Or(l, r) => Expr::Or(
                Box::new(Self::normalize_temporal_literals(l, schema)),
                Box::new(Self::normalize_temporal_literals(r, schema)),
            ),
            Expr::Not(x) => Expr::Not(Box::new(Self::normalize_temporal_literals(x, schema))),
        }
    }

    /// Extracts field names from an Expr
    fn extract_fields_from_expr(expr: &Expr, fields: &mut HashSet<String>) {
        match expr {
            Expr::Compare { field, .. } => {
                fields.insert(field.clone());
            }
            Expr::In { field, .. } => {
                fields.insert(field.clone());
            }
            Expr::And(left, right) | Expr::Or(left, right) => {
                Self::extract_fields_from_expr(left, fields);
                Self::extract_fields_from_expr(right, fields);
            }
            Expr::Not(expr) => {
                Self::extract_fields_from_expr(expr, fields);
            }
        }
    }
}

