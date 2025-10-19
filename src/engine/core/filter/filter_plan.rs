use crate::command::types::{Command, CompareOp, Expr};
use crate::engine::schema::registry::SchemaRegistry;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

#[derive(Debug, Clone)]
pub struct FilterPlan {
    pub column: String,
    pub operation: Option<CompareOp>,
    pub value: Option<Value>,
    pub priority: u32,
    pub uid: Option<String>,
}

/// A set-like collection for unique-by-column filter plans, replacing entries based on precedence
/// (lower numeric priority means higher precedence) and preserving insertion order for stability.
#[derive(Debug, Default, Clone)]
struct FilterPlanSet {
    by_column: HashMap<String, FilterPlan>,
    insertion_order: Vec<String>,
}

impl FilterPlanSet {
    fn new() -> Self {
        Self {
            by_column: HashMap::new(),
            insertion_order: Vec::new(),
        }
    }

    fn insert_with_precedence(&mut self, plan: FilterPlan) {
        let column = plan.column.clone();
        if let Some(existing) = self.by_column.get(&column) {
            if plan.priority < existing.priority {
                debug!(target: "query::filter", column = %column, "Replacing existing filter plan due to higher precedence");
                self.by_column.insert(column, plan);
            } else {
                debug!(target: "query::filter", column = %column, "Keeping existing filter plan with higher or equal precedence");
            }
        } else {
            debug!(target: "query::filter", column = %column, "Adding new filter plan");
            self.insertion_order.push(column.clone());
            self.by_column.insert(column, plan);
        }
    }

    fn into_vec(self) -> Vec<FilterPlan> {
        let mut out = Vec::with_capacity(self.by_column.len());
        for col in self.insertion_order {
            if let Some(plan) = self.by_column.get(&col) {
                out.push(plan.clone());
            }
        }
        out
    }
}

impl FilterPlan {
    pub fn add_to(self, filter_plans: &mut Vec<FilterPlan>) {
        if let Some(existing) = filter_plans.iter_mut().find(|f| f.column == self.column) {
            // Prefer lower priority value (higher precedence). Do not override a more specific plan
            // with a fallback one of lower precedence.
            if self.priority < existing.priority {
                debug!(target: "query::filter", column = %self.column, "Replacing existing filter plan due to higher precedence");
                *existing = self;
            } else {
                debug!(target: "query::filter", column = %self.column, "Keeping existing filter plan with higher or equal precedence");
            }
        } else {
            debug!(target: "query::filter", column = %self.column, "Adding new filter plan");
            filter_plans.push(self);
        }
    }

    pub async fn build_all(
        command: &Command,
        registry: &Arc<RwLock<SchemaRegistry>>,
    ) -> Vec<FilterPlan> {
        // Collect one unique plan per column here (precedence-aware)
        let mut unique_filters = FilterPlanSet::new();
        // Where clause filters can yield multiple plans per column; collect separately
        let mut where_filters: Vec<FilterPlan> = Vec::new();

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

            info!(target: "query::planner", event_type = %event_type, "Planning filters for query");

            Self::add_event_type(event_type, &event_type_uid, &mut unique_filters);
            Self::add_context_id(context_id, &event_type_uid, &mut unique_filters);
            let tf = time_field.as_deref().unwrap_or("timestamp");
            Self::add_time_filter(tf, since, &event_type_uid, &mut unique_filters);

            let mut where_clause_fields = HashSet::new();
            if let Some(expr) = where_clause {
                debug!(target: "query::planner", ?expr, "Processing where clause");
                Self::extract_fields_from_expr(expr, &mut where_clause_fields);
                Self::add_where_clause_filters(expr, &event_type_uid, &mut where_filters);
            }

            if let Some(schema) = registry.read().await.get(event_type) {
                for field in schema.fields() {
                    if ["context_id", "event_type", "timestamp"].contains(&field.as_str()) {
                        continue;
                    }
                    if !where_clause_fields.contains(field) {
                        debug!(target: "query::fallbacks", field = %field, "Adding fallback filter plan (no where clause)");
                        let plan = FilterPlan {
                            column: field.clone(),
                            operation: None,
                            value: None,
                            priority: 3,
                            uid: event_type_uid.clone(),
                        };
                        unique_filters.insert_with_precedence(plan);
                    }
                }
            }
        }

        let mut filter_plans = unique_filters.into_vec();
        filter_plans.extend(where_filters);

        info!(target: "query::planner", total_filters = filter_plans.len(), "Filter planning complete");
        filter_plans
    }

    fn extract_fields_from_expr(expr: &Expr, fields: &mut HashSet<String>) {
        match expr {
            Expr::Compare { field, .. } => {
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

    pub fn is_event_type(&self) -> bool {
        self.column == "event_type"
    }

    pub fn is_context_id(&self) -> bool {
        self.column == "context_id"
    }

    fn add_event_type(
        event_type: &str,
        event_type_uid: &Option<String>,
        filter_plans: &mut FilterPlanSet,
    ) {
        debug!(target: "query::filter", value = %event_type, "Adding event_type filter");
        let plan = FilterPlan {
            column: "event_type".to_string(),
            operation: Some(CompareOp::Eq),
            value: Some(Value::String(event_type.to_string())),
            priority: 0,
            uid: event_type_uid.clone(),
        };
        filter_plans.insert_with_precedence(plan);
    }

    fn add_context_id(
        context_id: &Option<String>,
        event_type_uid: &Option<String>,
        filter_plans: &mut FilterPlanSet,
    ) {
        debug!(target: "query::filter", value = ?context_id, "Adding context_id filter");
        let value = context_id.as_ref().map(|id| Value::String(id.clone()));
        let plan = FilterPlan {
            column: "context_id".to_string(),
            operation: Some(CompareOp::Eq),
            value,
            priority: 0,
            uid: event_type_uid.clone(),
        };
        filter_plans.insert_with_precedence(plan);
    }

    fn add_time_filter(
        time_field: &str,
        since: &Option<String>,
        event_type_uid: &Option<String>,
        filter_plans: &mut FilterPlanSet,
    ) {
        if let Some(since_val) = since {
            debug!(target: "query::filter", field = %time_field, value = %since_val, "Adding time filter (GTE)");
        }
        let plan = FilterPlan {
            column: time_field.to_string(),
            operation: Some(CompareOp::Gte),
            value: since.as_ref().map(|s| Value::String(s.clone())),
            priority: 1,
            uid: event_type_uid.clone(),
        };
        filter_plans.insert_with_precedence(plan);
    }

    fn add_where_clause_filters(
        expr: &Expr,
        event_type_uid: &Option<String>,
        filter_plans: &mut Vec<FilterPlan>,
    ) {
        match expr {
            Expr::Compare { field, op, value } => {
                let priority = if field == "timestamp" { 1 } else { 2 };
                debug!(
                    target: "query::filter",
                    field = %field,
                    op = ?op,
                    value = %value,
                    "Adding where clause filter"
                );
                // For where-clause filters, allow multiple entries for the same column
                // (e.g., plan = "pro" OR plan = "premium") so each branch gets its own step.
                filter_plans.push(FilterPlan {
                    column: field.clone(),
                    operation: Some(op.clone()),
                    value: Some(value.clone()),
                    priority,
                    uid: event_type_uid.clone(),
                });
            }
            Expr::And(left, right) | Expr::Or(left, right) => {
                Self::add_where_clause_filters(left, event_type_uid, filter_plans);
                Self::add_where_clause_filters(right, event_type_uid, filter_plans);
            }
            Expr::Not(expr) => {
                Self::add_where_clause_filters(expr, event_type_uid, filter_plans);
            }
        }
    }
}

impl FilterPlan {
    pub fn remove_implicit_since(
        filter_plans: &mut Vec<FilterPlan>,
        time_field: &str,
        since: &Option<String>,
    ) {
        if since.is_none() {
            return;
        }
        filter_plans.retain(|fp| {
            if fp.column != time_field {
                return true;
            }
            match (&fp.operation, &fp.value) {
                (Some(CompareOp::Gte), Some(Value::String(s))) if since.as_deref() == Some(s) => {
                    false
                }
                _ => true,
            }
        });
    }
}
