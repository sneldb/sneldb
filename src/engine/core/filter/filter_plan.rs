use crate::command::types::{Command, CompareOp, Expr};
use crate::engine::schema::registry::SchemaRegistry;
use serde_json::Value;
use std::collections::HashSet;
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

impl FilterPlan {
    pub fn add_to(self, filter_plans: &mut Vec<FilterPlan>) {
        if let Some(existing) = filter_plans.iter_mut().find(|f| f.column == self.column) {
            debug!(target: "query::filter", column = %self.column, "Overriding existing filter plan");
            *existing = self;
        } else {
            debug!(target: "query::filter", column = %self.column, "Adding new filter plan");
            filter_plans.push(self);
        }
    }

    pub async fn build_all(
        command: &Command,
        registry: &Arc<RwLock<SchemaRegistry>>,
    ) -> Vec<FilterPlan> {
        let mut filter_plans = Vec::new();

        if let Command::Query {
            event_type,
            context_id,
            since,
            where_clause,
            ..
        } = command
        {
            let event_type_uid = registry.read().await.get_uid(event_type);

            info!(target: "query::planner", event_type = %event_type, "Planning filters for query");

            Self::add_event_type(event_type, &event_type_uid, &mut filter_plans);
            Self::add_context_id(context_id, &event_type_uid, &mut filter_plans);
            Self::add_timestamp(since, &event_type_uid, &mut filter_plans);

            let mut where_clause_fields = HashSet::new();
            if let Some(expr) = where_clause {
                debug!(target: "query::planner", ?expr, "Processing where clause");
                Self::extract_fields_from_expr(expr, &mut where_clause_fields);
                Self::add_where_clause_filters(expr, &event_type_uid, &mut filter_plans);
            }

            if let Some(schema) = registry.read().await.get(event_type) {
                for field in schema.fields() {
                    if ["context_id", "event_type", "timestamp"].contains(&field.as_str()) {
                        continue;
                    }
                    if !where_clause_fields.contains(field) {
                        debug!(target: "query::fallbacks", field = %field, "Adding fallback filter plan (no where clause)");
                        FilterPlan {
                            column: field.clone(),
                            operation: None,
                            value: None,
                            priority: 3,
                            uid: event_type_uid.clone(),
                        }
                        .add_to(&mut filter_plans);
                    }
                }
            }
        }

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
        filter_plans: &mut Vec<FilterPlan>,
    ) {
        debug!(target: "query::filter", value = %event_type, "Adding event_type filter");
        FilterPlan {
            column: "event_type".to_string(),
            operation: Some(CompareOp::Eq),
            value: Some(Value::String(event_type.to_string())),
            priority: 0,
            uid: event_type_uid.clone(),
        }
        .add_to(filter_plans);
    }

    fn add_context_id(
        context_id: &Option<String>,
        event_type_uid: &Option<String>,
        filter_plans: &mut Vec<FilterPlan>,
    ) {
        debug!(target: "query::filter", value = ?context_id, "Adding context_id filter");
        let value = context_id.as_ref().map(|id| Value::String(id.clone()));
        FilterPlan {
            column: "context_id".to_string(),
            operation: Some(CompareOp::Eq),
            value,
            priority: 0,
            uid: event_type_uid.clone(),
        }
        .add_to(filter_plans);
    }

    fn add_timestamp(
        since: &Option<String>,
        event_type_uid: &Option<String>,
        filter_plans: &mut Vec<FilterPlan>,
    ) {
        if let Some(since_val) = since {
            debug!(target: "query::filter", value = %since_val, "Adding timestamp filter (GTE)");
        }
        FilterPlan {
            column: "timestamp".to_string(),
            operation: Some(CompareOp::Gte),
            value: since.as_ref().map(|s| Value::String(s.clone())),
            priority: 1,
            uid: event_type_uid.clone(),
        }
        .add_to(filter_plans);
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
