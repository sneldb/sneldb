use crate::command::types::{Command, CompareOp, Expr};
use crate::engine::core::{ConditionEvaluator, LogicalCondition, LogicalOp, QueryPlan};
use tracing::info;

/// Builds a ConditionEvaluator by combining where clause conditions and special field conditions
#[derive(Debug)]
pub struct ConditionEvaluatorBuilder {
    evaluator: ConditionEvaluator,
}

impl ConditionEvaluatorBuilder {
    pub fn new() -> Self {
        Self {
            evaluator: ConditionEvaluator::new(),
        }
    }

    pub fn add_where_clause(&mut self, where_clause: &Expr) {
        match where_clause {
            Expr::Compare { field, op, value } => {
                if let Some(num_value) = value.as_f64().map(|n| n as i64) {
                    info!(
                        target: "sneldb::evaluator",
                        "Adding numeric condition: {} {:?} {}",
                        field, op, num_value
                    );
                    self.evaluator.add_numeric_condition(
                        field.clone(),
                        op.clone().into(),
                        num_value,
                    );
                } else if let Some(str_value) = value.as_str() {
                    info!(
                        target: "sneldb::evaluator",
                        "Adding string condition: {} {:?} '{}'",
                        field, op, str_value
                    );
                    self.evaluator.add_string_condition(
                        field.clone(),
                        op.clone().into(),
                        str_value.to_string(),
                    );
                } else {
                    info!(
                        target: "sneldb::evaluator",
                        "Unsupported value type in comparison for field '{}'", field
                    );
                }
            }
            Expr::And(left, right) => {
                info!(target: "sneldb::evaluator", "Parsing AND expression");
                let mut left_builder = ConditionEvaluatorBuilder::new();
                left_builder.add_where_clause(left);
                let mut right_builder = ConditionEvaluatorBuilder::new();
                right_builder.add_where_clause(right);

                let left_condition = left_builder.into_evaluator().into_conditions();
                let right_condition = right_builder.into_evaluator().into_conditions();

                let mut combined_conditions = Vec::new();
                combined_conditions.extend(left_condition);
                combined_conditions.extend(right_condition);

                let logical_condition = LogicalCondition::new(combined_conditions, LogicalOp::And);
                self.evaluator.add_logical_condition(logical_condition);
            }
            Expr::Or(left, right) => {
                info!(target: "sneldb::evaluator", "Parsing OR expression");
                let mut left_builder = ConditionEvaluatorBuilder::new();
                left_builder.add_where_clause(left);
                let mut right_builder = ConditionEvaluatorBuilder::new();
                right_builder.add_where_clause(right);

                let left_condition = left_builder.into_evaluator().into_conditions();
                let right_condition = right_builder.into_evaluator().into_conditions();

                let mut combined_conditions = Vec::new();
                combined_conditions.extend(left_condition);
                combined_conditions.extend(right_condition);

                let logical_condition = LogicalCondition::new(combined_conditions, LogicalOp::Or);
                self.evaluator.add_logical_condition(logical_condition);
            }
            Expr::Not(expr) => {
                info!(target: "sneldb::evaluator", "Parsing NOT expression");
                let mut expr_builder = ConditionEvaluatorBuilder::new();
                expr_builder.add_where_clause(expr);

                let expr_condition = expr_builder.into_evaluator().into_conditions();

                let logical_condition = LogicalCondition::new(expr_condition, LogicalOp::Not);
                self.evaluator.add_logical_condition(logical_condition);
            }
        }
    }

    pub fn add_special_fields(&mut self, plan: &QueryPlan) {
        if let Command::Query { event_type, .. } = &plan.command {
            if event_type != "*" {
                info!(
                    target: "sneldb::evaluator",
                    "Adding event_type condition: event_type = '{}'", event_type
                );
                self.evaluator.add_string_condition(
                    "event_type".to_string(),
                    CompareOp::Eq.into(),
                    event_type.clone(),
                );
            }
        }

        if let Some(context_id) = plan.context_id() {
            info!(
                target: "sneldb::evaluator",
                "Adding context_id condition: context_id = '{}'", context_id
            );
            self.evaluator.add_string_condition(
                "context_id".to_string(),
                CompareOp::Eq.into(),
                context_id.to_string(),
            );
        }

        if let Command::Query { since, .. } = &plan.command {
            if let Some(since) = since {
                if let Ok(parsed) = since.parse::<i64>() {
                    info!(
                        target: "sneldb::evaluator",
                        "Adding timestamp condition: timestamp >= {}", parsed
                    );
                    self.evaluator.add_numeric_condition(
                        "timestamp".to_string(),
                        CompareOp::Gte.into(),
                        parsed,
                    );
                } else {
                    info!(
                        target: "sneldb::evaluator",
                        "Failed to parse 'since' as i64: '{}'", since
                    );
                }
            }
        }
    }

    pub fn into_evaluator(self) -> ConditionEvaluator {
        info!(target: "sneldb::evaluator", "ConditionEvaluator finalized");
        self.evaluator
    }

    pub fn build_from_plan(plan: &QueryPlan) -> ConditionEvaluator {
        let mut builder = ConditionEvaluatorBuilder::new();

        if let Some(where_clause) = plan.where_clause() {
            info!(target: "sneldb::evaluator", "Building from where clause");
            builder.add_where_clause(where_clause);
        }

        builder.add_special_fields(plan);
        builder.into_evaluator()
    }
}
