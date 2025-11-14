use crate::engine::core::read::aggregate::plan::{AggregateOpSpec, AggregatePlan};
use crate::engine::core::read::result::ColumnSpec;

/// Builds output schema for aggregate operations
pub struct SchemaBuilder;

impl SchemaBuilder {
    /// Build column specifications for aggregate output
    pub fn build(plan: &AggregatePlan) -> Vec<ColumnSpec> {
        let mut columns = Vec::new();

        if plan.time_bucket.is_some() {
            columns.push(ColumnSpec {
                name: "bucket".to_string(),
                logical_type: "Timestamp".to_string(),
            });
        }

        if let Some(group_by) = &plan.group_by {
            for field in group_by {
                columns.push(ColumnSpec {
                    name: field.clone(),
                    logical_type: "String".to_string(),
                });
            }
        }

        for spec in &plan.ops {
            Self::add_spec_columns(&mut columns, spec);
        }

        columns
    }

    fn add_spec_columns(columns: &mut Vec<ColumnSpec>, spec: &AggregateOpSpec) {
        match spec {
            AggregateOpSpec::CountAll => {
                columns.push(ColumnSpec {
                    name: "count".to_string(),
                    logical_type: "Integer".to_string(),
                });
            }
            AggregateOpSpec::CountField { field } => {
                columns.push(ColumnSpec {
                    name: format!("count_{}", field),
                    logical_type: "Integer".to_string(),
                });
            }
            AggregateOpSpec::CountUnique { field } => {
                columns.push(ColumnSpec {
                    name: format!("count_unique_{}_values", field),
                    logical_type: "String".to_string(),
                });
            }
            AggregateOpSpec::Total { field } => {
                columns.push(ColumnSpec {
                    name: format!("total_{}", field),
                    logical_type: "Integer".to_string(),
                });
            }
            AggregateOpSpec::Avg { field } => {
                columns.push(ColumnSpec {
                    name: format!("avg_{}_sum", field),
                    logical_type: "Integer".to_string(),
                });
                columns.push(ColumnSpec {
                    name: format!("avg_{}_count", field),
                    logical_type: "Integer".to_string(),
                });
            }
            AggregateOpSpec::Min { field } => {
                columns.push(ColumnSpec {
                    name: format!("min_{}", field),
                    logical_type: "String".to_string(),
                });
            }
            AggregateOpSpec::Max { field } => {
                columns.push(ColumnSpec {
                    name: format!("max_{}", field),
                    logical_type: "String".to_string(),
                });
            }
        }
    }
}
