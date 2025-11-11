use crate::command::types::{AggSpec, Command, QueryCommand, TimeGranularity};

/// Describes a single aggregate operation requested by the query
#[derive(Debug, Clone, PartialEq)]
pub enum AggregateOpSpec {
    /// COUNT of all matching rows
    CountAll,
    /// COUNT of non-null values of a specific field
    CountField { field: String },
    /// COUNT UNIQUE over a specific field
    CountUnique { field: String },
    /// SUM over a numeric field
    Total { field: String },
    /// AVG over a numeric field
    Avg { field: String },
    /// MIN over a comparable field
    Min { field: String },
    /// MAX over a comparable field
    Max { field: String },
}

/// Aggregate plan derived from the Query command
#[derive(Debug, Clone, PartialEq)]
pub struct AggregatePlan {
    pub ops: Vec<AggregateOpSpec>,
    pub group_by: Option<Vec<String>>,
    pub time_bucket: Option<TimeGranularity>,
}

impl AggregatePlan {
    /// Builds an `AggregatePlan` from a `Command::Query` if any aggregates are present
    pub fn from_command(command: &Command) -> Option<Self> {
        if let Command::Query {
            aggs,
            time_bucket,
            group_by,
            ..
        } = command
        {
            let Some(aggs_vec) = aggs.as_ref() else {
                return None;
            };

            let mut ops: Vec<AggregateOpSpec> = Vec::with_capacity(aggs_vec.len());
            for spec in aggs_vec {
                match spec {
                    AggSpec::Count { unique_field } => match unique_field {
                        Some(f) => ops.push(AggregateOpSpec::CountUnique { field: f.clone() }),
                        None => ops.push(AggregateOpSpec::CountAll),
                    },
                    AggSpec::CountField { field } => ops.push(AggregateOpSpec::CountField {
                        field: field.clone(),
                    }),
                    AggSpec::Total { field } => ops.push(AggregateOpSpec::Total {
                        field: field.clone(),
                    }),
                    AggSpec::Avg { field } => ops.push(AggregateOpSpec::Avg {
                        field: field.clone(),
                    }),
                    AggSpec::Min { field } => ops.push(AggregateOpSpec::Min {
                        field: field.clone(),
                    }),
                    AggSpec::Max { field } => ops.push(AggregateOpSpec::Max {
                        field: field.clone(),
                    }),
                }
            }

            Some(Self {
                ops,
                group_by: group_by.clone(),
                time_bucket: time_bucket.clone(),
            })
        } else {
            None
        }
    }

    /// Builds an `AggregatePlan` from a `QueryCommand` if any aggregates are present
    pub fn from_query_command(query: &QueryCommand) -> Option<Self> {
        let Some(aggs_vec) = query.aggs.as_ref() else {
            return None;
        };

        let mut ops: Vec<AggregateOpSpec> = Vec::with_capacity(aggs_vec.len());
        for spec in aggs_vec {
            match spec {
                AggSpec::Count { unique_field } => match unique_field {
                    Some(f) => ops.push(AggregateOpSpec::CountUnique { field: f.clone() }),
                    None => ops.push(AggregateOpSpec::CountAll),
                },
                AggSpec::CountField { field } => ops.push(AggregateOpSpec::CountField {
                    field: field.clone(),
                }),
                AggSpec::Total { field } => ops.push(AggregateOpSpec::Total {
                    field: field.clone(),
                }),
                AggSpec::Avg { field } => ops.push(AggregateOpSpec::Avg {
                    field: field.clone(),
                }),
                AggSpec::Min { field } => ops.push(AggregateOpSpec::Min {
                    field: field.clone(),
                }),
                AggSpec::Max { field } => ops.push(AggregateOpSpec::Max {
                    field: field.clone(),
                }),
            }
        }

        Some(Self {
            ops,
            group_by: query.group_by.clone(),
            time_bucket: query.time_bucket.clone(),
        })
    }
}
