use crate::command::types::TimeGranularity;
use crate::engine::core::read::aggregate::plan::AggregateOpSpec;
use serde_json::Value;
use std::collections::{HashMap, hash_map::Entry};

#[derive(Debug, Clone)]
pub struct ColumnSpec {
    pub name: String,
    pub logical_type: String,
}

#[derive(Debug, Clone)]
pub struct ResultTable {
    pub columns: Vec<ColumnSpec>,
    pub rows: Vec<Vec<Value>>, // flat rows aligned to columns
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct SelectionResult {
    pub columns: Vec<ColumnSpec>,
    pub rows: Vec<Vec<Value>>, // pre-shaped selection rows
}

#[derive(Debug, Clone)]
pub struct AggregateResult {
    pub group_by: Option<Vec<String>>,
    pub time_bucket: Option<TimeGranularity>,
    pub specs: Vec<AggregateOpSpec>,
    pub groups:
        HashMap<super::aggregate::partial::GroupKey, Vec<super::aggregate::partial::AggState>>,
}

#[derive(Debug, Clone)]
pub enum QueryResult {
    Selection(SelectionResult),
    Aggregation(AggregateResult),
}

impl SelectionResult {
    pub fn merge(&mut self, other: SelectionResult) {
        // Move rows from the other selection into self to avoid cloning
        self.rows.extend(other.rows);
    }

    pub fn finalize(self) -> ResultTable {
        ResultTable {
            columns: self.columns,
            rows: self.rows,
            metadata: HashMap::new(),
        }
    }
}

impl AggregateResult {
    pub fn merge(&mut self, other: AggregateResult) {
        // Move groups and states from the other aggregate into self to avoid cloning
        for (k, v) in other.groups {
            match self.groups.entry(k) {
                Entry::Vacant(e) => {
                    e.insert(v);
                }
                Entry::Occupied(mut e) => {
                    let entry = e.get_mut();
                    if entry.len() == v.len() {
                        for (a, b) in entry.iter_mut().zip(v.into_iter()) {
                            a.merge(&b);
                        }
                    }
                }
            }
        }
    }

    pub fn finalize(self) -> ResultTable {
        // Build columns: bucket?, group_by..., metrics...
        let mut columns: Vec<ColumnSpec> = Vec::new();
        if self.time_bucket.is_some() {
            columns.push(ColumnSpec {
                name: "bucket".to_string(),
                logical_type: "Timestamp".to_string(),
            });
        }
        if let Some(gb) = &self.group_by {
            for g in gb {
                columns.push(ColumnSpec {
                    name: g.clone(),
                    logical_type: "String".to_string(),
                });
            }
        }
        for spec in &self.specs {
            match spec {
                AggregateOpSpec::CountAll => columns.push(ColumnSpec {
                    name: "count".to_string(),
                    logical_type: "Integer".to_string(),
                }),
                AggregateOpSpec::CountField { field } => columns.push(ColumnSpec {
                    name: format!("count_{}", field),
                    logical_type: "Integer".to_string(),
                }),
                AggregateOpSpec::CountUnique { field } => columns.push(ColumnSpec {
                    name: format!("count_unique_{}", field),
                    logical_type: "Integer".to_string(),
                }),
                AggregateOpSpec::Total { field } => columns.push(ColumnSpec {
                    name: format!("total_{}", field),
                    logical_type: "Integer".to_string(),
                }),
                AggregateOpSpec::Avg { field } => columns.push(ColumnSpec {
                    name: format!("avg_{}", field),
                    logical_type: "Float".to_string(),
                }),
                AggregateOpSpec::Min { field } => columns.push(ColumnSpec {
                    name: format!("min_{}", field),
                    logical_type: "String".to_string(),
                }),
                AggregateOpSpec::Max { field } => columns.push(ColumnSpec {
                    name: format!("max_{}", field),
                    logical_type: "String".to_string(),
                }),
            }
        }

        let mut rows: Vec<Vec<Value>> = Vec::with_capacity(self.groups.len());
        for (k, states) in self.groups.into_iter() {
            let mut row: Vec<Value> = Vec::with_capacity(columns.len());
            if let Some(b) = k.bucket {
                row.push(Value::from(b as i64));
            }
            if let Some(gb) = &self.group_by {
                for (i, _name) in gb.iter().enumerate() {
                    let v = k
                        .groups
                        .get(i)
                        .map(|s| Value::from(s.clone()))
                        .unwrap_or(Value::from(""));
                    row.push(v);
                }
            }
            for (spec, st) in self.specs.iter().zip(states.into_iter()) {
                match (spec, st) {
                    (
                        AggregateOpSpec::CountAll,
                        super::aggregate::partial::AggState::CountAll { count },
                    ) => row.push(Value::from(count)),
                    (
                        AggregateOpSpec::CountField { .. },
                        super::aggregate::partial::AggState::CountAll { count },
                    ) => row.push(Value::from(count)),
                    (
                        AggregateOpSpec::CountUnique { .. },
                        super::aggregate::partial::AggState::CountUnique { values },
                    ) => row.push(Value::from(values.len() as i64)),
                    (
                        AggregateOpSpec::Total { .. },
                        super::aggregate::partial::AggState::Sum { sum },
                    ) => row.push(Value::from(sum)),
                    (
                        AggregateOpSpec::Avg { .. },
                        super::aggregate::partial::AggState::Avg { sum, count },
                    ) => {
                        let avg = if count == 0 {
                            0.0
                        } else {
                            (sum as f64) / (count as f64)
                        };
                        row.push(Value::from(avg));
                    }
                    (
                        AggregateOpSpec::Min { .. },
                        super::aggregate::partial::AggState::Min { min_num, min_str },
                    ) => {
                        if let Some(n) = min_num {
                            row.push(Value::from(n));
                        } else if let Some(s) = min_str {
                            row.push(Value::from(s));
                        } else {
                            row.push(Value::from(""));
                        }
                    }
                    (
                        AggregateOpSpec::Max { .. },
                        super::aggregate::partial::AggState::Max { max_num, max_str },
                    ) => {
                        if let Some(n) = max_num {
                            row.push(Value::from(n));
                        } else if let Some(s) = max_str {
                            row.push(Value::from(s));
                        } else {
                            row.push(Value::from(""));
                        }
                    }
                    _ => row.push(Value::from(Value::Null)),
                }
            }
            rows.push(row);
        }

        ResultTable {
            columns,
            rows,
            metadata: HashMap::new(),
        }
    }
}

impl QueryResult {
    pub fn merge(&mut self, other: QueryResult) {
        match (self, other) {
            (QueryResult::Selection(a), QueryResult::Selection(b)) => a.merge(b),
            (QueryResult::Aggregation(a), QueryResult::Aggregation(b)) => a.merge(b),
            _ => {
                // If result types do not match, drop the other
            }
        }
    }

    pub fn finalize_table(self) -> ResultTable {
        match self {
            QueryResult::Selection(s) => s.finalize(),
            QueryResult::Aggregation(a) => a.finalize(),
        }
    }
}
