use serde_json::Value;

use crate::engine::core::read::result::{ColumnSpec, SelectionResult};
use crate::test_helpers::factories::ColumnSpecFactory;

pub struct SelectionResultFactory {
    columns: Vec<ColumnSpec>,
    rows: Vec<Vec<Value>>,
}

impl SelectionResultFactory {
    pub fn new() -> Self {
        Self {
            columns: vec![ColumnSpecFactory::string("context_id")],
            rows: Vec::new(),
        }
    }

    pub fn with_columns(mut self, columns: Vec<ColumnSpec>) -> Self {
        self.columns = columns;
        self
    }

    pub fn with_row(mut self, row: Vec<Value>) -> Self {
        self.rows.push(row);
        self
    }

    pub fn with_rows(mut self, rows: Vec<Vec<Value>>) -> Self {
        self.rows = rows;
        self
    }

    pub fn simple(columns: &[(&str, &str)], rows: &[&[Value]]) -> SelectionResult {
        let cols: Vec<ColumnSpec> = columns
            .iter()
            .map(|(n, t)| match *t {
                "String" => ColumnSpecFactory::string(n),
                "Integer" => ColumnSpecFactory::integer(n),
                "Float" => ColumnSpecFactory::float(n),
                "Timestamp" => ColumnSpecFactory::timestamp(n),
                other => ColumnSpec {
                    name: (*n).to_string(),
                    logical_type: other.to_string(),
                },
            })
            .collect();
        let rs: Vec<Vec<Value>> = rows.iter().map(|r| r.to_vec()).collect();
        SelectionResult {
            columns: cols,
            rows: rs,
        }
    }

    pub fn create(self) -> SelectionResult {
        SelectionResult {
            columns: self.columns,
            rows: self.rows,
        }
    }
}
