use std::collections::HashMap;

use crate::engine::core::Event;
use crate::engine::core::column::column_values::ColumnValues;

/// A sink that consumes matching rows or events produced by the execution path
pub trait ResultSink {
    /// Called for each matching row in a zone (columnar path)
    fn on_row(&mut self, _row_idx: usize, _columns: &HashMap<String, ColumnValues>) {}
    /// Called for each matching event from memtable (row path)
    fn on_event(&mut self, _event: &Event) {}
}
