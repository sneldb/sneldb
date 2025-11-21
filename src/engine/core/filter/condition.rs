use crate::command::types::CompareOp as CommandCompareOp;
use crate::command::types::Expr;
use crate::engine::core::column::column_values::ColumnValues;
use crate::engine::core::filter::direct_event_accessor::DirectEventAccessor;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;

/// Represents a condition that can be evaluated against zone values
pub trait Condition: Send + Sync + Debug {
    fn evaluate(&self, values: &HashMap<String, Vec<String>>) -> bool;

    /// Fast path evaluation against a zone by index without constructing
    /// per-event HashMaps. Implementors should override this for performance.
    fn evaluate_at(&self, _accessor: &dyn FieldAccessor, _index: usize) -> bool {
        // Default fallback (slow path): materialize a single-value map
        // for all available fields using the accessor. Since the accessor
        // does not expose iteration over fields, we return false by default.
        // All current implementors override this method.
        false
    }

    /// Fast path evaluation against a single event without materializing all fields.
    /// All implementors must override this for optimal memtable query performance.
    fn evaluate_event_direct(&self, accessor: &DirectEventAccessor) -> bool;

    /// Indicates whether this condition (or any of its descendants) requires numeric access.
    fn is_numeric(&self) -> bool {
        false
    }

    /// Collects the names of fields that require numeric access for this condition.
    fn collect_numeric_fields(&self, _out: &mut HashSet<String>) {}

    /// Used for downcasting to concrete condition types when needed for
    /// SIMD fast-paths or specialized handling.
    fn as_any(&self) -> &dyn Any;
}

/// Provides indexed access to field values for a candidate zone.
pub trait FieldAccessor {
    fn get_str_at(&self, field: &str, index: usize) -> Option<&str>;
    fn get_i64_at(&self, field: &str, index: usize) -> Option<i64>;
    fn get_u64_at(&self, field: &str, index: usize) -> Option<u64>;
    fn get_f64_at(&self, field: &str, index: usize) -> Option<f64>;
    fn event_count(&self) -> usize;
}

/// A concrete accessor over a zone's columnar values that lazily builds
/// per-column numeric caches to avoid repeated string parsing.
pub struct PreparedAccessor<'a> {
    columns: &'a HashMap<String, ColumnValues>,
    event_count: usize,
}

impl<'a> PreparedAccessor<'a> {
    pub fn new(columns: &'a HashMap<String, ColumnValues>) -> Self {
        let mut max_len = 0usize;
        let mut min_len = usize::MAX;
        let mut zero_column_total = 0usize;
        let mut zero_column_sample: Vec<String> = Vec::new();

        for (name, column) in columns.iter() {
            let len = column.len();
            if len > max_len {
                max_len = len;
            }
            if len < min_len {
                min_len = len;
            }
            if len == 0 {
                zero_column_total += 1;
                if zero_column_sample.len() < 5 {
                    zero_column_sample.push(name.clone());
                }
            }
        }

        if tracing::enabled!(tracing::Level::DEBUG)
            && !columns.is_empty()
            && ((min_len != usize::MAX && min_len != max_len) || zero_column_total > 0)
        {
            let min_len = if min_len == usize::MAX { 0 } else { min_len };
            tracing::debug!(
                target: "sneldb::prepared_accessor",
                zone_columns = columns.len(),
                max_len = max_len,
                min_len = min_len,
                zero_column_total = zero_column_total,
                zero_column_sample = ?zero_column_sample,
                "Derived event_count from sparse column lengths"
            );
        }

        let event_count = max_len;
        Self {
            columns,
            event_count,
        }
    }

    /// Prepares numeric caches for the specified columns so numeric conditions avoid repeated parsing.
    pub fn warm_numeric_cache(&self, fields: &HashSet<String>) {
        for field in fields {
            if let Some(column) = self.columns.get(field) {
                if !column.is_typed() {
                    column.warm_numeric_cache();
                }
            }
        }
    }

    /// Builds a dense i64 buffer and a parallel validity mask for a field.
    /// Invalid/missing entries are represented as any value in the buffer and false in validity.
    pub fn get_i64_buffer_with_validity(
        &self,
        field: &str,
        start: usize,
        end: usize,
    ) -> Option<(Vec<i64>, Vec<bool>)> {
        let column = self.columns.get(field)?;
        let end = end.min(self.event_count);
        if start >= end {
            return Some((Vec::new(), Vec::new()));
        }
        let mut values: Vec<i64> = Vec::with_capacity(end - start);
        let mut valid: Vec<bool> = Vec::with_capacity(end - start);
        for i in start..end {
            if let Some(v) = column.get_i64_at(i) {
                values.push(v);
                valid.push(true);
            } else {
                values.push(0);
                valid.push(false);
            }
        }
        Some((values, valid))
    }

    /// Builds a dense u64 buffer and a parallel validity mask for a field.
    pub fn get_u64_buffer_with_validity(
        &self,
        field: &str,
        start: usize,
        end: usize,
    ) -> Option<(Vec<u64>, Vec<bool>)> {
        let column = self.columns.get(field)?;
        let end = end.min(self.event_count);
        if start >= end {
            return Some((Vec::new(), Vec::new()));
        }
        let mut values: Vec<u64> = Vec::with_capacity(end - start);
        let mut valid: Vec<bool> = Vec::with_capacity(end - start);
        let mut any_valid = false;
        for i in start..end {
            if let Some(v) = column.get_u64_at(i) {
                values.push(v);
                valid.push(true);
                any_valid = true;
            } else {
                values.push(0);
                valid.push(false);
            }
        }
        if any_valid {
            Some((values, valid))
        } else {
            None
        }
    }

    /// Builds a dense f64 buffer and a parallel validity mask for a field.
    pub fn get_f64_buffer_with_validity(
        &self,
        field: &str,
        start: usize,
        end: usize,
    ) -> Option<(Vec<f64>, Vec<bool>)> {
        let column = self.columns.get(field)?;
        let end = end.min(self.event_count);
        if start >= end {
            return Some((Vec::new(), Vec::new()));
        }
        let mut values: Vec<f64> = Vec::with_capacity(end - start);
        let mut valid: Vec<bool> = Vec::with_capacity(end - start);
        let mut any_valid = false;
        for i in start..end {
            if let Some(v) = column.get_f64_at(i) {
                values.push(v);
                valid.push(true);
                any_valid = true;
            } else {
                values.push(0.0);
                valid.push(false);
            }
        }
        if any_valid {
            Some((values, valid))
        } else {
            None
        }
    }
}

impl<'a> FieldAccessor for PreparedAccessor<'a> {
    fn get_str_at(&self, field: &str, index: usize) -> Option<&str> {
        self.columns
            .get(field)
            .and_then(|col| col.get_str_at(index))
    }

    fn get_i64_at(&self, field: &str, index: usize) -> Option<i64> {
        self.columns
            .get(field)
            .and_then(|col| col.get_i64_at(index))
    }

    fn get_u64_at(&self, field: &str, index: usize) -> Option<u64> {
        self.columns
            .get(field)
            .and_then(|col| col.get_u64_at(index))
    }

    fn get_f64_at(&self, field: &str, index: usize) -> Option<f64> {
        self.columns
            .get(field)
            .and_then(|col| col.get_f64_at(index))
    }

    fn event_count(&self) -> usize {
        self.event_count
    }
}

impl<'a> PreparedAccessor<'a> {
    /// Convenience wrapper used by SIMD fast-paths.
    #[inline]
    pub fn get_i64_slice_with_validity(
        &self,
        field: &str,
        start: usize,
        end: usize,
    ) -> Option<(Vec<i64>, Vec<bool>)> {
        self.get_i64_buffer_with_validity(field, start, end)
    }

    /// Convenience wrapper used by SIMD fast-paths.
    #[inline]
    pub fn get_u64_slice_with_validity(
        &self,
        field: &str,
        start: usize,
        end: usize,
    ) -> Option<(Vec<u64>, Vec<bool>)> {
        self.get_u64_buffer_with_validity(field, start, end)
    }

    /// Convenience wrapper used by SIMD fast-paths.
    #[inline]
    pub fn get_f64_slice_with_validity(
        &self,
        field: &str,
        start: usize,
        end: usize,
    ) -> Option<(Vec<f64>, Vec<bool>)> {
        self.get_f64_buffer_with_validity(field, start, end)
    }
}

/// Numeric comparison condition
#[derive(Debug)]
pub struct NumericCondition {
    field: String,
    operation: CompareOp,
    value: i64,
}

impl NumericCondition {
    pub fn new(field: String, operation: CompareOp, value: i64) -> Self {
        Self {
            field,
            operation,
            value,
        }
    }

    #[inline]
    pub fn field(&self) -> &str {
        &self.field
    }

    #[inline]
    pub fn op(&self) -> CompareOp {
        self.operation
    }

    #[inline]
    pub fn value(&self) -> i64 {
        self.value
    }

    #[inline]
    pub fn evaluate_scalar(&self, lhs: i64) -> bool {
        match self.operation {
            CompareOp::Gt => lhs > self.value,
            CompareOp::Gte => lhs >= self.value,
            CompareOp::Lt => lhs < self.value,
            CompareOp::Lte => lhs <= self.value,
            CompareOp::Eq => lhs == self.value,
            CompareOp::Neq => lhs != self.value,
            CompareOp::In => {
                // IN operation should use InNumericCondition, not NumericCondition
                unreachable!("IN operation should not be used with NumericCondition")
            }
        }
    }
}

impl Condition for NumericCondition {
    fn evaluate(&self, values: &HashMap<String, Vec<String>>) -> bool {
        if let Some(field_values) = values.get(&self.field) {
            // For now, we'll check if any value in the zone matches the condition
            field_values.iter().any(|v| {
                if let Ok(num) = v.parse::<i64>() {
                    match self.operation {
                        CompareOp::Gt => num > self.value,
                        CompareOp::Gte => num >= self.value,
                        CompareOp::Lt => num < self.value,
                        CompareOp::Lte => num <= self.value,
                        CompareOp::Eq => num == self.value,
                        CompareOp::Neq => num != self.value,
                        CompareOp::In => {
                            // IN operation should use InNumericCondition, not NumericCondition
                            unreachable!("IN operation should not be used with NumericCondition")
                        }
                    }
                } else {
                    false
                }
            })
        } else {
            false
        }
    }

    fn evaluate_at(&self, accessor: &dyn FieldAccessor, index: usize) -> bool {
        // Prefer u64 for performance and to match common schemas (e.g., id)
        if let Some(u) = accessor.get_u64_at(&self.field, index) {
            let rhs = if self.value < 0 {
                return false;
            } else {
                self.value as u64
            };
            return match self.operation {
                CompareOp::Gt => u > rhs,
                CompareOp::Gte => u >= rhs,
                CompareOp::Lt => u < rhs,
                CompareOp::Lte => u <= rhs,
                CompareOp::Eq => u == rhs,
                CompareOp::Neq => u != rhs,
                CompareOp::In => {
                    // IN operation should use InNumericCondition, not NumericCondition
                    unreachable!("IN operation should not be used with NumericCondition")
                }
            };
        }
        if let Some(num) = accessor.get_i64_at(&self.field, index) {
            return match self.operation {
                CompareOp::Gt => num > self.value,
                CompareOp::Gte => num >= self.value,
                CompareOp::Lt => num < self.value,
                CompareOp::Lte => num <= self.value,
                CompareOp::Eq => num == self.value,
                CompareOp::Neq => num != self.value,
                CompareOp::In => {
                    // IN operation should use InNumericCondition, not NumericCondition
                    unreachable!("IN operation should not be used with NumericCondition")
                }
            };
        }
        if let Some(f) = accessor.get_f64_at(&self.field, index) {
            let rhs = self.value as f64;
            return match self.operation {
                CompareOp::Gt => f > rhs,
                CompareOp::Gte => f >= rhs,
                CompareOp::Lt => f < rhs,
                CompareOp::Lte => f <= rhs,
                CompareOp::Eq => f == rhs,
                CompareOp::Neq => f != rhs,
                CompareOp::In => {
                    // IN operation should use InNumericCondition, not NumericCondition
                    unreachable!("IN operation should not be used with NumericCondition")
                }
            };
        }
        false
    }

    fn evaluate_event_direct(&self, accessor: &DirectEventAccessor) -> bool {
        if let Some(num) = accessor.get_field_as_i64(&self.field) {
            match self.operation {
                CompareOp::Gt => num > self.value,
                CompareOp::Gte => num >= self.value,
                CompareOp::Lt => num < self.value,
                CompareOp::Lte => num <= self.value,
                CompareOp::Eq => num == self.value,
                CompareOp::Neq => num != self.value,
                CompareOp::In => {
                    // IN operation should use InNumericCondition, not NumericCondition
                    unreachable!("IN operation should not be used with NumericCondition")
                }
            }
        } else {
            false
        }
    }

    fn is_numeric(&self) -> bool {
        true
    }

    fn collect_numeric_fields(&self, out: &mut HashSet<String>) {
        out.insert(self.field.clone());
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// String comparison condition
#[derive(Debug)]
pub struct StringCondition {
    field: String,
    operation: CompareOp,
    value: String,
}

impl StringCondition {
    pub fn new(field: String, operation: CompareOp, value: String) -> Self {
        Self {
            field,
            operation,
            value,
        }
    }
}

impl Condition for StringCondition {
    fn evaluate(&self, values: &HashMap<String, Vec<String>>) -> bool {
        if let Some(field_values) = values.get(&self.field) {
            field_values.iter().any(|v| match self.operation {
                CompareOp::Eq => v == &self.value,
                CompareOp::Neq => v != &self.value,
                CompareOp::In => {
                    // IN operation should use InStringCondition, not StringCondition
                    unreachable!("IN operation should not be used with StringCondition")
                }
                _ => false, // Other operations don't make sense for strings
            })
        } else {
            false
        }
    }

    fn evaluate_at(&self, accessor: &dyn FieldAccessor, index: usize) -> bool {
        if let Some(val) = accessor.get_str_at(&self.field, index) {
            match self.operation {
                CompareOp::Eq => val == self.value,
                CompareOp::Neq => val != self.value,
                CompareOp::In => {
                    // IN operation should use InStringCondition, not StringCondition
                    unreachable!("IN operation should not be used with StringCondition")
                }
                _ => false,
            }
        } else {
            false
        }
    }

    fn evaluate_event_direct(&self, accessor: &DirectEventAccessor) -> bool {
        let val = accessor.get_field_value(&self.field);
        match self.operation {
            CompareOp::Eq => val == self.value,
            CompareOp::Neq => val != self.value,
            CompareOp::In => {
                // IN operation should use InStringCondition, not StringCondition
                unreachable!("IN operation should not be used with StringCondition")
            }
            _ => false,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// IN condition for numeric values (uses HashSet for efficient lookups)
#[derive(Debug)]
pub struct InNumericCondition {
    field: String,
    values: HashSet<i64>,
}

impl InNumericCondition {
    pub fn new(field: String, values: Vec<i64>) -> Self {
        Self {
            field,
            values: values.into_iter().collect(),
        }
    }

    #[inline]
    pub fn field(&self) -> &str {
        &self.field
    }

    #[inline]
    pub fn contains(&self, value: i64) -> bool {
        self.values.contains(&value)
    }
}

impl Condition for InNumericCondition {
    fn evaluate(&self, values: &HashMap<String, Vec<String>>) -> bool {
        if let Some(field_values) = values.get(&self.field) {
            field_values.iter().any(|v| {
                if let Ok(num) = v.parse::<i64>() {
                    self.values.contains(&num)
                } else {
                    false
                }
            })
        } else {
            false
        }
    }

    fn evaluate_at(&self, accessor: &dyn FieldAccessor, index: usize) -> bool {
        // Try u64 first for performance
        if let Some(u) = accessor.get_u64_at(&self.field, index) {
            // Check if u64 value can fit in i64 and is in the set
            if u <= i64::MAX as u64 {
                return self.values.contains(&(u as i64));
            }
            return false;
        }
        // Try i64
        if let Some(num) = accessor.get_i64_at(&self.field, index) {
            return self.values.contains(&num);
        }
        // Try f64 (with conversion)
        if let Some(f) = accessor.get_f64_at(&self.field, index) {
            let rounded = f.round() as i64;
            if (f - rounded as f64).abs() < f64::EPSILON {
                return self.values.contains(&rounded);
            }
        }
        false
    }

    fn evaluate_event_direct(&self, accessor: &DirectEventAccessor) -> bool {
        if let Some(num) = accessor.get_field_as_i64(&self.field) {
            self.values.contains(&num)
        } else {
            false
        }
    }

    fn is_numeric(&self) -> bool {
        true
    }

    fn collect_numeric_fields(&self, out: &mut HashSet<String>) {
        out.insert(self.field.clone());
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// IN condition for string values (uses HashSet for efficient lookups)
#[derive(Debug)]
pub struct InStringCondition {
    field: String,
    values: HashSet<String>,
}

impl InStringCondition {
    pub fn new(field: String, values: Vec<String>) -> Self {
        Self {
            field,
            values: values.into_iter().collect(),
        }
    }

    #[inline]
    pub fn field(&self) -> &str {
        &self.field
    }

    #[inline]
    pub fn contains(&self, value: &str) -> bool {
        self.values.contains(value)
    }
}

impl Condition for InStringCondition {
    fn evaluate(&self, values: &HashMap<String, Vec<String>>) -> bool {
        if let Some(field_values) = values.get(&self.field) {
            field_values.iter().any(|v| self.values.contains(v))
        } else {
            false
        }
    }

    fn evaluate_at(&self, accessor: &dyn FieldAccessor, index: usize) -> bool {
        if let Some(val) = accessor.get_str_at(&self.field, index) {
            self.values.contains(val)
        } else {
            false
        }
    }

    fn evaluate_event_direct(&self, accessor: &DirectEventAccessor) -> bool {
        let val = accessor.get_field_value(&self.field);
        self.values.contains(&val)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Logical combination of conditions
#[derive(Debug)]
pub struct LogicalCondition {
    conditions: Vec<Box<dyn Condition>>,
    operation: LogicalOp,
}

impl LogicalCondition {
    pub fn new(conditions: Vec<Box<dyn Condition>>, operation: LogicalOp) -> Self {
        Self {
            conditions,
            operation,
        }
    }
}

impl Condition for LogicalCondition {
    fn evaluate(&self, values: &HashMap<String, Vec<String>>) -> bool {
        match self.operation {
            LogicalOp::And => self.conditions.iter().all(|c| c.evaluate(values)),
            LogicalOp::Or => self.conditions.iter().any(|c| c.evaluate(values)),
            LogicalOp::Not => !self.conditions[0].evaluate(values),
        }
    }

    fn evaluate_at(&self, accessor: &dyn FieldAccessor, index: usize) -> bool {
        match self.operation {
            LogicalOp::And => self
                .conditions
                .iter()
                .all(|c| c.evaluate_at(accessor, index)),
            LogicalOp::Or => self
                .conditions
                .iter()
                .any(|c| c.evaluate_at(accessor, index)),
            LogicalOp::Not => !self.conditions[0].evaluate_at(accessor, index),
        }
    }

    fn evaluate_event_direct(&self, accessor: &DirectEventAccessor) -> bool {
        match self.operation {
            LogicalOp::And => self
                .conditions
                .iter()
                .all(|c| c.evaluate_event_direct(accessor)),
            LogicalOp::Or => self
                .conditions
                .iter()
                .any(|c| c.evaluate_event_direct(accessor)),
            LogicalOp::Not => !self.conditions[0].evaluate_event_direct(accessor),
        }
    }

    fn is_numeric(&self) -> bool {
        self.conditions.iter().any(|c| c.is_numeric())
    }

    fn collect_numeric_fields(&self, out: &mut HashSet<String>) {
        for condition in &self.conditions {
            condition.collect_numeric_fields(out);
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Clone, Copy)]
pub enum CompareOp {
    Gt,
    Gte,
    Lt,
    Lte,
    Eq,
    Neq,
    In,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LogicalOp {
    And,
    Or,
    Not,
}

impl LogicalOp {
    /// Determines the logical operation from a query expression
    pub fn from_expr(expr: Option<&Expr>) -> Self {
        match expr {
            Some(Expr::And(_, _)) => LogicalOp::And,
            Some(Expr::Or(_, _)) => LogicalOp::Or,
            Some(Expr::Not(_)) => LogicalOp::Not,
            Some(Expr::Compare { .. }) => LogicalOp::And, // Single comparison is treated as AND
            Some(Expr::In { .. }) => LogicalOp::And,      // Single IN is treated as AND
            None => LogicalOp::And,                       // Default to AND if no where clause
        }
    }

    /// Determines if this operation requires special handling
    pub fn requires_special_handling(&self) -> bool {
        matches!(self, LogicalOp::Not)
    }
}

impl From<CommandCompareOp> for CompareOp {
    fn from(op: CommandCompareOp) -> Self {
        match op {
            CommandCompareOp::Gt => CompareOp::Gt,
            CommandCompareOp::Gte => CompareOp::Gte,
            CommandCompareOp::Lt => CompareOp::Lt,
            CommandCompareOp::Lte => CompareOp::Lte,
            CommandCompareOp::Eq => CompareOp::Eq,
            CommandCompareOp::Neq => CompareOp::Neq,
            CommandCompareOp::In => CompareOp::In,
        }
    }
}
