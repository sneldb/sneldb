use crate::command::types::CompareOp as CommandCompareOp;
use crate::command::types::Expr;
use std::collections::HashMap;
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

    /// Batch evaluation: returns a boolean mask for all rows in the zone.
    /// Default implementation falls back to per-row evaluate_at.
    fn evaluate_mask(&self, accessor: &dyn FieldAccessor) -> Vec<bool> {
        let n = accessor.event_count();
        (0..n).map(|i| self.evaluate_at(accessor, i)).collect()
    }

    /// In-place batch evaluation that ANDs this condition into the provided mask.
    /// Default implementation allocates a temporary mask and merges it.
    fn evaluate_mask_into(&self, accessor: &dyn FieldAccessor, out: &mut [bool]) {
        let tmp = self.evaluate_mask(accessor);
        for (i, &v) in tmp.iter().enumerate() {
            out[i] = out[i] && v;
        }
    }

    /// In-place batch evaluation into a bitset mask (AND semantics).
    /// Default implementation builds a temporary boolean mask and folds it into the bitset.
    fn evaluate_mask_bits_into(&self, accessor: &dyn FieldAccessor, out: &mut BitMask) {
        let tmp = self.evaluate_mask(accessor);
        let mut cond_bits = BitMask::new_zeros(accessor.event_count());
        for (i, v) in tmp.iter().enumerate() {
            if *v {
                cond_bits.set(i);
            }
        }
        out.and_assign(&cond_bits);
    }
}

/// Provides indexed access to field values for a candidate zone.
pub trait FieldAccessor {
    fn get_str_at(&self, field: &str, index: usize) -> Option<&str>;
    fn get_i64_at(&self, field: &str, index: usize) -> Option<i64>;
    fn event_count(&self) -> usize;
    fn get_str_column(&self, field: &str) -> Option<&[String]>;
    fn get_i64_column_owned(&self, field: &str) -> Option<Vec<i64>>;
    fn get_i64_column(&self, field: &str) -> Option<&[i64]>;
}

/// A concrete accessor over a zone's columnar values that lazily builds
/// per-column numeric caches to avoid repeated string parsing.
pub struct PreparedAccessor<'a> {
    columns: &'a HashMap<String, crate::engine::core::column::column_values::ColumnValues>,
    event_count: usize,
    numeric_columns: HashMap<String, Vec<i64>>,
}

impl<'a> PreparedAccessor<'a> {
    pub fn new(columns: &'a HashMap<String, crate::engine::core::column::column_values::ColumnValues>) -> Self {
        let event_count = columns.values().next().map(|v| v.len()).unwrap_or(0);
        // Build numeric cache once per zone
        let mut numeric_columns: HashMap<String, Vec<i64>> = HashMap::new();
        for (field, col) in columns.iter() {
            let mut parsed: Vec<i64> = Vec::with_capacity(col.len());
            let mut ok = true;
            for s in col.iter() {
                if let Ok(v) = s.parse::<i64>() {
                    parsed.push(v);
                } else {
                    ok = false;
                    break;
                }
            }
            if ok {
                numeric_columns.insert(field.clone(), parsed);
            }
        }
        Self {
            columns,
            event_count,
            numeric_columns,
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

    fn event_count(&self) -> usize {
        self.event_count
    }

    fn get_str_column(&self, field: &str) -> Option<&[String]> {
        self.columns.get(field).map(|v| v.as_slice())
    }

    fn get_i64_column_owned(&self, field: &str) -> Option<Vec<i64>> {
        self.columns.get(field).map(|col| {
            col.iter()
                .map(|s| s.parse::<i64>().ok().unwrap_or_default())
                .collect::<Vec<i64>>()
        })
    }

    fn get_i64_column(&self, field: &str) -> Option<&[i64]> {
        self.numeric_columns.get(field).map(|v| v.as_slice())
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
        if let Some(num) = accessor.get_i64_at(&self.field, index) {
            match self.operation {
                CompareOp::Gt => num > self.value,
                CompareOp::Gte => num >= self.value,
                CompareOp::Lt => num < self.value,
                CompareOp::Lte => num <= self.value,
                CompareOp::Eq => num == self.value,
                CompareOp::Neq => num != self.value,
            }
        } else {
            false
        }
    }

    fn evaluate_mask(&self, accessor: &dyn FieldAccessor) -> Vec<bool> {
        if let Some(col) = accessor.get_i64_column(&self.field) {
            match self.operation {
                CompareOp::Gt => col.iter().map(|&v| v > self.value).collect(),
                CompareOp::Gte => col.iter().map(|&v| v >= self.value).collect(),
                CompareOp::Lt => col.iter().map(|&v| v < self.value).collect(),
                CompareOp::Lte => col.iter().map(|&v| v <= self.value).collect(),
                CompareOp::Eq => col.iter().map(|&v| v == self.value).collect(),
                CompareOp::Neq => col.iter().map(|&v| v != self.value).collect(),
            }
        } else {
            vec![false; accessor.event_count()]
        }
    }

    fn evaluate_mask_into(&self, accessor: &dyn FieldAccessor, out: &mut [bool]) {
        if let Some(col) = accessor.get_i64_column(&self.field) {
            match self.operation {
                CompareOp::Gt => {
                    for (i, &v) in col.iter().enumerate() {
                        out[i] = out[i] && (v > self.value);
                    }
                }
                CompareOp::Gte => {
                    for (i, &v) in col.iter().enumerate() {
                        out[i] = out[i] && (v >= self.value);
                    }
                }
                CompareOp::Lt => {
                    for (i, &v) in col.iter().enumerate() {
                        out[i] = out[i] && (v < self.value);
                    }
                }
                CompareOp::Lte => {
                    for (i, &v) in col.iter().enumerate() {
                        out[i] = out[i] && (v <= self.value);
                    }
                }
                CompareOp::Eq => {
                    for (i, &v) in col.iter().enumerate() {
                        out[i] = out[i] && (v == self.value);
                    }
                }
                CompareOp::Neq => {
                    for (i, &v) in col.iter().enumerate() {
                        out[i] = out[i] && (v != self.value);
                    }
                }
            }
        } else {
            for i in 0..out.len() {
                if out[i] {
                    out[i] = out[i] && self.evaluate_at(accessor, i);
                }
            }
        }
    }

    fn evaluate_mask_bits_into(&self, accessor: &dyn FieldAccessor, out: &mut BitMask) {
        if let Some(col) = accessor.get_i64_column(&self.field) {
            match self.operation {
                CompareOp::Gt => {
                    for (i, &v) in col.iter().enumerate() {
                        if !(v > self.value) {
                            out.clear(i);
                        }
                    }
                }
                CompareOp::Gte => {
                    for (i, &v) in col.iter().enumerate() {
                        if !(v >= self.value) {
                            out.clear(i);
                        }
                    }
                }
                CompareOp::Lt => {
                    for (i, &v) in col.iter().enumerate() {
                        if !(v < self.value) {
                            out.clear(i);
                        }
                    }
                }
                CompareOp::Lte => {
                    for (i, &v) in col.iter().enumerate() {
                        if !(v <= self.value) {
                            out.clear(i);
                        }
                    }
                }
                CompareOp::Eq => {
                    for (i, &v) in col.iter().enumerate() {
                        if !(v == self.value) {
                            out.clear(i);
                        }
                    }
                }
                CompareOp::Neq => {
                    for (i, &v) in col.iter().enumerate() {
                        if !(v != self.value) {
                            out.clear(i);
                        }
                    }
                }
            }
        } else {
            // Fallback to per-row lookup
            let n = accessor.event_count();
            for i in 0..n {
                if out.test(i) && !self.evaluate_at(accessor, i) {
                    out.clear(i);
                }
            }
        }
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
                _ => false,
            }
        } else {
            false
        }
    }

    fn evaluate_mask(&self, accessor: &dyn FieldAccessor) -> Vec<bool> {
        if let Some(col) = accessor.get_str_column(&self.field) {
            match self.operation {
                CompareOp::Eq => col.iter().map(|v| v == &self.value).collect(),
                CompareOp::Neq => col.iter().map(|v| v != &self.value).collect(),
                _ => vec![false; accessor.event_count()],
            }
        } else {
            vec![false; accessor.event_count()]
        }
    }

    fn evaluate_mask_into(&self, accessor: &dyn FieldAccessor, out: &mut [bool]) {
        if let Some(col) = accessor.get_str_column(&self.field) {
            match self.operation {
                CompareOp::Eq => {
                    for (i, v) in col.iter().enumerate() {
                        out[i] = out[i] && (v == &self.value);
                    }
                }
                CompareOp::Neq => {
                    for (i, v) in col.iter().enumerate() {
                        out[i] = out[i] && (v != &self.value);
                    }
                }
                _ => {
                    for i in 0..out.len() {
                        out[i] = false;
                    }
                }
            }
        } else {
            for i in 0..out.len() {
                out[i] = false;
            }
        }
    }

    fn evaluate_mask_bits_into(&self, accessor: &dyn FieldAccessor, out: &mut BitMask) {
        if let Some(col) = accessor.get_str_column(&self.field) {
            match self.operation {
                CompareOp::Eq => {
                    for (i, v) in col.iter().enumerate() {
                        if !(v == &self.value) {
                            out.clear(i);
                        }
                    }
                }
                CompareOp::Neq => {
                    for (i, v) in col.iter().enumerate() {
                        if !(v != &self.value) {
                            out.clear(i);
                        }
                    }
                }
                _ => {
                    // No string ops supported -> clear all
                    out.fill_zeros();
                }
            }
        } else {
            out.fill_zeros();
        }
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

    fn evaluate_mask(&self, accessor: &dyn FieldAccessor) -> Vec<bool> {
        let n = accessor.event_count();
        match self.operation {
            LogicalOp::And => {
                let mut mask = vec![true; n];
                for c in &self.conditions {
                    let cm = c.evaluate_mask(accessor);
                    for i in 0..n {
                        mask[i] = mask[i] && cm[i];
                    }
                    if !mask.iter().any(|&b| b) {
                        break;
                    }
                }
                mask
            }
            LogicalOp::Or => {
                let mut mask = vec![false; n];
                for c in &self.conditions {
                    let cm = c.evaluate_mask(accessor);
                    for i in 0..n {
                        mask[i] = mask[i] || cm[i];
                    }
                    if mask.iter().all(|&b| b) {
                        break;
                    }
                }
                mask
            }
            LogicalOp::Not => {
                let mut cm = self.conditions[0].evaluate_mask(accessor);
                for b in &mut cm {
                    *b = !*b;
                }
                cm
            }
        }
    }

    fn evaluate_mask_into(&self, accessor: &dyn FieldAccessor, out: &mut [bool]) {
        let n = accessor.event_count();
        match self.operation {
            LogicalOp::And => {
                for c in &self.conditions {
                    c.evaluate_mask_into(accessor, out);
                    if !out.iter().any(|&b| b) {
                        break;
                    }
                }
            }
            LogicalOp::Or => {
                let mut tmp = vec![false; n];
                for c in &self.conditions {
                    // fill tmp with child mask
                    let child = c.evaluate_mask(accessor);
                    for i in 0..n {
                        tmp[i] = child[i];
                    }
                    // OR into out
                    for i in 0..n {
                        out[i] = out[i] || tmp[i];
                    }
                    if out.iter().all(|&b| b) {
                        break;
                    }
                }
            }
            LogicalOp::Not => {
                let child = self.conditions[0].evaluate_mask(accessor);
                for i in 0..n {
                    out[i] = out[i] && !child[i];
                }
            }
        }
    }

    fn evaluate_mask_bits_into(&self, accessor: &dyn FieldAccessor, out: &mut BitMask) {
        let n = accessor.event_count();
        match self.operation {
            LogicalOp::And => {
                for c in &self.conditions {
                    c.evaluate_mask_bits_into(accessor, out);
                    if !out.any() {
                        break;
                    }
                }
            }
            LogicalOp::Or => {
                let mut accum = BitMask::new_zeros(n);
                for c in &self.conditions {
                    let mut child = BitMask::new_ones(n);
                    c.evaluate_mask_bits_into(accessor, &mut child);
                    accum.or_assign(&child);
                    if accum.all() {
                        break;
                    }
                }
                out.and_assign(&accum);
            }
            LogicalOp::Not => {
                let mut child = BitMask::new_ones(n);
                self.conditions[0].evaluate_mask_bits_into(accessor, &mut child);
                child.not_inplace();
                out.and_assign(&child);
            }
        }
    }
}

/// Compact bitset for row-selection masks
#[derive(Clone, Debug)]
pub struct BitMask {
    lanes: Vec<u64>,
    len: usize,
}

impl BitMask {
    pub fn new_ones(len: usize) -> Self {
        let lanes_len = (len + 63) / 64;
        let lanes = vec![!0u64; lanes_len];
        let mut bm = Self { lanes, len };
        bm.mask_tail();
        bm
    }

    pub fn new_zeros(len: usize) -> Self {
        Self {
            lanes: vec![0u64; (len + 63) / 64],
            len,
        }
    }

    #[inline]
    pub fn clear(&mut self, index: usize) {
        let lane = index >> 6;
        let bit = index & 63;
        self.lanes[lane] &= !(1u64 << bit);
    }

    #[inline]
    pub fn set(&mut self, index: usize) {
        let lane = index >> 6;
        let bit = index & 63;
        self.lanes[lane] |= 1u64 << bit;
    }

    #[inline]
    pub fn test(&self, index: usize) -> bool {
        let lane = index >> 6;
        let bit = index & 63;
        (self.lanes[lane] & (1u64 << bit)) != 0
    }

    pub fn and_assign(&mut self, other: &BitMask) {
        for (a, b) in self.lanes.iter_mut().zip(other.lanes.iter()) {
            *a &= *b;
        }
        self.mask_tail();
    }

    pub fn or_assign(&mut self, other: &BitMask) {
        for (a, b) in self.lanes.iter_mut().zip(other.lanes.iter()) {
            *a |= *b;
        }
        self.mask_tail();
    }

    pub fn not_inplace(&mut self) {
        for a in self.lanes.iter_mut() {
            *a = !*a;
        }
        self.mask_tail();
    }

    #[inline]
    pub fn any(&self) -> bool {
        self.lanes.iter().any(|&x| x != 0)
    }

    pub fn all(&self) -> bool {
        if self.lanes.is_empty() {
            return true;
        }
        let full = self.lanes.len() - 1;
        if full > 0 && !self.lanes[..full].iter().all(|&x| x == !0u64) {
            return false;
        }
        let rem = self.len & 63;
        if rem == 0 {
            return self.lanes[full] == !0u64;
        }
        let mask = if rem == 64 { !0u64 } else { (1u64 << rem) - 1 };
        (self.lanes[full] & mask) == mask
    }

    pub fn fill_zeros(&mut self) {
        for a in self.lanes.iter_mut() {
            *a = 0;
        }
    }

    pub fn collect_ones(&self) -> Vec<usize> {
        let mut out = Vec::new();
        for (lane_idx, &lane) in self.lanes.iter().enumerate() {
            let mut bits = lane;
            while bits != 0 {
                let tz = bits.trailing_zeros() as usize;
                let idx = (lane_idx << 6) + tz;
                if idx < self.len {
                    out.push(idx);
                }
                bits &= bits - 1;
            }
        }
        out
    }

    fn mask_tail(&mut self) {
        if self.lanes.is_empty() {
            return;
        }
        let rem = self.len & 63;
        if rem == 0 {
            return;
        }
        let last = self.lanes.len() - 1;
        let mask = (1u64 << rem) - 1;
        self.lanes[last] &= mask;
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
        }
    }
}
