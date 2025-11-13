use std::collections::{HashMap, HashSet};

use crate::engine::core::Event;
use crate::engine::core::column::column_values::ColumnValues;
use crate::engine::core::column::format::PhysicalType;
use crate::engine::core::read::aggregate::plan::AggregateOpSpec;
use crate::engine::types::ScalarValue;
use std::simd::Simd;
use std::simd::prelude::*;

const SIMD_LANES: usize = 4;

/// Finalized output of an aggregator
#[derive(Debug, Clone, PartialEq)]
pub enum AggOutput {
    Count(i64),
    CountUnique(usize),
    Sum(i64),
    Min(String),
    Max(String),
    Avg(f64),
}

/// Aggregator enum with concrete implementations per operation
#[derive(Debug, Clone, PartialEq)]
pub enum AggregatorImpl {
    CountAll(CountAll),
    CountField(CountField),
    CountUnique(CountUnique),
    Sum(Sum),
    Min(Min),
    Max(Max),
    Avg(Avg),
}

impl AggregatorImpl {
    pub fn from_spec(spec: &AggregateOpSpec) -> Self {
        match spec {
            AggregateOpSpec::CountAll => Self::CountAll(CountAll::new()),
            AggregateOpSpec::CountField { field } => {
                Self::CountField(CountField::new(field.clone()))
            }
            AggregateOpSpec::CountUnique { field } => {
                Self::CountUnique(CountUnique::new(field.clone()))
            }
            AggregateOpSpec::Total { field } => Self::Sum(Sum::new(field.clone())),
            AggregateOpSpec::Avg { field } => Self::Avg(Avg::new(field.clone())),
            AggregateOpSpec::Min { field } => Self::Min(Min::new(field.clone())),
            AggregateOpSpec::Max { field } => Self::Max(Max::new(field.clone())),
        }
    }

    pub fn field(&self) -> Option<&str> {
        match self {
            AggregatorImpl::CountAll(_) => None,
            AggregatorImpl::CountField(a) => Some(&a.field),
            AggregatorImpl::CountUnique(a) => Some(&a.field),
            AggregatorImpl::Sum(a) => Some(&a.field),
            AggregatorImpl::Min(a) => Some(&a.field),
            AggregatorImpl::Max(a) => Some(&a.field),
            AggregatorImpl::Avg(a) => Some(&a.field),
        }
    }

    /// Update aggregator with a matching row
    pub fn update(&mut self, row_idx: usize, columns: &HashMap<String, ColumnValues>) {
        match self {
            AggregatorImpl::CountAll(a) => a.update(),
            AggregatorImpl::CountField(a) => a.update(row_idx, columns),
            AggregatorImpl::CountUnique(a) => a.update(row_idx, columns),
            AggregatorImpl::Sum(a) => a.update(row_idx, columns),
            AggregatorImpl::Min(a) => a.update(row_idx, columns),
            AggregatorImpl::Max(a) => a.update(row_idx, columns),
            AggregatorImpl::Avg(a) => a.update(row_idx, columns),
        }
    }

    /// Update aggregator with a column slice (columnar processing path)
    /// This is optimized for SIMD operations when processing entire columns
    pub fn update_column(
        &mut self,
        start: usize,
        end: usize,
        columns: &HashMap<String, ColumnValues>,
    ) {
        match self {
            AggregatorImpl::CountAll(a) => {
                // Count all: just add the count
                a.count += (end - start) as i64;
            }
            AggregatorImpl::CountField(a) => {
                // Fallback to row-by-row for CountField
                for row_idx in start..end {
                    a.update(row_idx, columns);
                }
            }
            AggregatorImpl::CountUnique(a) => {
                // Fallback to row-by-row for CountUnique
                for row_idx in start..end {
                    a.update(row_idx, columns);
                }
            }
            AggregatorImpl::Sum(a) => {
                a.update_column_simd(start, end, columns);
            }
            AggregatorImpl::Min(a) => {
                // Fallback to row-by-row for Min (needs comparison)
                for row_idx in start..end {
                    a.update(row_idx, columns);
                }
            }
            AggregatorImpl::Max(a) => {
                // Fallback to row-by-row for Max (needs comparison)
                for row_idx in start..end {
                    a.update(row_idx, columns);
                }
            }
            AggregatorImpl::Avg(a) => {
                a.update_column_simd(start, end, columns);
            }
        }
    }

    /// Merge another aggregator of the same variant
    pub fn merge(&mut self, other: &AggregatorImpl) {
        match (self, other) {
            (AggregatorImpl::CountAll(a), AggregatorImpl::CountAll(b)) => a.merge(b),
            (AggregatorImpl::CountField(a), AggregatorImpl::CountField(b)) => a.merge(b),
            (AggregatorImpl::CountUnique(a), AggregatorImpl::CountUnique(b)) => a.merge(b),
            (AggregatorImpl::Sum(a), AggregatorImpl::Sum(b)) => a.merge(b),
            (AggregatorImpl::Min(a), AggregatorImpl::Min(b)) => a.merge(b),
            (AggregatorImpl::Max(a), AggregatorImpl::Max(b)) => a.merge(b),
            (AggregatorImpl::Avg(a), AggregatorImpl::Avg(b)) => a.merge(b),
            _ => {}
        }
    }

    pub fn finalize(&self) -> AggOutput {
        match self {
            AggregatorImpl::CountAll(a) => a.finalize(),
            AggregatorImpl::CountField(a) => a.finalize(),
            AggregatorImpl::CountUnique(a) => a.finalize(),
            AggregatorImpl::Sum(a) => a.finalize(),
            AggregatorImpl::Min(a) => a.finalize(),
            AggregatorImpl::Max(a) => a.finalize(),
            AggregatorImpl::Avg(a) => a.finalize(),
        }
    }

    /// Update aggregator using a row from a full `Event` (row-based path)
    /// Optimized to use get_field_scalar() to avoid string allocations
    pub fn update_from_event(&mut self, event: &Event) {
        match self {
            AggregatorImpl::CountAll(a) => a.update(),
            AggregatorImpl::CountField(a) => {
                let val = event.get_field_scalar(a.field.as_str());
                if !matches!(val, None | Some(ScalarValue::Null)) {
                    a.update_non_null();
                }
            }
            AggregatorImpl::CountUnique(a) => {
                let s = match a.field.as_str() {
                    "context_id" => event.context_id.clone(),
                    "event_type" => event.event_type.clone(),
                    "timestamp" => event.timestamp.to_string(),
                    other => {
                        // Use get_field_scalar to avoid string allocation when possible
                        match event.get_field_scalar(other) {
                            Some(ScalarValue::Utf8(s)) => s,
                            Some(ScalarValue::Int64(i)) => i.to_string(),
                            Some(ScalarValue::Float64(f)) => f.to_string(),
                            Some(ScalarValue::Boolean(b)) => b.to_string(),
                            Some(ScalarValue::Timestamp(ts)) => ts.to_string(),
                            _ => String::new(),
                        }
                    }
                };
                a.update_value_str(&s);
            }
            AggregatorImpl::Sum(a) => {
                let n = match a.field.as_str() {
                    "timestamp" => Some(event.timestamp as i64),
                    other => {
                        // Use get_field_scalar to avoid string allocation
                        match event.get_field_scalar(other) {
                            Some(ScalarValue::Int64(i)) => Some(i),
                            Some(ScalarValue::Timestamp(ts)) => Some(ts),
                            Some(ScalarValue::Utf8(s)) => s.parse::<i64>().ok(),
                            _ => None,
                        }
                    }
                };
                if let Some(v) = n {
                    a.update_value_i64(v);
                }
            }
            AggregatorImpl::Min(a) => {
                // Use get_field_scalar to avoid string allocation
                let s = match event.get_field_scalar(&a.field) {
                    Some(ScalarValue::Utf8(s)) => s,
                    Some(ScalarValue::Int64(i)) => i.to_string(),
                    Some(ScalarValue::Float64(f)) => f.to_string(),
                    Some(ScalarValue::Boolean(b)) => b.to_string(),
                    Some(ScalarValue::Timestamp(ts)) => ts.to_string(),
                    _ => String::new(),
                };
                a.update_value_str(&s);
            }
            AggregatorImpl::Max(a) => {
                // Use get_field_scalar to avoid string allocation
                let s = match event.get_field_scalar(&a.field) {
                    Some(ScalarValue::Utf8(s)) => s,
                    Some(ScalarValue::Int64(i)) => i.to_string(),
                    Some(ScalarValue::Float64(f)) => f.to_string(),
                    Some(ScalarValue::Boolean(b)) => b.to_string(),
                    Some(ScalarValue::Timestamp(ts)) => ts.to_string(),
                    _ => String::new(),
                };
                a.update_value_str(&s);
            }
            AggregatorImpl::Avg(a) => {
                let n = match a.field.as_str() {
                    "timestamp" => Some(event.timestamp as i64),
                    other => {
                        // Use get_field_scalar to avoid string allocation
                        match event.get_field_scalar(other) {
                            Some(ScalarValue::Int64(i)) => Some(i),
                            Some(ScalarValue::Timestamp(ts)) => Some(ts),
                            Some(ScalarValue::Utf8(s)) => s.parse::<i64>().ok(),
                            _ => None,
                        }
                    }
                };
                if let Some(v) = n {
                    a.update_value_i64(v);
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CountAll {
    count: i64,
}

impl CountAll {
    pub fn new() -> Self {
        Self { count: 0 }
    }

    #[inline]
    pub fn update(&mut self) {
        self.count += 1;
    }

    #[inline]
    pub fn merge(&mut self, other: &CountAll) {
        self.count += other.count;
    }

    pub fn finalize(&self) -> AggOutput {
        AggOutput::Count(self.count)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CountField {
    pub field: String,
    count: i64,
}

impl CountField {
    pub fn new(field: String) -> Self {
        Self { field, count: 0 }
    }

    pub fn update(&mut self, row_idx: usize, columns: &HashMap<String, ColumnValues>) {
        if let Some(col) = columns.get(&self.field) {
            // Check if the column is typed and use the appropriate getter
            let is_non_null = match col.physical_type() {
                Some(PhysicalType::I64) => col.get_i64_at(row_idx).is_some(),
                Some(PhysicalType::U64) => col.get_u64_at(row_idx).is_some(),
                Some(PhysicalType::F64) => col.get_f64_at(row_idx).is_some(),
                Some(PhysicalType::Bool) => col.get_bool_at(row_idx).is_some(),
                _ => {
                    // For string/untyped columns, use get_str_at
                    col.get_str_at(row_idx).is_some()
                }
            };

            if is_non_null {
                self.count += 1;
            }
        }
    }

    pub fn update_non_null(&mut self) {
        self.count += 1;
    }

    pub fn merge(&mut self, other: &CountField) {
        self.count += other.count;
    }

    pub fn finalize(&self) -> AggOutput {
        AggOutput::Count(self.count)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CountUnique {
    pub field: String,
    uniq: HashSet<String>,
}

impl CountUnique {
    pub fn new(field: String) -> Self {
        Self {
            field,
            uniq: HashSet::new(),
        }
    }

    pub fn values(&self) -> &HashSet<String> {
        &self.uniq
    }

    pub fn update(&mut self, row_idx: usize, columns: &HashMap<String, ColumnValues>) {
        if let Some(col) = columns.get(&self.field) {
            if let Some(s) = col.get_str_at(row_idx) {
                self.uniq.insert(s.to_string());
            } else {
                // Missing value in column: treat as empty string (consistent with update_from_event)
                self.uniq.insert(String::new());
            }
        } else {
            // Missing column: treat as empty string (consistent with update_from_event)
            self.uniq.insert(String::new());
        }
    }

    pub fn update_value_str(&mut self, s: &str) {
        self.uniq.insert(s.to_string());
    }

    pub fn merge(&mut self, other: &CountUnique) {
        for v in &other.uniq {
            self.uniq.insert(v.clone());
        }
    }

    pub fn finalize(&self) -> AggOutput {
        AggOutput::CountUnique(self.uniq.len())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Sum {
    pub field: String,
    sum: i64,
}

impl Sum {
    pub fn new(field: String) -> Self {
        Self { field, sum: 0 }
    }

    pub fn update(&mut self, row_idx: usize, columns: &HashMap<String, ColumnValues>) {
        if let Some(col) = columns.get(&self.field) {
            if let Some(v) = col.get_i64_at(row_idx) {
                self.sum += v;
            }
        }
    }

    /// SIMD-optimized columnar update for Sum aggregation
    fn update_column_simd(
        &mut self,
        start: usize,
        end: usize,
        columns: &HashMap<String, ColumnValues>,
    ) {
        if let Some(col) = columns.get(&self.field) {
            if let Some((values, valid)) = col.get_i64_slice_with_validity(start, end) {
                // Use SIMD for fast summation
                let mut sum = 0i64;
                let len = values.len();
                let mut i = 0;

                // SIMD loop: process 4 values at a time
                while i + SIMD_LANES <= len {
                    let vals = Simd::<i64, SIMD_LANES>::from_array(
                        values[i..i + SIMD_LANES]
                            .try_into()
                            .expect("slice to array of SIMD_LANES"),
                    );
                    let mask = Simd::<i64, SIMD_LANES>::from_array(
                        valid[i..i + SIMD_LANES]
                            .iter()
                            .map(|&v| if v { 1 } else { 0 })
                            .collect::<Vec<_>>()
                            .try_into()
                            .expect("validity to array"),
                    );
                    // Multiply by mask to zero out invalid values
                    let masked = vals * mask;
                    // Horizontal sum
                    let lane_sum: i64 = masked.reduce_sum();
                    sum += lane_sum;
                    i += SIMD_LANES;
                }

                // Scalar tail
                while i < len {
                    if valid[i] {
                        sum += values[i];
                    }
                    i += 1;
                }

                self.sum += sum;
            } else {
                // Fallback to row-by-row if columnar path not available
                for row_idx in start..end {
                    self.update(row_idx, columns);
                }
            }
        }
    }

    #[inline]
    pub fn merge(&mut self, other: &Sum) {
        self.sum += other.sum;
    }

    pub fn finalize(&self) -> AggOutput {
        AggOutput::Sum(self.sum)
    }

    pub fn update_value_i64(&mut self, v: i64) {
        self.sum += v;
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Min {
    pub field: String,
    min_num: Option<i64>,
    min_str: Option<String>,
}

impl Min {
    pub fn new(field: String) -> Self {
        Self {
            field,
            min_num: None,
            min_str: None,
        }
    }

    pub fn update(&mut self, row_idx: usize, columns: &HashMap<String, ColumnValues>) {
        if let Some(col) = columns.get(&self.field) {
            if let Some(v) = col.get_i64_at(row_idx) {
                match self.min_num {
                    Some(cur) if v < cur => self.min_num = Some(v),
                    None => self.min_num = Some(v),
                    _ => {}
                }
            } else if let Some(s) = col.get_str_at(row_idx) {
                match &self.min_str {
                    Some(cur) if s < cur.as_str() => self.min_str = Some(s.to_string()),
                    None => self.min_str = Some(s.to_string()),
                    _ => {}
                }
            }
        }
    }

    pub fn merge(&mut self, other: &Min) {
        match (self.min_num, other.min_num) {
            (Some(a), Some(b)) => {
                if b < a {
                    self.min_num = Some(b)
                }
            }
            (None, Some(b)) => self.min_num = Some(b),
            _ => {}
        }

        match (&self.min_str, &other.min_str) {
            (Some(a), Some(b)) => {
                if b < a {
                    self.min_str = Some(b.clone())
                }
            }
            (None, Some(b)) => self.min_str = Some(b.clone()),
            _ => {}
        }
    }

    pub fn finalize(&self) -> AggOutput {
        if let Some(v) = self.min_num {
            AggOutput::Min(v.to_string())
        } else if let Some(s) = &self.min_str {
            AggOutput::Min(s.clone())
        } else {
            AggOutput::Min(String::new())
        }
    }

    pub fn update_value_str(&mut self, s: &str) {
        if let Ok(v) = s.parse::<i64>() {
            match self.min_num {
                Some(cur) if v < cur => self.min_num = Some(v),
                None => self.min_num = Some(v),
                _ => {}
            }
        } else {
            match &self.min_str {
                Some(cur) if s < cur.as_str() => self.min_str = Some(s.to_string()),
                None => self.min_str = Some(s.to_string()),
                _ => {}
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Max {
    pub field: String,
    max_num: Option<i64>,
    max_str: Option<String>,
}

impl Max {
    pub fn new(field: String) -> Self {
        Self {
            field,
            max_num: None,
            max_str: None,
        }
    }

    pub fn update(&mut self, row_idx: usize, columns: &HashMap<String, ColumnValues>) {
        if let Some(col) = columns.get(&self.field) {
            if let Some(v) = col.get_i64_at(row_idx) {
                match self.max_num {
                    Some(cur) if v > cur => self.max_num = Some(v),
                    None => self.max_num = Some(v),
                    _ => {}
                }
            } else if let Some(s) = col.get_str_at(row_idx) {
                match &self.max_str {
                    Some(cur) if s > cur.as_str() => self.max_str = Some(s.to_string()),
                    None => self.max_str = Some(s.to_string()),
                    _ => {}
                }
            }
        }
    }

    pub fn merge(&mut self, other: &Max) {
        match (self.max_num, other.max_num) {
            (Some(a), Some(b)) => {
                if b > a {
                    self.max_num = Some(b)
                }
            }
            (None, Some(b)) => self.max_num = Some(b),
            _ => {}
        }

        match (&self.max_str, &other.max_str) {
            (Some(a), Some(b)) => {
                if b > a {
                    self.max_str = Some(b.clone())
                }
            }
            (None, Some(b)) => self.max_str = Some(b.clone()),
            _ => {}
        }
    }

    pub fn finalize(&self) -> AggOutput {
        if let Some(v) = self.max_num {
            AggOutput::Max(v.to_string())
        } else if let Some(s) = &self.max_str {
            AggOutput::Max(s.clone())
        } else {
            AggOutput::Max(String::new())
        }
    }

    pub fn update_value_str(&mut self, s: &str) {
        if let Ok(v) = s.parse::<i64>() {
            match self.max_num {
                Some(cur) if v > cur => self.max_num = Some(v),
                None => self.max_num = Some(v),
                _ => {}
            }
        } else {
            match &self.max_str {
                Some(cur) if s > cur.as_str() => self.max_str = Some(s.to_string()),
                None => self.max_str = Some(s.to_string()),
                _ => {}
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Avg {
    pub field: String,
    sum: i64,
    count: i64,
}

impl Avg {
    pub fn new(field: String) -> Self {
        Self {
            field,
            sum: 0,
            count: 0,
        }
    }

    pub fn update(&mut self, row_idx: usize, columns: &HashMap<String, ColumnValues>) {
        if let Some(col) = columns.get(&self.field) {
            if let Some(v) = col.get_i64_at(row_idx) {
                self.sum += v;
                self.count += 1;
            }
        }
    }

    /// SIMD-optimized columnar update for Avg aggregation
    fn update_column_simd(
        &mut self,
        start: usize,
        end: usize,
        columns: &HashMap<String, ColumnValues>,
    ) {
        if let Some(col) = columns.get(&self.field) {
            if let Some((values, valid)) = col.get_i64_slice_with_validity(start, end) {
                // Use SIMD for fast summation and counting
                let mut sum = 0i64;
                let mut count = 0i64;
                let len = values.len();
                let mut i = 0;

                // SIMD loop: process 4 values at a time
                while i + SIMD_LANES <= len {
                    let vals = Simd::<i64, SIMD_LANES>::from_array(
                        values[i..i + SIMD_LANES]
                            .try_into()
                            .expect("slice to array of SIMD_LANES"),
                    );
                    let mask = Simd::<i64, SIMD_LANES>::from_array(
                        valid[i..i + SIMD_LANES]
                            .iter()
                            .map(|&v| if v { 1 } else { 0 })
                            .collect::<Vec<_>>()
                            .try_into()
                            .expect("validity to array"),
                    );
                    // Multiply by mask to zero out invalid values
                    let masked = vals * mask;
                    // Horizontal sum
                    let lane_sum: i64 = masked.reduce_sum();
                    sum += lane_sum;
                    count += mask.reduce_sum();
                    i += SIMD_LANES;
                }

                // Scalar tail
                while i < len {
                    if valid[i] {
                        sum += values[i];
                        count += 1;
                    }
                    i += 1;
                }

                self.sum += sum;
                self.count += count;
            } else {
                // Fallback to row-by-row if columnar path not available
                for row_idx in start..end {
                    self.update(row_idx, columns);
                }
            }
        }
    }

    #[inline]
    pub fn merge(&mut self, other: &Avg) {
        self.sum += other.sum;
        self.count += other.count;
    }

    pub fn finalize(&self) -> AggOutput {
        if self.count == 0 {
            return AggOutput::Avg(0.0);
        }
        AggOutput::Avg(self.sum as f64 / self.count as f64)
    }

    pub fn update_value_i64(&mut self, v: i64) {
        self.sum += v;
        self.count += 1;
    }

    /// Get sum and count without finalizing (for streaming mode merging)
    /// This preserves the mergeable state needed for accurate AVG aggregation across shards
    pub fn sum_count(&self) -> (i64, i64) {
        (self.sum, self.count)
    }
}
