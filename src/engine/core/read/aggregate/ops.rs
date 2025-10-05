use std::collections::{HashMap, HashSet};

use crate::engine::core::column::column_values::ColumnValues;
use crate::engine::core::read::aggregate::plan::AggregateOpSpec;

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
    pub fn update_from_event(&mut self, event: &crate::engine::core::Event) {
        match self {
            AggregatorImpl::CountAll(a) => a.update(),
            AggregatorImpl::CountField(a) => {
                let val = event.get_field(a.field.as_str());
                if !matches!(val, None | Some(serde_json::Value::Null)) {
                    a.update_non_null();
                }
            }
            AggregatorImpl::CountUnique(a) => {
                let s = match a.field.as_str() {
                    "context_id" => event.context_id.clone(),
                    "event_type" => event.event_type.clone(),
                    "timestamp" => event.timestamp.to_string(),
                    other => event.get_field_value(other),
                };
                a.update_value_str(&s);
            }
            AggregatorImpl::Sum(a) => {
                let n = match a.field.as_str() {
                    "timestamp" => Some(event.timestamp as i64),
                    other => event.get_field_value(other).parse::<i64>().ok(),
                };
                if let Some(v) = n {
                    a.update_value_i64(v);
                }
            }
            AggregatorImpl::Min(a) => {
                let s = event.get_field_value(&a.field);
                a.update_value_str(&s);
            }
            AggregatorImpl::Max(a) => {
                let s = event.get_field_value(&a.field);
                a.update_value_str(&s);
            }
            AggregatorImpl::Avg(a) => {
                let n = match a.field.as_str() {
                    "timestamp" => Some(event.timestamp as i64),
                    other => event.get_field_value(other).parse::<i64>().ok(),
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
            if col.get_str_at(row_idx).is_some() {
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
            }
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
}
