use crate::engine::core::filter::condition::{FieldAccessor, PreparedAccessor};
use crate::engine::core::filter::direct_event_accessor::DirectEventAccessor;
use crate::engine::core::{
    CandidateZone, Condition, Event, EventBuilder, LogicalCondition, NumericCondition,
    StringCondition,
};
use std::collections::HashSet;
use std::thread;
use tracing::info;

/// Evaluates conditions against candidate zones
#[derive(Debug)]
pub struct ConditionEvaluator {
    conditions: Vec<Box<dyn Condition>>,
    numeric_fields: HashSet<String>,
}

impl ConditionEvaluator {
    pub fn new() -> Self {
        Self {
            conditions: Vec::new(),
            numeric_fields: HashSet::new(),
        }
    }

    pub fn add_numeric_condition(
        &mut self,
        field: String,
        operation: super::condition::CompareOp,
        value: i64,
    ) {
        if tracing::enabled!(tracing::Level::INFO) {
            info!(
                target: "sneldb::evaluator",
                "Adding numeric condition: {} {:?} {}",
                field, operation, value
            );
        }
        self.numeric_fields.insert(field.clone());
        self.conditions
            .push(Box::new(NumericCondition::new(field, operation, value)));
    }

    pub fn add_string_condition(
        &mut self,
        field: String,
        operation: super::condition::CompareOp,
        value: String,
    ) {
        if tracing::enabled!(tracing::Level::INFO) {
            info!(
                target: "sneldb::evaluator",
                "Adding string condition: {} {:?} '{}'",
                field, operation, value
            );
        }
        self.conditions
            .push(Box::new(StringCondition::new(field, operation, value)));
    }

    pub fn add_logical_condition(&mut self, condition: LogicalCondition) {
        if tracing::enabled!(tracing::Level::INFO) {
            info!(
                target: "sneldb::evaluator",
                "Adding logical condition: {:?}",
                condition
            );
        }
        if condition.is_numeric() {
            condition.collect_numeric_fields(&mut self.numeric_fields);
        }
        self.conditions.push(Box::new(condition));
    }

    pub fn into_conditions(self) -> Vec<Box<dyn Condition>> {
        self.conditions
    }

    #[inline]
    pub fn has_numeric_conditions(&self) -> bool {
        !self.numeric_fields.is_empty()
    }

    /// Evaluates all conditions against a single event using direct field access
    #[inline]
    pub fn evaluate_event(&self, event: &Event) -> bool {
        let accessor = DirectEventAccessor::new(event);
        // Short-circuit: if any condition fails, return false immediately
        for condition in &self.conditions {
            if !condition.evaluate_event_direct(&accessor) {
                return false;
            }
        }
        true
    }

    pub fn evaluate_zones(&self, zones: Vec<CandidateZone>) -> Vec<Event> {
        self.evaluate_zones_with_limit(zones, None)
    }

    pub fn evaluate_zones_with_limit(
        &self,
        zones: Vec<CandidateZone>,
        limit: Option<usize>,
    ) -> Vec<Event> {
        if tracing::enabled!(tracing::Level::INFO) {
            info!(
                target: "sneldb::evaluator",
                "Starting parallel condition evaluation on {} zones",
                zones.len()
            );
        }

        let mut results: Vec<Event> = Vec::new();
        'zones: for zone in zones.into_iter() {
            if let Some(lim) = limit {
                if results.len() >= lim {
                    break 'zones;
                }
            }

            if tracing::enabled!(tracing::Level::INFO) {
                let event_count = zone.values.values().next().map(|v| v.len()).unwrap_or(0);
                info!(
                    target: "sneldb::evaluator",
                    thread_id = ?thread::current().id(),
                    zone_id = zone.zone_id,
                    event_count,
                    "Processing zone"
                );
            }

            let event_count = zone.values.values().next().map(|v| v.len()).unwrap_or(0);
            let accessor = PreparedAccessor::new(&zone.values);
            if self.has_numeric_conditions() {
                accessor.warm_numeric_cache(&self.numeric_fields);
            }
            if tracing::enabled!(tracing::Level::DEBUG) {
                if let Some(id_vals) = zone.values.get("id") {
                    let sample = usize::min(3, event_count);
                    for i in 0..sample {
                        let has_u64 = id_vals.get_u64_at(i).is_some();
                        let has_i64 = id_vals.get_i64_at(i).is_some();
                        let has_f64 = id_vals.get_f64_at(i).is_some();
                        tracing::debug!(target: "sneldb::evaluator", zone_id = zone.zone_id, row = i, has_u64, has_i64, has_f64, u64 = id_vals.get_u64_at(i), i64 = id_vals.get_i64_at(i), f64 = id_vals.get_f64_at(i), "id field sample");
                    }
                }
            }
            for i in 0..event_count {
                if let Some(lim) = limit {
                    if results.len() >= lim {
                        break;
                    }
                }
                let passes = self
                    .conditions
                    .iter()
                    .all(|condition| condition.evaluate_at(&accessor, i));
                if passes {
                    let mut builder = EventBuilder::new();
                    for (field, values) in &zone.values {
                        if let Some(n) = values.get_u64_at(i) {
                            builder.add_field_u64(field, n);
                            continue;
                        }
                        if let Some(n) = values.get_i64_at(i) {
                            builder.add_field_i64(field, n);
                            continue;
                        }
                        if let Some(f) = values.get_f64_at(i) {
                            builder.add_field_f64(field, f);
                            continue;
                        }
                        if let Some(b) = values.get_bool_at(i) {
                            builder.add_field_bool(field, b);
                            continue;
                        }
                        if let Some(value) = values.get_str_at(i) {
                            builder.add_field(field, value);
                        } else {
                            builder.add_field_null(field);
                        }
                    }
                    results.push(builder.build());
                }
            }
        }

        if tracing::enabled!(tracing::Level::INFO) {
            info!(
                target: "sneldb::evaluator",
                "Parallel evaluation complete: {} total matching events",
                results.len()
            );
        }

        results
    }

    /// Evaluates all conditions against a single row in a zone via accessor
    pub fn evaluate_row_at(&self, accessor: &dyn FieldAccessor, index: usize) -> bool {
        self.conditions
            .iter()
            .all(|condition| condition.evaluate_at(accessor, index))
    }
}
