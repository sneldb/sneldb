use crate::engine::core::filter::condition::{FieldAccessor, PreparedAccessor};
use crate::engine::core::{
    CandidateZone, Condition, Event, EventBuilder, LogicalCondition, NumericCondition,
    StringCondition,
};
use std::collections::HashMap;
use std::thread;
use tracing::info;

/// Evaluates conditions against candidate zones
#[derive(Debug)]
pub struct ConditionEvaluator {
    conditions: Vec<Box<dyn Condition>>,
}

impl ConditionEvaluator {
    pub fn new() -> Self {
        Self {
            conditions: Vec::new(),
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
        self.conditions.push(Box::new(condition));
    }

    pub fn into_conditions(self) -> Vec<Box<dyn Condition>> {
        self.conditions
    }

    /// Evaluates all conditions against a single event
    pub fn evaluate_event(&self, event: &Event) -> bool {
        if tracing::enabled!(tracing::Level::INFO) {
            info!(target: "sneldb::evaluator", "Evaluating single event");
        }

        let mut event_values = HashMap::new();
        event_values.insert("event_type".to_string(), vec![event.event_type.clone()]);
        event_values.insert("context_id".to_string(), vec![event.context_id.clone()]);
        event_values.insert("timestamp".to_string(), vec![event.timestamp.to_string()]);

        if let Some(obj) = event.payload.as_object() {
            for (key, _) in obj {
                event_values.insert(key.clone(), vec![event.get_field_value(key)]);
            }
        }

        if tracing::enabled!(tracing::Level::INFO) {
            info!(
                target: "sneldb::evaluator",
                "Event values: {:?}, Evaluating against {} condition(s)",
                event_values,
                self.conditions.len()
            );
        }

        self.conditions
            .iter()
            .all(|condition| condition.evaluate(&event_values))
    }

    pub fn evaluate_zones(&self, zones: Vec<CandidateZone>) -> Vec<Event> {
        if tracing::enabled!(tracing::Level::INFO) {
            info!(
                target: "sneldb::evaluator",
                "Starting parallel condition evaluation on {} zones",
                zones.len()
            );
        }

        let mut results: Vec<Event> = Vec::new();
        for zone in zones.into_iter() {
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
            for i in 0..event_count {
                let passes = self
                    .conditions
                    .iter()
                    .all(|condition| condition.evaluate_at(&accessor, i));
                if passes {
                    let mut builder = EventBuilder::new();
                    for (field, values) in &zone.values {
                        if let Some(value) = values.get_str_at(i) {
                            builder.add_field(field, value);
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
