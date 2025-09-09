use crate::engine::core::{
    CandidateZone, Condition, Event, EventBuilder, LogicalCondition, NumericCondition,
    StringCondition,
};
use rayon::prelude::*;
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
        info!(
            target: "sneldb::evaluator",
            "Adding numeric condition: {} {:?} {}",
            field, operation, value
        );
        self.conditions
            .push(Box::new(NumericCondition::new(field, operation, value)));
    }

    pub fn add_string_condition(
        &mut self,
        field: String,
        operation: super::condition::CompareOp,
        value: String,
    ) {
        info!(
            target: "sneldb::evaluator",
            "Adding string condition: {} {:?} '{}'",
            field, operation, value
        );
        self.conditions
            .push(Box::new(StringCondition::new(field, operation, value)));
    }

    pub fn add_logical_condition(&mut self, condition: LogicalCondition) {
        info!(
            target: "sneldb::evaluator",
            "Adding logical condition: {:?}",
            condition
        );
        self.conditions.push(Box::new(condition));
    }

    pub fn into_conditions(self) -> Vec<Box<dyn Condition>> {
        self.conditions
    }

    /// Evaluates all conditions against a single event
    pub fn evaluate_event(&self, event: &Event) -> bool {
        info!(target: "sneldb::evaluator", "Evaluating single event");

        let mut event_values = HashMap::new();
        event_values.insert("event_type".to_string(), vec![event.event_type.clone()]);
        event_values.insert("context_id".to_string(), vec![event.context_id.clone()]);
        event_values.insert("timestamp".to_string(), vec![event.timestamp.to_string()]);

        if let Some(obj) = event.payload.as_object() {
            for (key, _) in obj {
                event_values.insert(key.clone(), vec![event.get_field_value(key)]);
            }
        }

        info!(
            target: "sneldb::evaluator",
            "Event values: {:?}, Evaluating against {} condition(s)",
            event_values,
            self.conditions.len()
        );

        self.conditions
            .iter()
            .all(|condition| condition.evaluate(&event_values))
    }

    pub fn evaluate_zones(&self, zones: Vec<CandidateZone>) -> Vec<Event> {
        info!(
            target: "sneldb::evaluator",
            "Starting parallel condition evaluation on {} zones",
            zones.len()
        );

        let results: Vec<Event> = zones
            .into_par_iter()
            .flat_map(|zone| {
                let thread_id = thread::current().id();
                let zone_id = zone.zone_id.clone();
                let event_count = zone.values.values().next().map(|v| v.len()).unwrap_or(0);

                info!(
                    target: "sneldb::evaluator",
                    "Thread {:?} processing zone {} with {} events",
                    thread_id,
                    zone_id,
                    event_count
                );

                if event_count == 0 {
                    return Vec::new();
                }

                let events: Vec<Event> = (0..event_count)
                    .into_par_iter()
                    .filter_map(|i| {
                        let mut event_values = HashMap::new();
                        for (field, values) in &zone.values {
                            if let Some(value) = values.get(i) {
                                event_values.insert(field.clone(), vec![value.clone()]);
                            }
                        }

                        let passes = self
                            .conditions
                            .iter()
                            .all(|condition| condition.evaluate(&event_values));

                        if passes {
                            let mut builder = EventBuilder::new();
                            for (field, values) in &zone.values {
                                if let Some(value) = values.get(i) {
                                    builder.add_field(field, value);
                                }
                            }
                            Some(builder.build())
                        } else {
                            None
                        }
                    })
                    .collect();

                info!(
                    target: "sneldb::evaluator",
                    "Thread {:?} zone {}: {} events passed conditions",
                    thread_id,
                    zone_id,
                    events.len()
                );

                events
            })
            .collect();

        info!(
            target: "sneldb::evaluator",
            "Parallel evaluation complete: {} total matching events",
            results.len()
        );

        results
    }
}
