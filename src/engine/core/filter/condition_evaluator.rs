use crate::engine::core::column::format::PhysicalType;
use crate::engine::core::filter::condition::{FieldAccessor, PreparedAccessor};
use crate::engine::core::filter::direct_event_accessor::DirectEventAccessor;
use crate::engine::core::{
    CandidateZone, Condition, Event, EventBuilder, EventId, InNumericCondition, InStringCondition,
    LogicalCondition, NumericCondition, StringCondition,
};
use std::collections::HashSet;
use std::simd::Simd;
use std::simd::prelude::*;

const LANES: usize = 4;

/// Evaluates conditions against candidate zones (SIMD-optimized)
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
        self.conditions
            .push(Box::new(StringCondition::new(field, operation, value)));
    }

    pub fn add_in_numeric_condition(&mut self, field: String, values: Vec<i64>) {
        self.numeric_fields.insert(field.clone());
        self.conditions
            .push(Box::new(InNumericCondition::new(field, values)));
    }

    pub fn add_in_string_condition(&mut self, field: String, values: Vec<String>) {
        self.conditions
            .push(Box::new(InStringCondition::new(field, values)));
    }

    pub fn add_logical_condition(&mut self, condition: LogicalCondition) {
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

    #[inline]
    pub fn evaluate_event(&self, event: &Event) -> bool {
        let accessor = DirectEventAccessor::new(event);
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
        let mut results: Vec<Event> = Vec::new();

        'zones: for zone in zones.into_iter() {
            if let Some(lim) = limit {
                if results.len() >= lim {
                    break 'zones;
                }
            }

            let event_count = zone.values.values().next().map(|v| v.len()).unwrap_or(0);
            if event_count == 0 {
                continue;
            }

            let accessor = PreparedAccessor::new(&zone.values);
            if self.has_numeric_conditions() {
                accessor.warm_numeric_cache(&self.numeric_fields);
            }

            // Boolean keep-mask for this zone
            let mut mask = vec![true; event_count];

            // SIMD for numeric; scalar for the rest
            for condition in &self.conditions {
                if let Some(nc) = condition.as_any().downcast_ref::<NumericCondition>() {
                    Self::evaluate_numeric_simd(nc, &accessor, 0, event_count, &mut mask);
                } else {
                    for i in 0..event_count {
                        if mask[i] && !condition.evaluate_at(&accessor, i) {
                            mask[i] = false;
                        }
                    }
                }
            }

            // Materialize passing rows
            // Check once per zone if event_id column is missing/empty to avoid per-event checks
            let event_id_missing = zone
                .values
                .get("event_id")
                .map(|vals| vals.len() == 0)
                .unwrap_or(true);

            // OPTIMIZATION: Pre-classify fields by type to avoid repeated physical_type() calls
            // in the hot inner loop. physical_type() doesn't change between rows.
            let mut u64_fields: Vec<(&String, &_)> = Vec::new();
            let mut i64_fields: Vec<(&String, &_)> = Vec::new();
            let mut f64_fields: Vec<(&String, &_)> = Vec::new();
            let mut bool_fields: Vec<(&String, &_)> = Vec::new();
            let mut str_fields: Vec<(&String, &_)> = Vec::new();

            for (field, values) in &zone.values {
                match values.physical_type() {
                    Some(PhysicalType::U64) => u64_fields.push((field, values)),
                    Some(PhysicalType::I64) => i64_fields.push((field, values)),
                    Some(PhysicalType::F64) => f64_fields.push((field, values)),
                    Some(PhysicalType::Bool) => bool_fields.push((field, values)),
                    _ => str_fields.push((field, values)),
                }
            }

            for i in 0..event_count {
                if !mask[i] {
                    continue;
                }
                if let Some(lim) = limit {
                    if results.len() >= lim {
                        break 'zones;
                    }
                }
                let mut builder = EventBuilder::new();

                // Optimized: Use pre-classified field lists instead of matching every field
                for (field, values) in &u64_fields {
                    if let Some(n) = values.get_u64_at(i) {
                        builder.add_field_u64(field, n);
                    } else {
                        builder.add_field_null(field);
                    }
                }
                for (field, values) in &i64_fields {
                    if let Some(n) = values.get_i64_at(i) {
                        builder.add_field_i64(field, n);
                    } else {
                        builder.add_field_null(field);
                    }
                }
                for (field, values) in &f64_fields {
                    if let Some(f) = values.get_f64_at(i) {
                        builder.add_field_f64(field, f);
                    } else {
                        builder.add_field_null(field);
                    }
                }
                for (field, values) in &bool_fields {
                    if let Some(b) = values.get_bool_at(i) {
                        builder.add_field_bool(field, b);
                    } else {
                        builder.add_field_null(field);
                    }
                }
                for (field, values) in &str_fields {
                    if let Some(value) = values.get_str_at(i) {
                        builder.add_field(field, value);
                    } else {
                        builder.add_field_null(field);
                    }
                }

                let mut event = builder.build();
                // If event_id column is missing/empty, generate a unique ID based on zone and row index
                // This prevents deduplication from incorrectly removing valid events
                // Only check/update if we know the column is missing (optimization)
                if event_id_missing || event.event_id().is_zero() {
                    let synthetic_id = (zone.zone_id as u64) << 32 | (i as u64);
                    event.set_event_id(EventId::from(synthetic_id));
                }
                results.push(event);
            }
        }

        results
    }

    /// SIMD numeric evaluator – tries u64 then i64 then f64
    fn evaluate_numeric_simd(
        condition: &NumericCondition,
        accessor: &PreparedAccessor,
        start: usize,
        end: usize,
        mask: &mut [bool],
    ) {
        // u64 fast-path (and early-out for negative thresholds)
        if let Some((col, valid)) =
            accessor.get_u64_slice_with_validity(condition.field(), start, end)
        {
            if condition.value() < 0 {
                // u64 cannot satisfy negative thresholds
                for m in mask.iter_mut() {
                    *m = false;
                }
                return;
            }
            let cmp_val = condition.value() as u64;
            simd_scan_u64(&col, &valid, cmp_val, condition.op(), mask);
            return;
        }

        // i64 path
        if let Some((col, valid)) =
            accessor.get_i64_slice_with_validity(condition.field(), start, end)
        {
            let cmp_val = condition.value();
            simd_scan_i64(&col, &valid, cmp_val, condition.op(), mask);
            return;
        }

        // f64 path
        if let Some((col, valid)) =
            accessor.get_f64_slice_with_validity(condition.field(), start, end)
        {
            let cmp_val = condition.value() as f64;
            simd_scan_f64(&col, &valid, cmp_val, condition.op(), mask);
            return;
        }

        // Fallback: evaluate per-row using the accessor
        for i in start..end {
            let local = i - start;
            if mask[local] && !condition.evaluate_at(accessor, i) {
                mask[local] = false;
            }
        }
    }

    pub fn evaluate_row_at(&self, accessor: &dyn FieldAccessor, index: usize) -> bool {
        self.conditions
            .iter()
            .all(|condition| condition.evaluate_at(accessor, index))
    }
}

/* ------------------------- SIMD helpers (u64 / i64 / f64) ------------------------ */

#[inline]
fn apply_validity(mask: &mut [bool], valid: &[bool]) {
    // mask &= valid
    for (m, v) in mask.iter_mut().zip(valid.iter()) {
        if *m && !*v {
            *m = false;
        }
    }
}

#[inline]
fn apply_bitmask(mask: &mut [bool], base: usize, bitmask: u16, width: usize) {
    // Clear rows where predicate failed
    for j in 0..width {
        if mask[base + j] && ((bitmask >> j) & 1) == 0 {
            mask[base + j] = false;
        }
    }
}

#[inline]
fn simd_scan_u64(
    col: &[u64],
    valid: &[bool],
    cmp_val: u64,
    op: super::condition::CompareOp,
    mask: &mut [bool],
) {
    debug_assert_eq!(col.len(), mask.len());
    debug_assert_eq!(valid.len(), mask.len());

    apply_validity(mask, valid);

    let len = col.len();
    let mut i = 0;
    while i + LANES <= len {
        let vals = Simd::<u64, LANES>::from_array(
            col[i..i + LANES]
                .try_into()
                .expect("slice to array of LANES"),
        );
        let m = match op {
            super::condition::CompareOp::Gt => vals.simd_gt(Simd::splat(cmp_val)),
            super::condition::CompareOp::Gte => vals.simd_ge(Simd::splat(cmp_val)),
            super::condition::CompareOp::Lt => vals.simd_lt(Simd::splat(cmp_val)),
            super::condition::CompareOp::Lte => vals.simd_le(Simd::splat(cmp_val)),
            super::condition::CompareOp::Eq => vals.simd_eq(Simd::splat(cmp_val)),
            super::condition::CompareOp::Neq => vals.simd_ne(Simd::splat(cmp_val)),
            super::condition::CompareOp::In => {
                // IN operations use InNumericCondition, not SIMD paths
                unreachable!("IN operation should not use SIMD path")
            }
        };
        let bits = m.to_bitmask();
        apply_bitmask(mask, i, bits as u16, LANES);
        i += LANES;
    }

    // scalar tail
    while i < len {
        if mask[i] {
            let keep = match op {
                super::condition::CompareOp::Gt => col[i] > cmp_val,
                super::condition::CompareOp::Gte => col[i] >= cmp_val,
                super::condition::CompareOp::Lt => col[i] < cmp_val,
                super::condition::CompareOp::Lte => col[i] <= cmp_val,
                super::condition::CompareOp::Eq => col[i] == cmp_val,
                super::condition::CompareOp::Neq => col[i] != cmp_val,
                super::condition::CompareOp::In => {
                    // IN operations use InNumericCondition, not SIMD paths
                    unreachable!("IN operation should not use SIMD path")
                }
            };
            if !keep {
                mask[i] = false;
            }
        }
        i += 1;
    }
}

#[inline]
fn simd_scan_i64(
    col: &[i64],
    valid: &[bool],
    cmp_val: i64,
    op: super::condition::CompareOp,
    mask: &mut [bool],
) {
    debug_assert_eq!(col.len(), mask.len());
    debug_assert_eq!(valid.len(), mask.len());

    apply_validity(mask, valid);

    let len = col.len();
    let mut i = 0;
    while i + LANES <= len {
        let vals = Simd::<i64, LANES>::from_array(
            col[i..i + LANES]
                .try_into()
                .expect("slice to array of LANES"),
        );
        let m = match op {
            super::condition::CompareOp::Gt => vals.simd_gt(Simd::splat(cmp_val)),
            super::condition::CompareOp::Gte => vals.simd_ge(Simd::splat(cmp_val)),
            super::condition::CompareOp::Lt => vals.simd_lt(Simd::splat(cmp_val)),
            super::condition::CompareOp::Lte => vals.simd_le(Simd::splat(cmp_val)),
            super::condition::CompareOp::Eq => vals.simd_eq(Simd::splat(cmp_val)),
            super::condition::CompareOp::Neq => vals.simd_ne(Simd::splat(cmp_val)),
            super::condition::CompareOp::In => {
                // IN operations use InNumericCondition, not SIMD paths
                unreachable!("IN operation should not use SIMD path")
            }
        };
        let bits = m.to_bitmask();
        apply_bitmask(mask, i, bits as u16, LANES);
        i += LANES;
    }

    // scalar tail
    while i < len {
        if mask[i] && !scalar_cmp_i64(col[i], cmp_val, op) {
            mask[i] = false;
        }
        i += 1;
    }
}

#[inline]
fn simd_scan_f64(
    col: &[f64],
    valid: &[bool],
    cmp_val: f64,
    op: super::condition::CompareOp,
    mask: &mut [bool],
) {
    debug_assert_eq!(col.len(), mask.len());
    debug_assert_eq!(valid.len(), mask.len());

    apply_validity(mask, valid);

    let len = col.len();
    let mut i = 0;
    while i + LANES <= len {
        let vals = Simd::<f64, LANES>::from_array(
            col[i..i + LANES]
                .try_into()
                .expect("slice to array of LANES"),
        );
        let m = match op {
            super::condition::CompareOp::Gt => vals.simd_gt(Simd::splat(cmp_val)),
            super::condition::CompareOp::Gte => vals.simd_ge(Simd::splat(cmp_val)),
            super::condition::CompareOp::Lt => vals.simd_lt(Simd::splat(cmp_val)),
            super::condition::CompareOp::Lte => vals.simd_le(Simd::splat(cmp_val)),
            super::condition::CompareOp::Eq => vals.simd_eq(Simd::splat(cmp_val)),
            super::condition::CompareOp::Neq => vals.simd_ne(Simd::splat(cmp_val)),
            super::condition::CompareOp::In => {
                // IN operations use InNumericCondition, not SIMD paths
                unreachable!("IN operation should not use SIMD path")
            }
        };
        let bits = m.to_bitmask();
        apply_bitmask(mask, i, bits as u16, LANES);
        i += LANES;
    }

    // scalar tail (NaN semantics preserved by direct compare)
    while i < len {
        if mask[i] && !scalar_cmp_f64(col[i], cmp_val, op) {
            mask[i] = false;
        }
        i += 1;
    }
}

#[inline]
fn scalar_cmp_i64(lhs: i64, rhs: i64, op: super::condition::CompareOp) -> bool {
    match op {
        super::condition::CompareOp::Gt => lhs > rhs,
        super::condition::CompareOp::Gte => lhs >= rhs,
        super::condition::CompareOp::Lt => lhs < rhs,
        super::condition::CompareOp::Lte => lhs <= rhs,
        super::condition::CompareOp::Eq => lhs == rhs,
        super::condition::CompareOp::Neq => lhs != rhs,
        super::condition::CompareOp::In => {
            // IN operations use InNumericCondition, not scalar comparison
            unreachable!("IN operation should not use scalar comparison")
        }
    }
}

#[inline]
fn scalar_cmp_f64(lhs: f64, rhs: f64, op: super::condition::CompareOp) -> bool {
    match op {
        super::condition::CompareOp::Gt => lhs > rhs,
        super::condition::CompareOp::Gte => lhs >= rhs,
        super::condition::CompareOp::Lt => lhs < rhs,
        super::condition::CompareOp::Lte => lhs <= rhs,
        super::condition::CompareOp::Eq => lhs == rhs,
        super::condition::CompareOp::Neq => lhs != rhs,
        super::condition::CompareOp::In => {
            // IN operations use InNumericCondition, not scalar comparison
            unreachable!("IN operation should not use scalar comparison")
        }
    }
}
