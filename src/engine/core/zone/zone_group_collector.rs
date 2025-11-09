use crate::engine::core::filter::filter_group::{FilterGroup, filter_key};
use crate::engine::core::{CandidateZone, LogicalOp, QueryCaches, QueryPlan, ZoneCombiner};
use std::collections::{HashMap, HashSet};

/// Collects zones from FilterGroup tree, preserving logical structure.
///
/// Process FilterGroup tree and combine zones according to operators.
pub struct ZoneGroupCollector<'a> {
    /// HashMap cache: filter key (column:operation:value) -> zones
    /// This allows O(1) lookups instead of O(n) linear search.
    filter_to_zones: HashMap<String, Vec<CandidateZone>>,
    /// Query plan for accessing segment information (required for NOT operations)
    plan: &'a QueryPlan,
    /// Query caches for loading zone metadata
    caches: Option<&'a QueryCaches>,
}

impl<'a> ZoneGroupCollector<'a> {
    /// Creates a new ZoneGroupCollector with a HashMap cache, plan, and caches.
    /// The cache maps filter keys (from filter_key function) to their zones.
    /// Plan and caches are required for smart NOT operation handling.
    pub fn new(
        filter_to_zones: HashMap<String, Vec<CandidateZone>>,
        plan: &'a QueryPlan,
        caches: Option<&'a QueryCaches>,
    ) -> Self {
        Self {
            filter_to_zones,
            plan,
            caches,
        }
    }

    /// Collects zones from children and combines them according to the operator
    fn collect_and_combine_children(
        &self,
        children: &[FilterGroup],
        op: LogicalOp,
    ) -> Vec<CandidateZone> {
        let mut child_zones: Vec<Vec<CandidateZone>> = Vec::with_capacity(children.len());

        for child in children {
            let zones = self.collect_zones_from_group(child);

            // Early exit for AND if any child has no zones
            if matches!(op, LogicalOp::And) && zones.is_empty() {
                return vec![];
            }

            child_zones.push(zones);
        }

        if child_zones.is_empty() {
            return vec![];
        }

        let result = ZoneCombiner::new(child_zones, op).combine();
        CandidateZone::uniq(result)
    }

    /// Collects zones from a FilterGroup tree recursively.
    pub fn collect_zones_from_group(&self, group: &FilterGroup) -> Vec<CandidateZone> {
        match group {
            FilterGroup::Filter { .. } => {
                // Single filter - lookup zones from cache
                self.get_zones_for_filter(group)
            }
            FilterGroup::And(children) => {
                self.collect_and_combine_children(children, LogicalOp::And)
            }
            FilterGroup::Or(children) => self.collect_and_combine_children(children, LogicalOp::Or),
            FilterGroup::Not(child) => {
                // Smart NOT handling: compute zone complement
                self.handle_not(child)
            }
        }
    }

    /// Handles NOT operations intelligently:
    /// - For NOT(Filter): Get all zones for segments, subtract zones matching the filter
    /// - For NOT(AND): Apply De Morgan's law: NOT A OR NOT B OR ...
    /// - For NOT(OR): Apply De Morgan's law: NOT A AND NOT B AND ...
    fn handle_not(&self, child: &FilterGroup) -> Vec<CandidateZone> {
        match child {
            // NOT(Filter): Compute complement - all zones minus zones matching filter
            FilterGroup::Filter { .. } => {
                let matching_zones = self.collect_zones_from_group(child);
                self.compute_complement(&matching_zones)
            }
            // NOT(AND): De Morgan's law -> NOT A OR NOT B OR ...
            FilterGroup::And(children) => {
                let not_children: Vec<FilterGroup> = children
                    .iter()
                    .map(|c| FilterGroup::Not(Box::new(c.clone())))
                    .collect();
                self.collect_and_combine_children(&not_children, LogicalOp::Or)
            }
            // NOT(OR): De Morgan's law -> NOT A AND NOT B AND ...
            FilterGroup::Or(children) => {
                let not_children: Vec<FilterGroup> = children
                    .iter()
                    .map(|c| FilterGroup::Not(Box::new(c.clone())))
                    .collect();
                self.collect_and_combine_children(&not_children, LogicalOp::And)
            }
            // NOT(NOT X): Double negation -> X
            FilterGroup::Not(grandchild) => self.collect_zones_from_group(grandchild),
        }
    }

    /// Computes zone complement: all zones for segments minus zones matching the filter
    fn compute_complement(&self, matching_zones: &[CandidateZone]) -> Vec<CandidateZone> {
        let plan = self.plan;
        let caches = self.caches;

        // Get all zones for all segments in the plan
        let all_zones = self.get_all_zones_for_segments(plan, caches);

        if matching_zones.is_empty() {
            // No matching zones means all zones match NOT
            return all_zones;
        }

        // Create a set of matching zone keys for fast lookup
        let matching_keys: HashSet<(u32, String)> = matching_zones
            .iter()
            .map(|z| (z.zone_id, z.segment_id.clone()))
            .collect();

        // Return zones that are in all_zones but NOT in matching_zones
        let complement: Vec<CandidateZone> = all_zones
            .into_iter()
            .filter(|z| !matching_keys.contains(&(z.zone_id, z.segment_id.clone())))
            .collect();

        if tracing::enabled!(tracing::Level::DEBUG) {
            tracing::debug!(
                target: "sneldb::zone_group_collector",
                matching_count = matching_zones.len(),
                all_count = matching_keys.len(),
                complement_count = complement.len(),
                "Computed NOT complement"
            );
        }

        complement
    }

    /// Gets all zones for segments in the query plan
    fn get_all_zones_for_segments(
        &self,
        plan: &QueryPlan,
        caches: Option<&QueryCaches>,
    ) -> Vec<CandidateZone> {
        let segment_ids = plan.segment_ids.read().unwrap();
        let event_type = plan.event_type();

        // Get UID for event type from plan's filter groups
        // Extract UID from the first filter group that has one
        let uid = plan
            .filter_groups
            .iter()
            .find_map(|fg| match fg {
                FilterGroup::Filter { uid: Some(u), .. } => Some(u.clone()),
                _ => None,
            })
            .unwrap_or_else(|| {
                // Fallback: use event_type as UID
                if tracing::enabled!(tracing::Level::DEBUG) {
                    tracing::debug!(
                        target: "sneldb::zone_group_collector",
                        event_type = %event_type,
                        "Using event_type as UID fallback for NOT operation"
                    );
                }
                event_type.to_string()
            });

        let mut all_zones = Vec::new();
        for segment_id in segment_ids.iter() {
            let zones = if let Some(caches) = caches {
                CandidateZone::create_all_zones_for_segment_from_meta_cached(
                    &plan.segment_base_dir,
                    segment_id,
                    &uid,
                    Some(caches),
                )
            } else {
                CandidateZone::create_all_zones_for_segment_from_meta(
                    &plan.segment_base_dir,
                    segment_id,
                    &uid,
                )
            };
            all_zones.extend(zones);
        }

        // Sort zones deterministically by (segment_id, zone_id) to ensure consistent processing order
        // This prevents flaky tests due to non-deterministic iteration order
        all_zones.sort_by(|a, b| {
            a.segment_id
                .cmp(&b.segment_id)
                .then_with(|| a.zone_id.cmp(&b.zone_id))
        });

        all_zones
    }

    /// Gets zones for a single FilterGroup::Filter by looking up in the cache.
    /// Uses O(1) HashMap lookup instead of O(n) linear search.
    fn get_zones_for_filter(&self, filter: &FilterGroup) -> Vec<CandidateZone> {
        // Extract filter fields and create key
        let key = match filter {
            FilterGroup::Filter {
                column,
                operation,
                value,
                ..
            } => filter_key(column, operation, value),
            _ => return vec![], // Not a single filter
        };

        // O(1) lookup in HashMap
        self.filter_to_zones.get(&key).cloned().unwrap_or_default()
    }
}
