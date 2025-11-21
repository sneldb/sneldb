use crate::engine::core::{CandidateZone, ExecutionStep, LogicalOp, QueryCaches, QueryPlan};
use tracing::info;

pub struct ZoneStepRunner<'a> {
    _plan: &'a QueryPlan,
    caches: Option<&'a QueryCaches>,
}

impl<'a> ZoneStepRunner<'a> {
    pub fn new(plan: &'a QueryPlan) -> Self {
        Self {
            _plan: plan,
            caches: None,
        }
    }

    pub fn with_caches(mut self, caches: Option<&'a QueryCaches>) -> Self {
        self.caches = caches;
        self
    }

    /// Execute steps in the given order; optionally derive pruned segments from first step’s zones
    pub fn run(
        &self,
        steps: &mut [ExecutionStep<'a>],
        order: &[(usize, Option<Vec<String>>)],
    ) -> (Vec<Vec<CandidateZone>>, Option<Vec<String>>) {
        let mut outputs: Vec<Vec<CandidateZone>> = Vec::with_capacity(order.len());
        let mut pruned: Option<Vec<String>> = None;

        // Full segment list to use before pruning exists
        let mut full_segments: Vec<String> = self._plan.segment_ids.read().unwrap().clone();
        if let Some(tracker) = self._plan.inflight_segments() {
            let inflight = tracker.snapshot();
            for seg in &inflight {
                if !full_segments.contains(seg) {
                    full_segments.push(seg.clone());
                }
            }
            if tracing::enabled!(tracing::Level::DEBUG) && !inflight.is_empty() {
                tracing::debug!(
                    target: "sneldb::zone_runner",
                    inflight_count = inflight.len(),
                    inflight_segments = ?inflight,
                    merged_segments = ?full_segments,
                    "Merged inflight segments into query plan"
                );
            }
        }

        // Decide if pruning is allowed: only when op is AND and the first planned step is context_id
        let op = LogicalOp::from_expr(self._plan.where_clause());
        let allow_prune = if let Some((first_idx, _)) = order.first() {
            matches!(op, LogicalOp::And)
                && steps
                    .get(*first_idx)
                    .map(|s| s.filter.is_context_id())
                    .unwrap_or(false)
        } else {
            false
        };

        for (pos, (idx, maybe_subset)) in order.iter().cloned().enumerate() {
            let step = &mut steps[idx];
            let segments: Vec<String> = if let Some(subset) = maybe_subset {
                subset
            } else if allow_prune {
                if pos == 0 {
                    if tracing::enabled!(tracing::Level::DEBUG) {
                        tracing::debug!(
                            target: "sneldb::zone_runner",
                            step_idx = idx,
                            step_pos = pos,
                            segments = ?full_segments,
                            "First prunable step using full segment list"
                        );
                    }
                    full_segments.clone()
                } else {
                    pruned.clone().unwrap_or_else(|| full_segments.clone())
                }
            } else {
                full_segments.clone()
            };

            let step_start = std::time::Instant::now();
            step.get_candidate_zones_with_segments(self.caches, &segments);
            let zones = std::mem::take(&mut step.candidate_zones);
            if tracing::enabled!(tracing::Level::DEBUG) && !zones.is_empty() {
                let segs: std::collections::HashSet<&str> =
                    zones.iter().map(|z| z.segment_id.as_str()).collect();
                tracing::debug!(
                    target: "sneldb::zone_runner",
                    step_idx = idx,
                    step_pos = pos,
                    zones_found = zones.len(),
                    segments_examined = segments.len(),
                    segment_sources = ?segs,
                    "Execution step produced candidate zones"
                );
            }
            let step_time = step_start.elapsed();

            // Log slow steps when they find no zones (indicates expensive work for no result)
            if zones.is_empty() && step_time.as_millis() > 10 {
                if tracing::enabled!(tracing::Level::INFO) {
                    tracing::info!(
                        target: "sneldb::zone_step_runner",
                        step_idx = idx,
                        step_pos = pos,
                        segments_checked = segments.len(),
                        step_time_ms = step_time.as_millis(),
                        "Step completed with 0 zones after expensive work"
                    );
                }
            }

            // Derive pruned only once, on the first step, when allowed
            if allow_prune && pos == 0 {
                let kept: std::collections::HashSet<String> =
                    zones.iter().map(|z| z.segment_id.clone()).collect();
                pruned = Some(kept.into_iter().collect());
                if tracing::enabled!(tracing::Level::INFO) {
                    info!(
                        target = "sneldb::zone_runner",
                        seg_kept = pruned.as_ref().map(|v| v.len()).unwrap_or(0),
                        zones_found = zones.len(),
                        "Derived pruned segment list"
                    );
                }
            }
            outputs.push(zones);
        }

        (outputs, if allow_prune { pruned } else { None })
    }
}
