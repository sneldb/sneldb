use std::collections::HashMap;

use crate::engine::core::SegmentIndex;
use crate::engine::core::segment::range_allocator::RangeAllocator;
use crate::engine::core::segment::segment_id::SegmentId;
use crate::shared::config::CONFIG;

use super::merge_plan::MergePlan;

/// Strategy for planning compaction work from a SegmentIndex.
pub trait CompactionPolicy {
    fn plan(&self, index: &SegmentIndex) -> Vec<MergePlan>;
}

/// K-way count policy: group segments by uid and chunk every k segments into one output.
/// Handles multiple levels (L0->L1, L1->L2, etc.) and forces compaction of leftovers
/// when they accumulate to k-1 segments (one less than the normal batch size).
pub struct KWayCountPolicy {
    pub k: usize,
    /// Minimum segments required to force compact leftovers (~67% of k)
    min_leftover_threshold: usize,
}

impl KWayCountPolicy {
    pub fn new(k: usize) -> Self {
        // Use a less aggressive threshold: only force-compact when leftovers accumulate significantly
        // For k=3: threshold = max(1, 2) = 2 (only 2+ leftovers trigger compaction)
        // For k=5: threshold = max(1, 3) = 3 (only 3+ leftovers trigger compaction)
        // For k=8: threshold = max(1, 5) = 5 (only 5+ leftovers trigger compaction)
        // This allows more leftovers to accumulate before forcing compaction
        Self {
            k,
            min_leftover_threshold: ((k * 2) / 3).max(1), // ~67% of k, minimum 1
        }
    }
}

impl Default for KWayCountPolicy {
    fn default() -> Self {
        let k = CONFIG.engine.segments_per_merge;
        Self {
            k,
            min_leftover_threshold: ((k * 2) / 3).max(1), // ~67% of k, minimum 1
        }
    }
}

impl CompactionPolicy for KWayCountPolicy {
    fn plan(&self, index: &SegmentIndex) -> Vec<MergePlan> {
        // Prepare allocator seeded from all existing labels for correct naming
        let existing_labels = index.all_labels();
        let mut allocator =
            RangeAllocator::from_existing_ids(existing_labels.iter().map(|s| s.as_str()));

        let mut plans: Vec<MergePlan> = Vec::new();
        let mut all_leftover_segments_by_level: HashMap<u32, HashMap<String, Vec<String>>> =
            HashMap::new();

        // Find the maximum level in the index
        let max_level = index.iter_all().map(|e| e.level()).max().unwrap_or(0);

        // Process each level: L0->L1, L1->L2, etc.
        for current_level in 0..=max_level {
            let next_level = current_level + 1;

            // Group segments at current level by UID
            let mut uid_to_segments: HashMap<String, Vec<String>> = HashMap::new();
            for entry in index.iter_all() {
                if entry.level() == current_level {
                    for uid in &entry.uids {
                        uid_to_segments
                            .entry(uid.clone())
                            .or_default()
                            .push(SegmentId::from(entry.id).dir_name());
                    }
                }
            }

            if uid_to_segments.is_empty() {
                continue;
            }

            tracing::warn!(
                target: "compaction_policy::plan",
                current_level = current_level,
                next_level = next_level,
                uid_count = uid_to_segments.len(),
                "Planning compaction for level"
            );

            let mut leftover_segments_by_uid: HashMap<String, Vec<String>> = HashMap::new();

            for (uid, mut labels) in uid_to_segments {
                // Ensure deterministic ordering when chunking
                labels.sort();

                // Note: Leftover segments from previous cycles are already in the index,
                // so they'll be included in uid_to_segments automatically in the next cycle

                // Check if we should force compact leftovers
                let should_force_compact =
                    labels.len() >= self.min_leftover_threshold && labels.len() < self.k;

                if labels.len() < self.min_leftover_threshold {
                    // Too few segments even for forced compaction
                    leftover_segments_by_uid.insert(uid.clone(), labels);
                    continue;
                }

                // Force compact if we have enough leftovers (>= min_leftover_threshold but < k)
                if should_force_compact {
                    tracing::warn!(
                        target: "compaction_policy::plan",
                        level = current_level,
                        uid = %uid,
                        segment_count = labels.len(),
                        min_threshold = self.min_leftover_threshold,
                        k = self.k,
                        "Forcing compaction of leftover segments"
                    );
                    let out_id = allocator.next_for_level(next_level);
                    plans.push(MergePlan {
                        level_from: current_level,
                        level_to: next_level,
                        uid: uid.clone(),
                        input_segment_labels: labels.clone(),
                        output_segment_id: out_id,
                    });
                    continue;
                }

                // Normal compaction: chunk segments into batches of k
                let mut processed_count = 0;
                for chunk in labels.chunks(self.k) {
                    if chunk.len() < self.k {
                        // Leftover segments that don't form a complete batch
                        // Don't force-compact these immediately - let them accumulate across cycles
                        leftover_segments_by_uid.insert(uid.clone(), chunk.to_vec());
                        break;
                    }
                    let out_id = allocator.next_for_level(next_level);
                    plans.push(MergePlan {
                        level_from: current_level,
                        level_to: next_level,
                        uid: uid.clone(),
                        input_segment_labels: chunk.to_vec(),
                        output_segment_id: out_id,
                    });
                    processed_count += chunk.len();
                }

                // Only force-compact leftovers that have accumulated significantly
                // This prevents excessive compaction of small leftover batches
                // Note: We don't force-compact leftovers from the current chunking operation
                // They will be included in the next cycle's planning automatically

                // Log leftover segments if any (that weren't force-compacted)
                if let Some(leftover) = leftover_segments_by_uid.get(&uid) {
                    if !leftover.is_empty() {
                        tracing::warn!(
                            target: "compaction_policy::plan",
                            level = current_level,
                            uid = %uid,
                            leftover_count = leftover.len(),
                            leftover_segments = ?leftover,
                            processed_count,
                            total_segments = labels.len(),
                            "UID has leftover segments that will be compacted in next cycle when more segments accumulate"
                        );
                    }
                }
            }

            // Store leftovers for this level
            if !leftover_segments_by_uid.is_empty() {
                all_leftover_segments_by_level.insert(current_level, leftover_segments_by_uid);
            }
        }

        tracing::warn!(
            target: "compaction_policy::plan",
            total_plans = plans.len(),
            plans_by_level = ?plans.iter().map(|p| (p.level_from, p.level_to)).collect::<std::collections::HashMap<_, _>>(),
            plans_by_uid = ?plans.iter().map(|p| (&p.uid, p.input_segment_labels.len())).collect::<std::collections::HashMap<_, _>>(),
            leftover_segments_by_level = ?all_leftover_segments_by_level.iter().map(|(level, uids)| (level.clone(), uids.iter().map(|(uid, segs)| (uid.clone(), segs.len())).collect::<std::collections::HashMap<_, _>>())).collect::<std::collections::HashMap<_, _>>(),
            "Generated compaction plans"
        );

        plans
    }
}
