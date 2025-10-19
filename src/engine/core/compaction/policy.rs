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

/// K-way count policy: group L0 by uid and chunk every k segments into one L1 output.
pub struct KWayCountPolicy {
    pub k: usize,
}

impl KWayCountPolicy {
    pub fn new(k: usize) -> Self {
        Self { k }
    }
}

impl Default for KWayCountPolicy {
    fn default() -> Self {
        Self {
            k: CONFIG.engine.segments_per_merge,
        }
    }
}

impl CompactionPolicy for KWayCountPolicy {
    fn plan(&self, index: &SegmentIndex) -> Vec<MergePlan> {
        // Group L0 entries by UID
        let mut uid_to_segments: HashMap<String, Vec<String>> = HashMap::new();
        for entry in &index.entries {
            if SegmentId::from(entry.id).level() == 0 {
                for uid in &entry.uids {
                    uid_to_segments
                        .entry(uid.clone())
                        .or_default()
                        .push(SegmentId::from(entry.id).dir_name());
                }
            }
        }

        // Prepare allocator seeded from all existing labels for correct naming
        let existing_labels = index.all_labels();
        let mut allocator =
            RangeAllocator::from_existing_ids(existing_labels.iter().map(|s| s.as_str()));

        let mut plans: Vec<MergePlan> = Vec::new();
        for (uid, mut labels) in uid_to_segments {
            // Ensure deterministic ordering when chunking
            labels.sort();
            if labels.len() < self.k {
                continue;
            }

            for chunk in labels.chunks(self.k) {
                if chunk.len() < self.k {
                    break;
                }
                let out_id = allocator.next_for_level(1);
                plans.push(MergePlan {
                    level_from: 0,
                    level_to: 1,
                    uid: uid.clone(),
                    input_segment_labels: chunk.to_vec(),
                    output_segment_id: out_id,
                });
            }
        }

        plans
    }
}
