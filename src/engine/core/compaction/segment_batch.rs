use super::merge_plan::MergePlan;
use std::collections::{HashMap, HashSet};

/// Groups compaction plans by their input segments.
/// Plans that share the same input segments can be processed together efficiently.
#[derive(Debug, Clone)]
pub struct SegmentBatch {
    /// Input segment labels shared by all plans in this batch
    pub input_segment_labels: Vec<String>,
    /// Plans grouped by UID, each with its output segment ID
    pub uid_plans: Vec<UidPlan>,
}

/// Represents a single UID's compaction plan within a batch
#[derive(Debug, Clone)]
pub struct UidPlan {
    pub uid: String,
    pub output_segment_id: u32,
}

impl SegmentBatch {
    /// Groups plans by their input segments.
    /// Plans with identical input segments (regardless of order) are grouped together for efficient processing.
    pub fn group_plans(plans: Vec<MergePlan>) -> Vec<Self> {
        let mut batches: HashMap<Vec<String>, Vec<UidPlan>> = HashMap::new();

        for plan in plans {
            // Normalize segment labels by sorting to ensure consistent grouping
            let mut normalized_labels = plan.input_segment_labels.clone();
            normalized_labels.sort();
            batches.entry(normalized_labels).or_default().push(UidPlan {
                uid: plan.uid,
                output_segment_id: plan.output_segment_id,
            });
        }

        batches
            .into_iter()
            .map(|(input_segment_labels, uid_plans)| Self {
                input_segment_labels,
                uid_plans,
            })
            .collect()
    }

    /// Returns the set of UIDs in this batch
    pub fn uids(&self) -> HashSet<&str> {
        self.uid_plans.iter().map(|p| p.uid.as_str()).collect()
    }

    /// Returns the number of UIDs in this batch
    pub fn uid_count(&self) -> usize {
        self.uid_plans.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn groups_plans_by_identical_input_segments() {
        let plans = vec![
            MergePlan {
                level_from: 0,
                level_to: 1,
                uid: "uid1".to_string(),
                input_segment_labels: vec!["00001".to_string(), "00002".to_string()],
                output_segment_id: 10000,
            },
            MergePlan {
                level_from: 0,
                level_to: 1,
                uid: "uid2".to_string(),
                input_segment_labels: vec!["00001".to_string(), "00002".to_string()],
                output_segment_id: 10001,
            },
            MergePlan {
                level_from: 0,
                level_to: 1,
                uid: "uid3".to_string(),
                input_segment_labels: vec!["00003".to_string(), "00004".to_string()],
                output_segment_id: 10002,
            },
        ];

        let batches = SegmentBatch::group_plans(plans);
        assert_eq!(batches.len(), 2);

        let batch1 = batches
            .iter()
            .find(|b| b.input_segment_labels == vec!["00001", "00002"])
            .unwrap();
        assert_eq!(batch1.uid_count(), 2);
        assert!(batch1.uids().contains("uid1"));
        assert!(batch1.uids().contains("uid2"));

        let batch2 = batches
            .iter()
            .find(|b| b.input_segment_labels == vec!["00003", "00004"])
            .unwrap();
        assert_eq!(batch2.uid_count(), 1);
        assert!(batch2.uids().contains("uid3"));
    }

    #[test]
    fn handles_empty_plans() {
        let batches = SegmentBatch::group_plans(vec![]);
        assert!(batches.is_empty());
    }

    #[test]
    fn handles_single_plan() {
        let plans = vec![MergePlan {
            level_from: 0,
            level_to: 1,
            uid: "uid1".to_string(),
            input_segment_labels: vec!["00001".to_string()],
            output_segment_id: 10000,
        }];

        let batches = SegmentBatch::group_plans(plans);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].uid_count(), 1);
        assert_eq!(batches[0].input_segment_labels, vec!["00001"]);
    }
}
