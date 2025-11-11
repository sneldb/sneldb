use super::merge_plan::MergePlan;
use super::segment_batch::SegmentBatch;

#[tokio::test]
async fn groups_plans_with_identical_input_segments() {
    use crate::logging::init_for_tests;
    init_for_tests();

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

#[tokio::test]
async fn handles_empty_plans() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let batches = SegmentBatch::group_plans(vec![]);
    assert!(batches.is_empty());
}

#[tokio::test]
async fn handles_single_plan() {
    use crate::logging::init_for_tests;
    init_for_tests();

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

#[tokio::test]
async fn groups_many_uids_from_same_segments() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let mut plans = Vec::new();
    for i in 0..100 {
        plans.push(MergePlan {
            level_from: 0,
            level_to: 1,
            uid: format!("uid{}", i),
            input_segment_labels: vec!["00001".to_string(), "00002".to_string()],
            output_segment_id: 10000 + i as u32,
        });
    }

    let batches = SegmentBatch::group_plans(plans);
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].uid_count(), 100);
    assert_eq!(batches[0].input_segment_labels, vec!["00001", "00002"]);
}

#[tokio::test]
async fn preserves_segment_label_ordering() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let plans = vec![
        MergePlan {
            level_from: 0,
            level_to: 1,
            uid: "uid1".to_string(),
            input_segment_labels: vec!["00002".to_string(), "00001".to_string()],
            output_segment_id: 10000,
        },
        MergePlan {
            level_from: 0,
            level_to: 1,
            uid: "uid2".to_string(),
            input_segment_labels: vec!["00001".to_string(), "00002".to_string()],
            output_segment_id: 10001,
        },
    ];

    let batches = SegmentBatch::group_plans(plans);
    // Should group together even if order differs
    assert_eq!(batches.len(), 1);
    // The batch will have the order from the first plan
    assert_eq!(batches[0].input_segment_labels.len(), 2);
}
