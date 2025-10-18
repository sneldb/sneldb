use crate::engine::core::SegmentIndex;
use crate::engine::core::SegmentIndexBuilder;
use tempfile::tempdir;

#[tokio::test]
async fn test_segment_index_builder_adds_segment_entry() {
    // Setup: temp dir to simulate segment directory
    let tmp_dir = tempdir().expect("Failed to create temp dir");
    let shard_dir = tmp_dir.path();
    let segment_dir = shard_dir.join("00042");
    std::fs::create_dir(&segment_dir).expect("Failed to create segment dir");

    // Given: UID list
    let uids = vec!["uid_1".to_string(), "uid_2".to_string()];
    let segment_id = 42;

    // When: we build and add the segment entry
    let builder = SegmentIndexBuilder {
        segment_id,
        segment_dir: &segment_dir,
        event_type_uids: uids.clone(),
        flush_coordination_lock: std::sync::Arc::new(tokio::sync::Mutex::new(())),
    };

    builder
        .add_segment_entry(None)
        .await
        .expect("Failed to add entry");

    // Then: verify segments.idx file is present in shard_dir
    let idx_path = shard_dir.join("segments.idx");
    assert!(
        idx_path.exists(),
        "Expected segments.idx to exist in shard directory"
    );

    // Load index and check contents
    let index = SegmentIndex::load(shard_dir)
        .await
        .expect("Failed to load index");
    assert_eq!(index.entries.len(), 1, "Expected one entry in SegmentIndex");

    let entry = &index.entries[0];
    assert_eq!(entry.id, 42);
    assert_eq!(format!("{:05}", entry.id), "00042");
    assert_eq!(entry.uids, uids);
}
