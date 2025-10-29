use crate::engine::core::EventId;
use crate::engine::shard::context::ShardContext;
use std::fs::{File, create_dir_all};
use tempfile::tempdir;
use tokio::sync::mpsc;

#[tokio::test]
async fn test_shard_context_initialization_with_existing_segments() {
    // Arrange
    let tmp_dir = tempdir().unwrap();
    let base_dir = tmp_dir.path().join("shard-0");
    let wal_dir = base_dir.clone();

    create_dir_all(&base_dir).unwrap();

    // Create dummy segment files
    File::create(base_dir.join("00001")).unwrap();
    File::create(base_dir.join("00003")).unwrap();
    File::create(base_dir.join("00002")).unwrap();
    File::create(base_dir.join("random-file.txt")).unwrap(); // ignored

    let (tx, _rx) = mpsc::channel(1);

    // Act
    let ctx = ShardContext::new(0, tx, base_dir.clone(), wal_dir.clone());

    // Assert
    assert_eq!(ctx.id, 0);
    assert_eq!(ctx.segment_id, 4); // max is 3, so next should be 4
    assert!(ctx.memtable.len() == 0);
    assert!(ctx.wal.is_some());

    // Segment files should be sorted
    let ids = ctx.segment_ids.read().unwrap();
    assert_eq!(
        *ids,
        vec![
            "00001".to_string(),
            "00002".to_string(),
            "00003".to_string()
        ]
    );
}

#[tokio::test]
async fn next_event_id_is_monotonic_and_shard_encoded() {
    let tmp_dir = tempdir().unwrap();
    let base_dir = tmp_dir.path().join("shard-7");
    let wal_dir = base_dir.clone();
    create_dir_all(&base_dir).unwrap();

    let (tx, _rx) = mpsc::channel(1);
    let mut ctx = ShardContext::new(7, tx, base_dir, wal_dir);

    let first: EventId = ctx.next_event_id();
    let second: EventId = ctx.next_event_id();

    assert!(
        second.raw() > first.raw(),
        "event ids must be strictly increasing"
    );

    let shard_bits = (first.raw() >> 12) & ((1 << 10) - 1);
    assert_eq!(shard_bits, 7, "shard id should be encoded in EventId");
}
