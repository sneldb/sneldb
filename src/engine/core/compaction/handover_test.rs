use super::handover::{CompactionHandover, SegmentCache};
use super::segment_batch::{SegmentBatch, UidPlan};
use crate::engine::core::{SegmentEntry, SegmentIndex};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

type StdRwLock<T> = std::sync::RwLock<T>;

#[derive(Clone, Default)]
struct StubCache {
    calls: Arc<StdRwLock<Vec<String>>>,
    name: &'static str,
}

impl StubCache {
    fn new(name: &'static str) -> Self {
        Self {
            calls: Arc::new(StdRwLock::new(Vec::new())),
            name,
        }
    }

    fn recorded(&self) -> Vec<String> {
        self.calls.read().unwrap().clone()
    }
}

impl SegmentCache for StubCache {
    fn invalidate_segment(&self, segment_label: &str) {
        self.calls
            .write()
            .unwrap()
            .push(format!("{}:{}", self.name, segment_label));
    }
}

#[tokio::test]
async fn commit_updates_index_segment_ids_and_reclaims() {
    let shard_dir = tempdir().unwrap();
    let shard_path = shard_dir.path().to_path_buf();

    // Seed shard directory with L0 segments and index
    for label in ["00001", "00002"] {
        let seg_dir = shard_path.join(label);
        std::fs::create_dir_all(&seg_dir).unwrap();
        std::fs::write(seg_dir.join("dummy"), b"ok").unwrap();
    }

    // Create output segment directory (required for crash safety verification)
    let output_seg_dir = shard_path.join("10000");
    std::fs::create_dir_all(&output_seg_dir).unwrap();
    std::fs::write(output_seg_dir.join("uidA.zones"), b"zone data").unwrap();

    let mut index = SegmentIndex::load(&shard_path).await.unwrap();
    index.insert_entry(SegmentEntry {
        id: 1,
        uids: vec!["uidA".to_string()],
    });
    index.insert_entry(SegmentEntry {
        id: 2,
        uids: vec!["uidA".to_string()],
    });
    index.save(&shard_path).await.unwrap();

    let segment_ids = Arc::new(StdRwLock::new(vec!["00001".into(), "00002".into()]));
    let flush_lock = Arc::new(tokio::sync::Mutex::new(()));
    let column_stub = StubCache::new("column_handle");
    let zone_stub = StubCache::new("zone_surf");
    let zone_index_stub = StubCache::new("zone_index");
    let catalog_stub = StubCache::new("index_catalog");
    let column_block_stub = StubCache::new("column_block");
    let handover = CompactionHandover::with_caches(
        0,
        shard_path.clone(),
        Arc::clone(&segment_ids),
        Arc::clone(&flush_lock),
        Arc::new(column_stub.clone()),
        Arc::new(zone_stub.clone()),
        Arc::new(zone_index_stub.clone()),
        Arc::new(catalog_stub.clone()),
        Arc::new(column_block_stub.clone()),
    );

    let batch = SegmentBatch {
        input_segment_labels: vec!["00001".into(), "00002".into()],
        uid_plans: vec![UidPlan {
            uid: "uidA".to_string(),
        output_segment_id: 10_000,
        }],
    };

    let new_entry = SegmentEntry {
        id: 10_000,
        uids: vec!["uidA".to_string()],
    };

    let drained = handover.commit_batch(&batch, vec![new_entry]).await.unwrap();
    handover.schedule_reclaim(drained);

    // Allow background reclaim task to finish
    tokio::time::sleep(Duration::from_millis(100)).await;

    let reloaded = SegmentIndex::load(&shard_path).await.unwrap();
    assert_eq!(reloaded.len(), 1);
    let entry = reloaded.iter_all().next().unwrap();
    assert_eq!(entry.id, 10_000);

    let ids = segment_ids.read().unwrap().clone();
    assert_eq!(ids, vec!["10000".to_string()]);

    assert!(!shard_path.join("00001").exists());
    assert!(!shard_path.join("00002").exists());

    let expected = vec!["00001", "00002"];
    for label in expected {
        assert!(
            column_stub
                .recorded()
                .contains(&format!("column_handle:{}", label))
        );
        assert!(
            zone_stub
                .recorded()
                .contains(&format!("zone_surf:{}", label))
        );
        assert!(
            zone_index_stub
                .recorded()
                .contains(&format!("zone_index:{}", label))
        );
        assert!(
            catalog_stub
                .recorded()
                .contains(&format!("index_catalog:{}", label))
        );
        assert!(
            column_block_stub
                .recorded()
                .contains(&format!("column_block:{}", label))
        );
    }
}

#[tokio::test]
async fn commit_batch_verifies_output_segments_exist() {
    let shard_dir = tempdir().unwrap();
    let shard_path = shard_dir.path().to_path_buf();

    // Create input segments
    for label in ["00001", "00002"] {
        let seg_dir = shard_path.join(label);
        std::fs::create_dir_all(&seg_dir).unwrap();
    }

    let mut index = SegmentIndex::load(&shard_path).await.unwrap();
    index.insert_entry(SegmentEntry {
        id: 1,
        uids: vec!["uidA".to_string()],
    });
    index.insert_entry(SegmentEntry {
        id: 2,
        uids: vec!["uidA".to_string()],
    });
    index.save(&shard_path).await.unwrap();

    let segment_ids = Arc::new(StdRwLock::new(vec!["00001".into(), "00002".into()]));
    let flush_lock = Arc::new(tokio::sync::Mutex::new(()));
    let column_stub = StubCache::new("column");
    let handover = CompactionHandover::with_caches(
        0,
        shard_path.clone(),
        Arc::clone(&segment_ids),
        Arc::clone(&flush_lock),
        Arc::new(column_stub.clone()),
        Arc::new(column_stub.clone()),
        Arc::new(column_stub.clone()),
        Arc::new(column_stub.clone()),
        Arc::new(column_stub.clone()),
    );

    let batch = SegmentBatch {
        input_segment_labels: vec!["00001".into(), "00002".into()],
        uid_plans: vec![UidPlan {
            uid: "uidA".to_string(),
            output_segment_id: 10_000,
        }],
    };

    // Try to commit without creating output segment - should fail
    let new_entry = SegmentEntry {
        id: 10_000,
        uids: vec!["uidA".to_string()],
    };

    let result = handover.commit_batch(&batch, vec![new_entry]).await;
    assert!(result.is_err(), "Should fail when output segment doesn't exist");
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Output segment directory does not exist"));

    // Verify index was not modified
    let reloaded = SegmentIndex::load(&shard_path).await.unwrap();
    assert_eq!(reloaded.len(), 2, "Index should not be modified on failure");
}

#[tokio::test]
async fn commit_batch_succeeds_when_output_segment_exists() {
    let shard_dir = tempdir().unwrap();
    let shard_path = shard_dir.path().to_path_buf();

    // Create input segments
    for label in ["00001", "00002"] {
        let seg_dir = shard_path.join(label);
        std::fs::create_dir_all(&seg_dir).unwrap();
    }

    // Create output segment directory
    let output_seg_dir = shard_path.join("10000");
    std::fs::create_dir_all(&output_seg_dir).unwrap();
    // Create a .zones file to make it look like a valid segment
    std::fs::write(output_seg_dir.join("uidA.zones"), b"zone data").unwrap();

    let mut index = SegmentIndex::load(&shard_path).await.unwrap();
    index.insert_entry(SegmentEntry {
        id: 1,
        uids: vec!["uidA".to_string()],
    });
    index.insert_entry(SegmentEntry {
        id: 2,
        uids: vec!["uidA".to_string()],
    });
    index.save(&shard_path).await.unwrap();

    let segment_ids = Arc::new(StdRwLock::new(vec!["00001".into(), "00002".into()]));
    let flush_lock = Arc::new(tokio::sync::Mutex::new(()));
    let column_stub = StubCache::new("column");
    let handover = CompactionHandover::with_caches(
        0,
        shard_path.clone(),
        Arc::clone(&segment_ids),
        Arc::clone(&flush_lock),
        Arc::new(column_stub.clone()),
        Arc::new(column_stub.clone()),
        Arc::new(column_stub.clone()),
        Arc::new(column_stub.clone()),
        Arc::new(column_stub.clone()),
    );

    let batch = SegmentBatch {
        input_segment_labels: vec!["00001".into(), "00002".into()],
        uid_plans: vec![UidPlan {
            uid: "uidA".to_string(),
            output_segment_id: 10_000,
        }],
    };

    let new_entry = SegmentEntry {
        id: 10_000,
        uids: vec!["uidA".to_string()],
    };

    // Should succeed since output segment exists
    let drained = handover.commit_batch(&batch, vec![new_entry]).await.unwrap();
    assert_eq!(drained.len(), 2, "Both input segments should be drained");

    // Verify index was updated
    let reloaded = SegmentIndex::load(&shard_path).await.unwrap();
    assert_eq!(reloaded.len(), 1);
    assert_eq!(reloaded.iter_all().next().unwrap().id, 10_000);
}

#[tokio::test]
async fn commit_batch_verifies_all_output_segments_in_batch() {
    let shard_dir = tempdir().unwrap();
    let shard_path = shard_dir.path().to_path_buf();

    // Create input segments
    for label in ["00001", "00002"] {
        let seg_dir = shard_path.join(label);
        std::fs::create_dir_all(&seg_dir).unwrap();
    }

    // Save index first (before creating output segments) to avoid recovery picking them up
    let mut index = SegmentIndex::load(&shard_path).await.unwrap();
    index.insert_entry(SegmentEntry {
        id: 1,
        uids: vec!["uidA".to_string()],
    });
    index.insert_entry(SegmentEntry {
        id: 2,
        uids: vec!["uidB".to_string()],
    });
    index.save(&shard_path).await.unwrap();

    // Create only one output segment (missing the other)
    let output_seg_dir_1 = shard_path.join("10000");
    std::fs::create_dir_all(&output_seg_dir_1).unwrap();
    std::fs::write(output_seg_dir_1.join("uidA.zones"), b"zone data").unwrap();

    // Don't create 20000 - this should cause failure

    let segment_ids = Arc::new(StdRwLock::new(vec!["00001".into(), "00002".into()]));
    let flush_lock = Arc::new(tokio::sync::Mutex::new(()));
    let column_stub = StubCache::new("column");
    let handover = CompactionHandover::with_caches(
        0,
        shard_path.clone(),
        Arc::clone(&segment_ids),
        Arc::clone(&flush_lock),
        Arc::new(column_stub.clone()),
        Arc::new(column_stub.clone()),
        Arc::new(column_stub.clone()),
        Arc::new(column_stub.clone()),
        Arc::new(column_stub.clone()),
    );

    let batch = SegmentBatch {
        input_segment_labels: vec!["00001".into(), "00002".into()],
        uid_plans: vec![
            UidPlan {
                uid: "uidA".to_string(),
                output_segment_id: 10_000,
            },
            UidPlan {
                uid: "uidB".to_string(),
                output_segment_id: 20_000,
            },
        ],
    };

    let new_entries = vec![
        SegmentEntry {
            id: 10_000,
            uids: vec!["uidA".to_string()],
        },
        SegmentEntry {
            id: 20_000,
            uids: vec!["uidB".to_string()],
        },
    ];

    // Should fail because 20000 doesn't exist
    let result = handover.commit_batch(&batch, new_entries).await;
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Output segment directory does not exist"));

    // Index should not be modified
    let reloaded = SegmentIndex::load(&shard_path).await.unwrap();
    assert_eq!(reloaded.len(), 2);
}
