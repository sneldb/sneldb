use super::handover::{CompactionHandover, SegmentCache};
use super::merge_plan::MergePlan;
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

    let plan = MergePlan {
        level_from: 0,
        level_to: 1,
        uid: "uidA".to_string(),
        input_segment_labels: vec!["00001".into(), "00002".into()],
        output_segment_id: 10_000,
    };

    let new_entry = SegmentEntry {
        id: 10_000,
        uids: vec!["uidA".to_string()],
    };

    handover.commit(&plan, new_entry).await.unwrap();

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
