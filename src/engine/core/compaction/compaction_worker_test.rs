use crate::engine::core::compaction::handover::{CompactionHandover, SegmentCache};
use crate::engine::core::{CompactionWorker, Flusher, SegmentIndex};
use crate::test_helpers::factories::*;
use serde_json::json;
use std::sync::Arc;
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
async fn compacts_l0_segments_into_l1() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().unwrap();
    let shard_dir = tmp_dir.path().join("shard-0");
    std::fs::create_dir_all(&shard_dir).unwrap();

    // Step 1: Define schemas
    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "user_logged_in";
    schema_factory
        .define_with_fields(event_type, &[("context_id", "string"), ("score", "int")])
        .await
        .unwrap();

    let uid = registry.read().await.get_uid(event_type).unwrap();

    // Step 2: Create and flush events for 4 segments (L0). With compaction_k=2 in test config,
    // this should create exactly 2 L1 outputs.
    let segment_ids = Arc::new(StdRwLock::new(Vec::new()));
    let flush_lock = Arc::new(tokio::sync::Mutex::new(()));

    for segment_id in 1..=4 {
        let segment_dir = shard_dir.join(format!("{:05}", segment_id));
        std::fs::create_dir_all(&segment_dir).unwrap();

        let events = vec![
            EventFactory::new()
                .with("event_type", event_type)
                .with("context_id", format!("ctx{}", segment_id))
                .with("payload", json!({ "score": segment_id * 10 }))
                .create(),
        ];

        let memtable = MemTableFactory::new()
            .with_capacity(2)
            .with_events(events)
            .create()
            .unwrap();

        let flusher = Flusher::new(
            memtable,
            segment_id,
            &segment_dir,
            registry.clone(),
            Arc::clone(&flush_lock),
        );
        flusher.flush().await.unwrap();

        {
            let mut ids = segment_ids.write().unwrap();
            ids.push(format!("{:05}", segment_id));
        }
    }

    let column_stub = StubCache::new("column_handle");
    let zone_stub = StubCache::new("zone_surf");
    let zone_index_stub = StubCache::new("zone_index");
    let catalog_stub = StubCache::new("index_catalog");
    let column_block_stub = StubCache::new("column_block");
    let handover = Arc::new(CompactionHandover::with_caches(
        0,
        shard_dir.clone(),
        Arc::clone(&segment_ids),
        Arc::clone(&flush_lock),
        Arc::new(column_stub.clone()),
        Arc::new(zone_stub.clone()),
        Arc::new(zone_index_stub.clone()),
        Arc::new(catalog_stub.clone()),
        Arc::new(column_block_stub.clone()),
    ));

    // Step 3: Run CompactionWorker (k-way policy groups by uid with k=2)
    let worker = CompactionWorker::new(
        0,
        shard_dir.clone(),
        registry.clone(),
        Arc::clone(&handover),
    );
    worker.run().await.unwrap();

    // Step 4: Validate index contents and on-disk artifacts
    let index = SegmentIndex::load(&shard_dir).await.unwrap();
    // Only two L1 entries should remain for this uid; no L0 leftovers
    assert_eq!(index.len(), 2, "Only compacted outputs should remain");
    for entry in index.iter_all() {
        assert!(
            entry.id >= 10_000,
            "All remaining entries must be L1 or higher"
        );
        assert_eq!(entry.uids, vec![uid.clone()]);
    }

    // Read back context_id values across both outputs and verify global sort and total count
    use crate::engine::core::{ColumnReader, ZoneMeta};
    let mut all_ctx = Vec::new();
    for entry in index.iter_all() {
        let label = format!("{:05}", entry.id);
        let out_dir = shard_dir.join(&label);
        let zones_path = out_dir.join(format!("{}.zones", uid));
        let zones = ZoneMeta::load(&zones_path).unwrap();
        // Verify numeric payload fields emit SuRF filters
        let surf_path = out_dir.join(format!("{}_{}.zsrf", uid, "score"));
        assert!(surf_path.exists(), "expected SuRF file at {:?}", surf_path);
        for z in &zones {
            let ctx_ids =
                ColumnReader::load_for_zone(&out_dir, &label, &uid, "context_id", z.zone_id)
                    .unwrap();
            all_ctx.extend(ctx_ids);
        }
    }
    assert_eq!(all_ctx.len(), 4, "Merged outputs must contain all 4 events");
    let mut sorted = all_ctx.clone();
    sorted.sort();
    assert_eq!(all_ctx, sorted, "context_id values must be globally sorted");

    // Shared segment_ids should only contain the new L1 segments
    let ids = segment_ids.read().unwrap().clone();
    assert_eq!(ids.len(), 2);
    assert!(
        ids.iter()
            .all(|label| label.parse::<u32>().unwrap() >= 10_000)
    );

    // Cache invalidations invoked for each retired segment
    let expected_labels: Vec<&str> = vec!["00001", "00002", "00003", "00004"];
    for label in &expected_labels {
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
async fn compaction_leaves_leftover_l0_when_not_multiple_of_k() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().unwrap();
    let shard_dir = tmp_dir.path().join("shard-0");
    std::fs::create_dir_all(&shard_dir).unwrap();

    // Define schema
    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "user_logged_in";
    schema_factory
        .define_with_fields(event_type, &[("context_id", "string"), ("score", "int")])
        .await
        .unwrap();
    let uid = registry.read().await.get_uid(event_type).unwrap();

    // Create 5 L0 segments (k=2 -> expect 2 L1 + 1 leftover L0)
    let segment_ids = Arc::new(StdRwLock::new(Vec::new()));
    let flush_lock = Arc::new(tokio::sync::Mutex::new(()));

    for segment_id in 1..=5 {
        let segment_dir = shard_dir.join(format!("{:05}", segment_id));
        std::fs::create_dir_all(&segment_dir).unwrap();
        let events = vec![
            EventFactory::new()
                .with("event_type", event_type)
                .with("context_id", format!("ctx{}", segment_id))
                .with("payload", json!({ "score": segment_id * 10 }))
                .create(),
        ];
        let memtable = MemTableFactory::new()
            .with_capacity(2)
            .with_events(events)
            .create()
            .unwrap();
        let flusher = Flusher::new(
            memtable,
            segment_id,
            &segment_dir,
            registry.clone(),
            Arc::clone(&flush_lock),
        );
        flusher.flush().await.unwrap();

        {
            let mut ids = segment_ids.write().unwrap();
            ids.push(format!("{:05}", segment_id));
        }
    }

    let column_stub = StubCache::new("column_handle");
    let zone_stub = StubCache::new("zone_surf");
    let zone_index_stub = StubCache::new("zone_index");
    let catalog_stub = StubCache::new("index_catalog");
    let column_block_stub = StubCache::new("column_block");
    let handover = Arc::new(CompactionHandover::with_caches(
        0,
        shard_dir.clone(),
        Arc::clone(&segment_ids),
        Arc::clone(&flush_lock),
        Arc::new(column_stub.clone()),
        Arc::new(zone_stub.clone()),
        Arc::new(zone_index_stub.clone()),
        Arc::new(catalog_stub.clone()),
        Arc::new(column_block_stub.clone()),
    ));
    let worker = CompactionWorker::new(
        0,
        shard_dir.clone(),
        registry.clone(),
        Arc::clone(&handover),
    );
    worker.run().await.unwrap();

    let index = SegmentIndex::load(&shard_dir).await.unwrap();
    // Expect 3 entries total: 2 L1 outputs + 1 L0 leftover
    assert_eq!(index.len(), 3);
    let l1_count = index.iter_all().filter(|e| e.id >= 10_000).count();
    let l0_count = index.iter_all().filter(|e| e.id < 10_000).count();
    assert_eq!(l1_count, 2);
    assert_eq!(l0_count, 1);
    // All entries must be for the same uid
    assert!(index.iter_all().all(|e| e.uids == vec![uid.clone()]));

    // segment_ids reflect both new L1 segments and leftover L0
    let ids = segment_ids.read().unwrap().clone();
    assert_eq!(ids.len(), 3);
    assert_eq!(
        ids.iter()
            .filter(|label| label.parse::<u32>().unwrap() >= 10_000)
            .count(),
        2
    );
    assert_eq!(
        ids.iter()
            .filter(|label| label.parse::<u32>().unwrap() < 10_000)
            .count(),
        1
    );

    let expected_labels: Vec<&str> = vec!["00001", "00002", "00003", "00004"]; // column invalidations for inputs
    for label in &expected_labels {
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

    // Confirm compacted outputs retained SuRF filters for numeric fields
    for entry in index.iter_all() {
        if entry.id >= 10_000 {
            let label = format!("{:05}", entry.id);
            let surf_path = shard_dir
                .join(&label)
                .join(format!("{}_{}.zsrf", uid, "score"));
            assert!(surf_path.exists(), "expected SuRF file at {:?}", surf_path);
        }
    }
}
