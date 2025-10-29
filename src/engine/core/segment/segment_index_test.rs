use crate::engine::core::compaction::policy::{CompactionPolicy, KWayCountPolicy};
use crate::engine::core::{SegmentEntry, SegmentIndex};
use crate::shared::storage_header::{BinaryHeader, FileKind};
use std::fs::File;
use std::io::{BufWriter, Write};
use tempfile::tempdir;

#[tokio::test]
async fn test_segment_index_e2e() {
    // Setup
    let temp = tempdir().unwrap();
    let shard_dir = temp.path().to_path_buf();
    let uid = "test_event_type".to_string();
    let label = "00001".to_string();
    let counter = 1;
    let level = 0;

    // Create dummy SegmentEntry
    let entry = SegmentEntry {
        id: counter,
        uids: vec![uid.clone()],
    };

    // Load or create SegmentIndex
    let mut index = SegmentIndex::load(&shard_dir).await.unwrap();
    assert!(index.is_empty());

    // Append and verify
    index.insert_entry(entry.clone());
    index.save(&shard_dir).await.unwrap();
    assert_eq!(index.len(), 1);
    let inserted = index.iter_all().next().unwrap();
    assert_eq!(format!("{:05}", inserted.id), label);

    // Reload and verify persistence
    let reloaded = SegmentIndex::load(&shard_dir).await.unwrap();
    assert_eq!(reloaded.len(), 1);
    let reloaded_entry = reloaded.iter_all().next().unwrap();
    assert_eq!(reloaded_entry.uids, vec![uid.clone()]);

    // Check list_for_uid
    let listed = reloaded.list_for_uid(&uid);
    assert_eq!(listed.len(), 1);
    assert_eq!(format!("{:05}", listed[0].id), label);

    // Check all_labels
    assert_eq!(reloaded.all_labels(), vec![label.clone()]);

    // Check current_level
    assert_eq!(reloaded.current_level(), level);

    let level_entries: Vec<_> = reloaded.entries_for_level(0).map(|e| e.id).collect();
    assert_eq!(level_entries, vec![counter]);

    for i in 2..=8 {
        let seg = SegmentEntry {
            id: i,
            uids: vec![uid.clone()],
        };
        index.insert_entry(seg);
    }
    index.save(&shard_dir).await.unwrap();

    let level_stats = index.level_stats();
    assert_eq!(level_stats.len(), 1);
    assert_eq!(level_stats[0].level, 0);
    assert_eq!(level_stats[0].count, 8);

    // Policy-based planning: with compaction_k=2 in test config and 8 L0 entries,
    // expect 4 merge plans for the same uid
    let policy = KWayCountPolicy { k: 2 };
    let plans = policy.plan(&index);
    assert_eq!(plans.len(), 4);
}

#[tokio::test]
async fn normalizes_legacy_serialized_ordering_on_load() {
    let temp = tempdir().unwrap();
    let shard_dir = temp.path();

    let path = shard_dir.join("segments.idx");
    let file = File::create(&path).unwrap();
    let mut writer = BufWriter::new(file);
    let header = BinaryHeader::new(FileKind::ShardSegmentIndex.magic(), 1, 0);
    header.write_to(&mut writer).unwrap();

    let legacy_entries = vec![
        SegmentEntry {
            id: 20_002,
            uids: vec!["u3".into()],
        },
        SegmentEntry {
            id: 5,
            uids: vec!["u1".into()],
        },
        SegmentEntry {
            id: 10_001,
            uids: vec!["u2".into()],
        },
        SegmentEntry {
            id: 10_000,
            uids: vec!["u2".into()],
        },
    ];

    bincode::serialize_into(&mut writer, &legacy_entries).unwrap();
    writer.flush().unwrap();

    let index = SegmentIndex::load(shard_dir).await.unwrap();
    let normalized: Vec<u32> = index.iter_all().map(|e| e.id).collect();
    assert_eq!(normalized, vec![5, 10_000, 10_001, 20_002]);

    index.save(shard_dir).await.unwrap();
    let reloaded = SegmentIndex::load(shard_dir).await.unwrap();
    let reloaded_ids: Vec<u32> = reloaded.iter_all().map(|e| e.id).collect();
    assert_eq!(reloaded_ids, normalized);
}

#[tokio::test]
async fn remove_labels_returns_removed_and_skips_missing() {
    let temp = tempdir().unwrap();
    let shard_dir = temp.path();

    let mut index = SegmentIndex::load(shard_dir).await.unwrap();
    index.insert_entry(SegmentEntry {
        id: 1,
        uids: vec!["a".into()],
    });
    index.insert_entry(SegmentEntry {
        id: 10_000,
        uids: vec!["b".into()],
    });
    index.save(shard_dir).await.unwrap();

    let removed = index.remove_labels(["00001", "10000", "99999"].iter().copied());
    assert_eq!(removed.len(), 2);
    assert!(removed.iter().any(|e| e.id == 1));
    assert!(removed.iter().any(|e| e.id == 10_000));

    index.save(shard_dir).await.unwrap();
    let reloaded = SegmentIndex::load(shard_dir).await.unwrap();
    assert!(reloaded.is_empty());
}

#[tokio::test]
async fn entries_for_level_and_stats_reflect_hierarchy() {
    let temp = tempdir().unwrap();
    let shard_dir = temp.path();

    let mut index = SegmentIndex::load(shard_dir).await.unwrap();
    index.insert_entry(SegmentEntry {
        id: 2,
        uids: vec!["x".into()],
    });
    index.insert_entry(SegmentEntry {
        id: 10_000,
        uids: vec!["y".into()],
    });
    index.insert_entry(SegmentEntry {
        id: 10_001,
        uids: vec!["z".into()],
    });

    let l0: Vec<u32> = index.entries_for_level(0).map(|e| e.id).collect();
    assert_eq!(l0, vec![2]);

    let l1: Vec<u32> = index.entries_for_level(1).map(|e| e.id).collect();
    assert_eq!(l1, vec![10_000, 10_001]);

    let stats = index.level_stats();
    assert_eq!(stats.len(), 2);
    assert_eq!(stats[0].level, 0);
    assert_eq!(stats[0].count, 1);
    assert_eq!(stats[1].level, 1);
    assert_eq!(stats[1].count, 2);
}
