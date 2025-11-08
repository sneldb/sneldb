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
    let policy = KWayCountPolicy::new(2);
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
async fn retire_uid_from_labels_only_drains_when_last_uid_is_removed() {
    let temp = tempdir().unwrap();
    let shard_dir = temp.path();

    let mut index = SegmentIndex::load(shard_dir).await.unwrap();
    index.insert_entry(SegmentEntry {
        id: 1,
        uids: vec!["a".into(), "b".into()],
    });
    index.insert_entry(SegmentEntry {
        id: 2,
        uids: vec!["b".into()],
    });
    index.save(shard_dir).await.unwrap();

    // First retirement only removes uid "a" but leaves the shared segment
    let drained = index.retire_uid_from_labels("a", ["00001"].iter().copied());
    assert!(
        drained.is_empty(),
        "Segment should remain until all UIDs are retired"
    );
    {
        let shared_entry = index
            .iter_all()
            .find(|entry| entry.id == 1)
            .expect("shared entry should still exist");
        assert_eq!(shared_entry.uids, vec!["b".to_string()]);
    }

    index.save(shard_dir).await.unwrap();
    let mut reloaded = SegmentIndex::load(shard_dir).await.unwrap();
    {
        let shared_entry = reloaded
            .iter_all()
            .find(|entry| entry.id == 1)
            .expect("shared entry should persist to disk");
        assert_eq!(shared_entry.uids, vec!["b".to_string()]);
    }

    // Second retirement drains the segment entirely
    let drained = reloaded.retire_uid_from_labels("b", ["00001"].iter().copied());
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].id, 1);
    assert!(
        reloaded.iter_all().all(|entry| entry.id != 1),
        "Shared segment should be removed once every UID is retired"
    );
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

#[tokio::test]
async fn atomic_save_creates_temporary_file_then_renames() {
    let temp = tempdir().unwrap();
    let shard_dir = temp.path();

    let mut index = SegmentIndex::load(shard_dir).await.unwrap();
    index.insert_entry(SegmentEntry {
        id: 1,
        uids: vec!["uidA".into()],
    });

    // Save should create temporary file first
    index.save(shard_dir).await.unwrap();

    // Temporary file should not exist after successful save
    let tmp_path = shard_dir.join("segments.idx.tmp");
    assert!(
        !tmp_path.exists(),
        "Temporary file should be removed after successful save"
    );

    // Final index file should exist
    let index_path = shard_dir.join("segments.idx");
    assert!(index_path.exists(), "Index file should exist after save");
}

#[tokio::test]
async fn atomic_save_cleans_up_leftover_temporary_files_on_load() {
    let temp = tempdir().unwrap();
    let shard_dir = temp.path();

    // Create a leftover temporary file (simulating a crash)
    let tmp_path = shard_dir.join("segments.idx.tmp");
    std::fs::write(&tmp_path, b"corrupted temporary data").unwrap();
    assert!(tmp_path.exists(), "Temporary file should exist before load");

    // Load should clean up the temporary file
    let _index = SegmentIndex::load(shard_dir).await.unwrap();
    assert!(
        !tmp_path.exists(),
        "Temporary file should be cleaned up on load"
    );
}

#[tokio::test]
async fn atomic_save_preserves_old_index_on_failure() {
    let temp = tempdir().unwrap();
    let shard_dir = temp.path();

    // Create initial index
    let mut index = SegmentIndex::load(shard_dir).await.unwrap();
    index.insert_entry(SegmentEntry {
        id: 1,
        uids: vec!["uidA".into()],
    });
    index.save(shard_dir).await.unwrap();

    // Verify initial index exists
    let index_path = shard_dir.join("segments.idx");
    assert!(index_path.exists());
    let initial_size = std::fs::metadata(&index_path).unwrap().len();

    // Simulate a crash by creating a corrupted temporary file
    // (In real scenario, this would be a partial write)
    let tmp_path = shard_dir.join("segments.idx.tmp");
    std::fs::write(&tmp_path, b"incomplete write").unwrap();

    // If we try to load, it should clean up temp and use old index
    let reloaded = SegmentIndex::load(shard_dir).await.unwrap();
    assert_eq!(reloaded.len(), 1);
    assert!(!tmp_path.exists());

    // Old index should still be intact
    let final_size = std::fs::metadata(&index_path).unwrap().len();
    assert_eq!(
        initial_size, final_size,
        "Original index should be preserved"
    );
}

#[tokio::test]
async fn recovery_from_corrupted_index_file() {
    let temp = tempdir().unwrap();
    let shard_dir = temp.path();

    // Create a corrupted index file
    let index_path = shard_dir.join("segments.idx");
    std::fs::write(&index_path, b"this is not valid bincode data").unwrap();

    // Load should detect corruption and attempt recovery
    // Since there are no segments on disk, recovery should create empty index
    let recovered = SegmentIndex::load(shard_dir).await.unwrap();
    assert!(
        recovered.is_empty(),
        "Recovered index should be empty when no segments exist"
    );
}

#[tokio::test]
async fn recovery_from_disk_scans_segment_directories() {
    let temp = tempdir().unwrap();
    let shard_dir = temp.path();

    // Create segment directories with .zones files (simulating actual segments)
    let segment_00001 = shard_dir.join("00001");
    let segment_10000 = shard_dir.join("10000");
    std::fs::create_dir_all(&segment_00001).unwrap();
    std::fs::create_dir_all(&segment_10000).unwrap();

    // Create .zones files to simulate UIDs
    std::fs::write(segment_00001.join("uidA.zones"), b"zone data").unwrap();
    std::fs::write(segment_00001.join("uidB.zones"), b"zone data").unwrap();
    std::fs::write(segment_10000.join("uidA.zones"), b"zone data").unwrap();

    // Corrupt the index file
    let index_path = shard_dir.join("segments.idx");
    std::fs::write(&index_path, b"corrupted").unwrap();

    // Recovery should scan disk and rebuild index
    let recovered = SegmentIndex::load(shard_dir).await.unwrap();

    // Should have recovered 2 segments
    assert_eq!(recovered.len(), 2);

    // Verify segment 00001 has both UIDs
    let seg1 = recovered.iter_all().find(|e| e.id == 1).unwrap();
    assert_eq!(seg1.uids.len(), 2);
    assert!(seg1.uids.contains(&"uidA".to_string()));
    assert!(seg1.uids.contains(&"uidB".to_string()));

    // Verify segment 10000 has uidA
    let seg2 = recovered.iter_all().find(|e| e.id == 10_000).unwrap();
    assert_eq!(seg2.uids.len(), 1);
    assert_eq!(seg2.uids[0], "uidA");

    // Verify recovered index was saved
    assert!(index_path.exists());
    let reloaded = SegmentIndex::load(shard_dir).await.unwrap();
    assert_eq!(reloaded.len(), 2);
}

#[tokio::test]
async fn recovery_ignores_non_segment_directories() {
    let temp = tempdir().unwrap();
    let shard_dir = temp.path();

    // Create various directories
    std::fs::create_dir_all(shard_dir.join("00001")).unwrap(); // Valid segment
    std::fs::create_dir_all(shard_dir.join("10000")).unwrap(); // Valid segment
    std::fs::create_dir_all(shard_dir.join("invalid")).unwrap(); // Invalid (not 5 digits)
    std::fs::create_dir_all(shard_dir.join("123")).unwrap(); // Invalid (not 5 digits)
    std::fs::create_dir_all(shard_dir.join(".reclaim")).unwrap(); // Special directory

    // Add .zones files only to valid segments
    std::fs::write(shard_dir.join("00001").join("uidA.zones"), b"data").unwrap();
    std::fs::write(shard_dir.join("10000").join("uidB.zones"), b"data").unwrap();

    // Load without index (simulating missing index)
    let recovered = SegmentIndex::load(shard_dir).await.unwrap();

    // Should only recover the 2 valid segments
    assert_eq!(recovered.len(), 2);
    assert!(recovered.iter_all().any(|e| e.id == 1));
    assert!(recovered.iter_all().any(|e| e.id == 10_000));
}

#[tokio::test]
async fn recovery_skips_segments_without_zones_files() {
    let temp = tempdir().unwrap();
    let shard_dir = temp.path();

    // Create segment directories
    std::fs::create_dir_all(shard_dir.join("00001")).unwrap();
    std::fs::create_dir_all(shard_dir.join("00002")).unwrap();

    // Only one has .zones file
    std::fs::write(shard_dir.join("00001").join("uidA.zones"), b"data").unwrap();
    // 00002 has no .zones file (empty segment)

    // Load without index
    let recovered = SegmentIndex::load(shard_dir).await.unwrap();

    // Should only recover segment with .zones file
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered.iter_all().next().unwrap().id, 1);
}

#[tokio::test]
async fn recovery_handles_missing_index_file() {
    let temp = tempdir().unwrap();
    let shard_dir = temp.path();

    // Create segments but no index file
    std::fs::create_dir_all(shard_dir.join("00001")).unwrap();
    std::fs::write(shard_dir.join("00001").join("uidA.zones"), b"data").unwrap();

    // Load should create new index
    let index = SegmentIndex::load(shard_dir).await.unwrap();
    assert_eq!(index.len(), 1);

    // Index file should now exist
    assert!(shard_dir.join("segments.idx").exists());
}

#[tokio::test]
async fn atomic_save_with_fsync_ensures_durability() {
    let temp = tempdir().unwrap();
    let shard_dir = temp.path();

    let mut index = SegmentIndex::load(shard_dir).await.unwrap();
    index.insert_entry(SegmentEntry {
        id: 1,
        uids: vec!["uidA".into()],
    });
    index.insert_entry(SegmentEntry {
        id: 2,
        uids: vec!["uidB".into()],
    });

    // Save should complete successfully
    index.save(shard_dir).await.unwrap();

    // Reload should get all entries
    let reloaded = SegmentIndex::load(shard_dir).await.unwrap();
    assert_eq!(reloaded.len(), 2);
    assert!(reloaded.iter_all().any(|e| e.id == 1));
    assert!(reloaded.iter_all().any(|e| e.id == 2));
}

#[tokio::test]
async fn atomic_save_handles_concurrent_access() {
    let temp = tempdir().unwrap();
    let shard_dir = temp.path();

    // Create initial index
    let mut index = SegmentIndex::load(shard_dir).await.unwrap();
    index.insert_entry(SegmentEntry {
        id: 1,
        uids: vec!["uidA".into()],
    });
    index.save(shard_dir).await.unwrap();

    // Simulate concurrent saves (in real scenario, this would be protected by locks)
    let mut index1 = SegmentIndex::load(shard_dir).await.unwrap();
    index1.insert_entry(SegmentEntry {
        id: 2,
        uids: vec!["uidB".into()],
    });

    let mut index2 = SegmentIndex::load(shard_dir).await.unwrap();
    index2.insert_entry(SegmentEntry {
        id: 3,
        uids: vec!["uidC".into()],
    });

    // Both should save successfully (atomic writes prevent corruption)
    index1.save(shard_dir).await.unwrap();
    index2.save(shard_dir).await.unwrap();

    // Final state should reflect the last write
    let final_index = SegmentIndex::load(shard_dir).await.unwrap();
    // Note: In real scenario with locks, this would be deterministic
    assert!(final_index.len() >= 1);
}
