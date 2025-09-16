use tempfile::tempdir;

use crate::engine::core::Event;
use crate::engine::core::snapshot::snapshot_meta::SnapshotMeta;
use crate::engine::core::snapshot::snapshot_reader::SnapshotReader;
use crate::engine::core::snapshot::snapshot_registry::{
    ReplayStrategy, SnapshotKey, SnapshotRegistry,
};
use crate::engine::core::snapshot::snapshot_writer::SnapshotWriter;
use crate::test_helpers::factories::EventFactory;

#[test]
fn registry_upsert_save_load_roundtrip() {
    let tmp = tempdir().unwrap();
    let base = tmp.path().join("snapshots");

    let mut reg = SnapshotRegistry::new(base.clone());

    // Upsert two metas
    let key1 = SnapshotKey::new("uid-1", "ctx-1");
    let key2 = SnapshotKey::new("uid-2", "ctx-2");
    reg.upsert(key1.clone(), 100, 200).unwrap();
    reg.upsert(key2.clone(), 150, 250).unwrap();

    // Save and reload
    reg.save().unwrap();
    let mut reg2 = SnapshotRegistry::new(base.clone());
    reg2.load().unwrap();

    let m1 = reg2.get_meta(&key1).cloned().unwrap();
    let m2 = reg2.get_meta(&key2).cloned().unwrap();

    assert_eq!(m1.uid, "uid-1");
    assert_eq!(m1.context_id, "ctx-1");
    assert_eq!(m1.from_ts, 100);
    assert_eq!(m1.to_ts, 200);

    assert_eq!(m2.uid, "uid-2");
    assert_eq!(m2.context_id, "ctx-2");
    assert_eq!(m2.from_ts, 150);
    assert_eq!(m2.to_ts, 250);
}

#[test]
fn strategy_uses_snapshot_only_when_since_within_range() {
    let tmp = tempdir().unwrap();
    let base = tmp.path().join("snapshots");
    let mut reg = SnapshotRegistry::new(base);

    let key = SnapshotKey::new("uid-x", "ctx-x");
    reg.upsert(key.clone(), 1_000, 2_000).unwrap();
    reg.save().unwrap();

    // since >= from_ts => UseSnapshot
    match reg.decide_replay_strategy(&key, 1_500) {
        ReplayStrategy::UseSnapshot {
            start_ts,
            end_ts,
            path,
        } => {
            assert_eq!(start_ts, 1_000);
            assert_eq!(end_ts, 2_000);
            assert!(path.ends_with("uid-x__ctx-x.snp"));
        }
        _ => panic!("expected UseSnapshot"),
    }

    // since < from_ts => IgnoreSnapshot
    match reg.decide_replay_strategy(&key, 999) {
        ReplayStrategy::IgnoreSnapshot => {}
        _ => panic!("expected IgnoreSnapshot"),
    }
}

#[test]
fn refresh_snapshot_writes_file_and_updates_meta() {
    let tmp = tempdir().unwrap();
    let base = tmp.path().join("snapshots");
    let mut reg = SnapshotRegistry::new(base.clone());

    let key = SnapshotKey::new("uid-z", "ctx-z");

    // Build small set of events and refresh
    let events: Vec<Event> = EventFactory::new()
        .with("event_type", "alpha")
        .with("context_id", "ctx-z")
        .create_list(3);

    reg.refresh_snapshot(key.clone(), 10, 20, &events).unwrap();

    // Validate meta persisted
    let mut reg2 = SnapshotRegistry::new(base.clone());
    reg2.load().unwrap();
    let meta = reg2.get_meta(&key).cloned().unwrap();
    assert_eq!(meta.uid, "uid-z");
    assert_eq!(meta.context_id, "ctx-z");
    assert_eq!(meta.from_ts, 10);
    assert_eq!(meta.to_ts, 20);

    // Validate file exists and can be read back
    let path = reg2.get_data_path(&key).expect("path");
    let loaded = SnapshotReader::new(&path).read_all().unwrap();
    assert_eq!(loaded.len(), 3);

    // Refresh again with different range and different events
    let events2: Vec<Event> = EventFactory::new()
        .with("event_type", "beta")
        .with("context_id", "ctx-z")
        .create_list(2);
    reg2.refresh_snapshot(key.clone(), 100, 200, &events2)
        .unwrap();

    let meta2 = reg2.get_meta(&key).cloned().unwrap();
    assert_eq!(meta2.from_ts, 100);
    assert_eq!(meta2.to_ts, 200);
    let loaded2 = SnapshotReader::new(&reg2.get_data_path(&key).unwrap())
        .read_all()
        .unwrap();
    assert_eq!(loaded2.len(), 2);
}
