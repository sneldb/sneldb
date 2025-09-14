use crate::test_helpers::factories::SnapshotMetaFactory;

#[test]
fn snapshot_meta_factory_creates_expected_defaults() {
    let meta = SnapshotMetaFactory::new().create();

    assert_eq!(meta.uid, "uid-default");
    assert_eq!(meta.context_id, "ctx-1");
    assert_eq!(meta.from_ts, 100);
    assert_eq!(meta.to_ts, 200);
}

#[test]
fn snapshot_meta_factory_chain_and_list() {
    let single = SnapshotMetaFactory::new()
        .with_uid("uid-x")
        .with_context_id("ctx-x")
        .with_range(1000, 1500)
        .create();

    assert_eq!(single.uid, "uid-x");
    assert_eq!(single.context_id, "ctx-x");
    assert_eq!(single.from_ts, 1000);
    assert_eq!(single.to_ts, 1500);

    let list = SnapshotMetaFactory::new()
        .with_uid("batch")
        .with_context_id("ctx")
        .with_range(10, 20)
        .create_list(3);

    assert_eq!(list.len(), 3);

    assert_eq!(list[0].uid, "batch-1");
    assert_eq!(list[1].uid, "batch-2");
    assert_eq!(list[2].uid, "batch-3");

    assert_eq!(list[0].context_id, "ctx-1");
    assert_eq!(list[1].context_id, "ctx-2");
    assert_eq!(list[2].context_id, "ctx-3");

    // Ranges should increase by +10 each step
    assert_eq!(list[0].from_ts, 10);
    assert_eq!(list[0].to_ts, 20);
    assert_eq!(list[1].from_ts, 20);
    assert_eq!(list[1].to_ts, 30);
    assert_eq!(list[2].from_ts, 30);
    assert_eq!(list[2].to_ts, 40);
}
