use crate::engine::core::WalRecovery;
use crate::test_helpers::factories::EventFactory;
use crate::test_helpers::factories::ShardContextFactory;
use std::fs::read_dir;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::info;

#[tokio::test]
async fn test_wal_recovery_recovers_inserted_events() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let shard_id = 0;
    let wal_dir = tempfile::tempdir();
    let wal_dir_path = wal_dir.unwrap().path().to_path_buf();

    // Arrange
    let ctx = ShardContextFactory::new()
        .with_id(shard_id)
        .with_wal_dir(wal_dir_path.clone())
        .create();

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let events = vec![
        EventFactory::new()
            .with("context_id", "ctx-wal-1")
            .with("timestamp", now)
            .with("event_type", "wal_event")
            .create(),
        EventFactory::new()
            .with("context_id", "ctx-wal-2")
            .with("timestamp", now + 1)
            .with("event_type", "wal_event")
            .create(),
    ];

    for event in &events {
        let entry = crate::engine::core::WalEntry::from_event(event);
        ctx.wal.as_ref().unwrap().append(entry).await;
    }

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    ctx.wal.as_ref().unwrap().shutdown().await;

    // Act: Recreate context and recover from WAL
    let mut ctx_recovered = ShardContextFactory::new()
        .with_id(shard_id)
        .with_wal_dir(wal_dir_path.clone())
        .create();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let wal_recovery = WalRecovery::new(shard_id, &wal_dir_path);
    wal_recovery
        .recover(&mut ctx_recovered)
        .expect("WAL recovery failed");
    // Assert: Events recovered into the memtable

    assert_eq!(ctx_recovered.memtable.len(), events.len());

    let recovered_ids: Vec<_> = ctx_recovered
        .memtable
        .iter()
        .map(|e| e.context_id.clone())
        .collect();

    assert!(recovered_ids.contains(&"ctx-wal-1".to_string()));
    assert!(recovered_ids.contains(&"ctx-wal-2".to_string()));

    // (Optional) debug log
    for event in ctx_recovered.memtable.iter() {
        info!("Recovered: {:?}", event);
    }

    let shard_path = wal_dir_path.clone();
    let logs: Vec<_> = read_dir(&shard_path)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().unwrap_or_default() == "log")
        .collect();

    assert!(!logs.is_empty(), "WAL files were not created");
}
