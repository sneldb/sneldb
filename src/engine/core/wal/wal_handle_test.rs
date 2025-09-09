use crate::engine::core::{WalEntry, WalHandle};
use crate::test_helpers::factories::WalEntryFactory;
use std::fs;
use std::sync::Arc;
use tokio::time::{Duration, sleep};
use tracing::info;

#[tokio::test]
async fn test_spawn_wal_thread_writes_entries_and_shuts_down() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let shard_id = 0;
    let wal_dir = tempfile::tempdir();
    let wal_dir_path = wal_dir.unwrap().path().to_path_buf();

    // Start WAL thread
    let wal_handle = WalHandle::new(shard_id, &wal_dir_path).unwrap();
    let wal = Arc::new(
        wal_handle
            .spawn_wal_thread()
            .expect("Failed to spawn WAL thread"),
    );
    info!("wal: {:?}", wal);

    sleep(Duration::from_millis(100)).await;

    // Prepare and send entries
    let entry1 = WalEntryFactory::new()
        .with("timestamp", 111)
        .with("context_id", "abc")
        .create();
    let entry2 = WalEntryFactory::new()
        .with("timestamp", 222)
        .with("context_id", "def")
        .create();

    wal.append(entry1.clone()).await;
    wal.append(entry2.clone()).await;

    // Give time for WAL thread to process
    sleep(Duration::from_millis(100)).await;

    // Shutdown
    wal.shutdown().await;
    sleep(Duration::from_millis(50)).await;

    // Assert file written
    let wal_dir = wal_dir_path.clone();
    let files: Vec<_> = fs::read_dir(&wal_dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().map(|e| e == "log").unwrap_or(false))
        .collect();

    assert_eq!(files.len(), 1);
    let contents = fs::read_to_string(&files[0]).unwrap();
    let lines: Vec<_> = contents.lines().collect();
    assert_eq!(lines.len(), 2);

    let parsed1: WalEntry = serde_json::from_str(lines[0]).unwrap();
    let parsed2: WalEntry = serde_json::from_str(lines[1]).unwrap();

    assert_eq!(parsed1.timestamp, 111);
    assert_eq!(parsed1.context_id, "abc");
    assert_eq!(parsed2.timestamp, 222);
    assert_eq!(parsed2.context_id, "def");

    // Clean up
    fs::remove_dir_all(&wal_dir).unwrap();
}
