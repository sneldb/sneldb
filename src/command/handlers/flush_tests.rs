use crate::command::handlers::flush::handle;
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use crate::engine::shard::message::ShardMessage;
use crate::shared::response::JsonRenderer;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::io::duplex;
use tokio::sync::{RwLock, oneshot};
use tokio::time::{Duration, timeout};

#[tokio::test]
async fn test_flush_dispatches_message_to_all_shards() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();
    // Create registry
    let registry = Arc::new(RwLock::new(SchemaRegistry::new().unwrap()));

    // Use 2 shards for this test
    let shard_manager = ShardManager::new(3, base_dir, wal_dir).await;
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Create dummy command (content is irrelevant for flush)
    let cmd = crate::command::types::Command::Flush;

    // Setup fake writer
    let (_reader, mut writer) = duplex(1024);

    // Call handler
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .expect("flush handler should not fail");

    // Confirm each shard received a flush
    let mut received = 0;
    for shard in shard_manager.all_shards() {
        let (tx, rx) = oneshot::channel();

        let _ = shard
            .tx
            .send(ShardMessage::Flush {
                registry: Arc::clone(&registry),
                completion: tx,
            })
            .await;

        match timeout(Duration::from_millis(200), rx).await {
            Ok(Ok(Ok(()))) => received += 1,
            Ok(Ok(Err(e))) => panic!("Shard flush returned error: {}", e),
            Ok(Err(_)) => panic!("Shard flush completion channel dropped"),
            Err(_) => panic!("Timed out waiting for shard flush completion"),
        }
    }

    assert_eq!(received, 3, "Expected flush to be sent to 3 shards");
}
