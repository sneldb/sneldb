use crate::engine::schema::registry::SchemaRegistry;
use crate::engine::shard::ShardManager;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn test_shard_manager_initialization_and_routing() {
    // Setup

    let registry = Arc::new(tokio::sync::RwLock::new(
        SchemaRegistry::new().expect("Failed to initialize SchemaRegistry"),
    ));
    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();
    let num_shards = 4;
    let manager = ShardManager::new(num_shards, Arc::clone(&registry), base_dir, wal_dir).await;

    // Verify correct number of shards created
    assert_eq!(manager.all_shards().len(), num_shards);

    // Check that context_id is deterministically mapped
    let s1 = manager.get_shard("ctx-foo") as *const _;
    let s2 = manager.get_shard("ctx-foo") as *const _;
    assert_eq!(s1, s2, "Same context_id should map to same shard");

    // Check different context_ids go to different shards
    let s3 = manager.get_shard("ctx-bar") as *const _;
    assert_ne!(s1, s3, "Different context_id may map to different shard");

    // Check index boundaries
    for id in 0..num_shards {
        let key = format!("key-{}", id * 1000);
        let shard = manager.get_shard(&key);
        assert!(
            shard.id < num_shards,
            "Returned shard id must be within valid range"
        );
    }
}
