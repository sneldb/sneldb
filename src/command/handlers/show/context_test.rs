use std::sync::Arc;

use super::context::ShowContext;
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;

#[test]
fn exposes_alias_and_dependencies() {
    let shard_manager = ShardManager { shards: Vec::new() };

    let temp_dir = tempfile::tempdir().expect("tempdir");
    let registry =
        SchemaRegistry::new_with_path(temp_dir.path().join("schemas.bin")).expect("registry");
    let registry = Arc::new(tokio::sync::RwLock::new(registry));

    let context = ShowContext::new("orders", &shard_manager, Arc::clone(&registry));

    assert_eq!(context.alias(), "orders");
    let cloned_registry = context.registry();
    assert!(Arc::ptr_eq(&cloned_registry, &registry));
    assert!(std::ptr::eq(context.shard_manager(), &shard_manager));
}
