use std::sync::Arc;

use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;

pub struct ShowContext<'a> {
    alias: &'a str,
    shard_manager: &'a ShardManager,
    registry: Arc<tokio::sync::RwLock<SchemaRegistry>>,
}

impl<'a> ShowContext<'a> {
    pub fn new(
        alias: &'a str,
        shard_manager: &'a ShardManager,
        registry: Arc<tokio::sync::RwLock<SchemaRegistry>>,
    ) -> Self {
        Self {
            alias,
            shard_manager,
            registry,
        }
    }

    pub fn alias(&self) -> &str {
        self.alias
    }

    pub fn shard_manager(&self) -> &ShardManager {
        self.shard_manager
    }

    pub fn registry(&self) -> Arc<tokio::sync::RwLock<SchemaRegistry>> {
        Arc::clone(&self.registry)
    }
}
