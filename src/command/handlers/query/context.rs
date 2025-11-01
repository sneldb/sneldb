use std::sync::Arc;

use tokio::sync::RwLock;

use crate::command::types::Command;
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;

/// Shared query execution context passed to each stage of the pipeline.
pub struct QueryContext<'a> {
    pub command: &'a Command,
    pub shard_manager: &'a ShardManager,
    pub registry: Arc<RwLock<SchemaRegistry>>,
}

impl<'a> QueryContext<'a> {
    pub fn new(
        command: &'a Command,
        shard_manager: &'a ShardManager,
        registry: Arc<RwLock<SchemaRegistry>>,
    ) -> Self {
        Self {
            command,
            shard_manager,
            registry,
        }
    }
}
