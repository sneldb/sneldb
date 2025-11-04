use std::collections::HashMap;
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
    pub metadata: HashMap<String, String>,
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
            metadata: HashMap::new(),
        }
    }

    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }
}
