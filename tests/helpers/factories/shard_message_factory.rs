use crate::command::types::Command;
use crate::engine::core::Event;
use crate::engine::core::MemTable;
use crate::engine::core::read::result::QueryResult;
use crate::engine::schema::registry::SchemaRegistry;
use crate::engine::shard::message::ShardMessage;
use crate::shared::config::CONFIG;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};

pub struct ShardMessageFactory {
    registry: Arc<RwLock<SchemaRegistry>>,
}

impl ShardMessageFactory {
    pub fn new(registry: Arc<RwLock<SchemaRegistry>>) -> Self {
        Self { registry }
    }

    pub fn store(&self, event: Event) -> ShardMessage {
        ShardMessage::Store(event, Arc::clone(&self.registry))
    }

    pub fn flush(&self, tx: mpsc::Sender<Vec<Event>>) -> ShardMessage {
        ShardMessage::Flush(
            tx,
            Arc::clone(&self.registry),
            Arc::new(tokio::sync::Mutex::new(MemTable::new(
                CONFIG.engine.flush_threshold,
            ))),
        )
    }

    pub fn query(&self, command: Command, tx: mpsc::Sender<QueryResult>) -> ShardMessage {
        ShardMessage::Query(command, tx, Arc::clone(&self.registry))
    }

    pub fn replay(&self, command: Command, tx: mpsc::Sender<Vec<Event>>) -> ShardMessage {
        ShardMessage::Replay(command, tx, Arc::clone(&self.registry))
    }
}
