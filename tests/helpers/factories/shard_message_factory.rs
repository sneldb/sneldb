use crate::command::types::Command;
use crate::engine::core::Event;
use crate::engine::core::read::flow::shard_pipeline::ShardFlowHandle;
use crate::engine::core::read::result::QueryResult;
use crate::engine::schema::registry::SchemaRegistry;
use crate::engine::shard::message::ShardMessage;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc, oneshot};

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

    pub fn flush(&self) -> (ShardMessage, oneshot::Receiver<Result<(), String>>) {
        let (tx, rx) = oneshot::channel();
        (
            ShardMessage::Flush {
                registry: Arc::clone(&self.registry),
                completion: tx,
            },
            rx,
        )
    }

    pub fn query(&self, command: Command, tx: mpsc::Sender<QueryResult>) -> ShardMessage {
        ShardMessage::Query(command, tx, Arc::clone(&self.registry))
    }

    pub fn replay(&self, command: Command, tx: mpsc::Sender<Vec<Event>>) -> ShardMessage {
        ShardMessage::Replay(command, tx, Arc::clone(&self.registry))
    }

    pub fn query_stream(
        &self,
        command: Command,
    ) -> (
        ShardMessage,
        oneshot::Receiver<Result<ShardFlowHandle, String>>,
    ) {
        let (tx, rx) = oneshot::channel();
        (
            ShardMessage::QueryStream {
                command,
                response: tx,
                registry: Arc::clone(&self.registry),
            },
            rx,
        )
    }

    pub fn shutdown(&self) -> (ShardMessage, oneshot::Receiver<Result<(), String>>) {
        let (tx, rx) = oneshot::channel();
        (ShardMessage::Shutdown { completion: tx }, rx)
    }
}
