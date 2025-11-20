use crate::command::types::Command;
use crate::engine::core::Event;
use crate::engine::core::read::flow::shard_pipeline::ShardFlowHandle;
use crate::engine::schema::registry::SchemaRegistry;
use crate::engine::shard::message::ShardMessage;
use std::sync::Arc;
use tokio::sync::{RwLock, oneshot};

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
                metadata: None,
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
