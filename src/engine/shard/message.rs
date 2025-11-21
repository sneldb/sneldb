use crate::command::types::Command;
use crate::engine::core::Event;
use crate::engine::core::read::flow::shard_pipeline::ShardFlowHandle;
use crate::engine::schema::registry::SchemaRegistry;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use tokio::sync::oneshot;

pub enum ShardMessage {
    Store(Event, Arc<RwLock<SchemaRegistry>>),
    Flush {
        registry: Arc<RwLock<SchemaRegistry>>,
        completion: oneshot::Sender<Result<(), String>>,
    },
    AwaitFlush {
        completion: oneshot::Sender<Result<(), String>>,
    },
    QueryStream {
        command: Command,
        metadata: Option<HashMap<String, String>>,
        response: oneshot::Sender<Result<ShardFlowHandle, String>>,
        registry: Arc<RwLock<SchemaRegistry>>,
    },
    Shutdown {
        completion: oneshot::Sender<Result<(), String>>,
    },
}
