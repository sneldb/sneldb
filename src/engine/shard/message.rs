use crate::command::types::Command;
use crate::engine::core::Event;
use crate::engine::core::read::flow::shard_pipeline::ShardFlowHandle;
use crate::engine::core::read::result::QueryResult;
use crate::engine::schema::registry::SchemaRegistry;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::sync::mpsc::Sender;

use tokio::sync::oneshot;

pub enum ShardMessage {
    Store(Event, Arc<RwLock<SchemaRegistry>>),
    Flush {
        registry: Arc<RwLock<SchemaRegistry>>,
        completion: oneshot::Sender<Result<(), String>>,
    },
    Query {
        command: Command,
        metadata: Option<HashMap<String, String>>,
        tx: Sender<QueryResult>,
        registry: Arc<RwLock<SchemaRegistry>>,
    },
    QueryStream {
        command: Command,
        metadata: Option<HashMap<String, String>>,
        response: oneshot::Sender<Result<ShardFlowHandle, String>>,
        registry: Arc<RwLock<SchemaRegistry>>,
    },
    // Note: Replay has been removed - REPLAY commands now use QueryStream
    Shutdown {
        completion: oneshot::Sender<Result<(), String>>,
    },
}
