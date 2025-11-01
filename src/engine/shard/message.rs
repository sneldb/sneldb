use crate::command::types::Command;
use crate::engine::core::Event;
use crate::engine::core::read::flow::shard_pipeline::ShardFlowHandle;
use crate::engine::core::read::result::QueryResult;
use crate::engine::schema::registry::SchemaRegistry;
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
    Query(Command, Sender<QueryResult>, Arc<RwLock<SchemaRegistry>>),
    QueryStream {
        command: Command,
        response: oneshot::Sender<Result<ShardFlowHandle, String>>,
        registry: Arc<RwLock<SchemaRegistry>>,
    },
    Replay(Command, Sender<Vec<Event>>, Arc<RwLock<SchemaRegistry>>),
    Shutdown {
        completion: oneshot::Sender<Result<(), String>>,
    },
}
