use crate::command::types::Command;
use crate::engine::core::Event;
use crate::engine::core::read::result::QueryResult;
use crate::engine::schema::registry::SchemaRegistry;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::sync::mpsc::Sender;

use crate::engine::core::MemTable;

pub enum ShardMessage {
    Store(Event, Arc<RwLock<SchemaRegistry>>),
    Flush(
        Sender<Vec<Event>>,
        Arc<RwLock<SchemaRegistry>>,
        Arc<tokio::sync::Mutex<MemTable>>,
    ),
    Query(Command, Sender<QueryResult>, Arc<RwLock<SchemaRegistry>>),
    Replay(Command, Sender<Vec<Event>>, Arc<RwLock<SchemaRegistry>>),
}
