use crate::command::types::Command;
use crate::engine::core::memory::passive_buffer_set::PassiveBufferSet;
use crate::engine::core::read::flow::shard_pipeline::ShardFlowHandle;
use crate::engine::core::{InflightSegments, MemTable};
use crate::engine::errors::QueryExecutionError;
use crate::engine::query::streaming::StreamingScan;
use crate::engine::schema::registry::SchemaRegistry;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Entry point used by shard workers to start a scan and return the
/// resulting flow handle back to the coordinator.
pub async fn scan(
    command: &Command,
    metadata: Option<std::collections::HashMap<String, String>>,
    registry: &Arc<RwLock<SchemaRegistry>>,
    segment_base_dir: &Path,
    segment_ids: &Arc<std::sync::RwLock<Vec<String>>>,
    memtable: &MemTable,
    passive_buffers: &Arc<PassiveBufferSet>,
    inflight_segments: Option<InflightSegments>,
) -> Result<ShardFlowHandle, QueryExecutionError> {
    let scan = StreamingScan::new(
        command,
        metadata,
        registry,
        segment_base_dir,
        segment_ids,
        memtable,
        passive_buffers,
        inflight_segments,
    )
    .await?;
    scan.execute().await
}
