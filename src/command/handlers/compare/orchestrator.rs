use std::sync::Arc;

use crate::command::handlers::query_batch_stream::QueryBatchStream;
use crate::command::types::{Command, QueryCommand};
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use futures::future::join_all;
use tokio::sync::RwLock;

use super::merge::ComparisonStreamMerger;
use crate::command::handlers::query::QueryExecutionPipeline;

pub struct ComparisonExecutionPipeline<'a> {
    queries: &'a [QueryCommand],
    shard_manager: &'a ShardManager,
    registry: Arc<RwLock<SchemaRegistry>>,
}

impl<'a> ComparisonExecutionPipeline<'a> {
    pub fn new(
        queries: &'a [QueryCommand],
        shard_manager: &'a ShardManager,
        registry: Arc<RwLock<SchemaRegistry>>,
    ) -> Self {
        Self {
            queries,
            shard_manager,
            registry,
        }
    }

    pub async fn execute_streaming(&self) -> Result<Option<QueryBatchStream>, String> {
        if self.queries.is_empty() {
            return Err("Comparison query requires at least one query".to_string());
        }

        // Execute all queries in parallel
        let mut query_futures = Vec::new();
        for query_cmd in self.queries {
            let query_cmd_clone = query_cmd.clone();
            let command = Command::from(query_cmd_clone);
            let shard_manager = self.shard_manager;
            let registry = Arc::clone(&self.registry);
            query_futures.push(async move {
                let pipeline = QueryExecutionPipeline::new(&command, shard_manager, registry);
                pipeline.execute_streaming().await
            });
        }

        // Wait for all queries to complete in parallel
        let results = join_all(query_futures).await;

        // Collect results, checking for errors
        let mut streams = Vec::new();
        for (idx, result) in results.into_iter().enumerate() {
            match result {
                Ok(Some(stream)) => streams.push(stream),
                Ok(None) => {
                    return Err(format!("Query {} did not return a stream", idx));
                }
                Err(e) => {
                    return Err(format!("Query {} failed: {}", idx, e));
                }
            }
        }

        // Check if we got the expected number of streams
        if streams.len() != self.queries.len() {
            return Err(format!(
                "Expected {} streams, got {}",
                self.queries.len(),
                streams.len()
            ));
        }

        // Merge streams using ComparisonStreamMerger
        let merger = ComparisonStreamMerger::new(self.queries);
        let merged_stream = merger.merge_streams(streams).await?;
        Ok(Some(merged_stream))
    }
}
