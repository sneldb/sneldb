use super::kway_merger::KWayMerger;
use super::rlte_coordinator::RlteCoordinator;
use super::segment_discovery::SegmentDiscovery;
use super::shard_command_builder::ShardCommandBuilder;
use crate::command::types::Command;
use crate::engine::core::read::result::QueryResult;
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use crate::engine::shard::message::ShardMessage;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc::channel};
use tracing::{debug, info};

/// Main orchestrator for query execution across shards.
///
/// Coordinates all aspects of query execution:
/// - Parallel segment discovery
/// - RLTE planning (if ORDER BY present)
/// - Command building with zero-copy optimization
/// - Shard dispatch and result collection
/// - Efficient k-way merge for ordered results
pub struct QueryOrchestrator<'a> {
    cmd: &'a Command,
    shard_manager: &'a ShardManager,
    registry: Arc<RwLock<SchemaRegistry>>,
}

impl<'a> QueryOrchestrator<'a> {
    pub fn new(
        cmd: &'a Command,
        shard_manager: &'a ShardManager,
        registry: Arc<RwLock<SchemaRegistry>>,
    ) -> Self {
        Self {
            cmd,
            shard_manager,
            registry,
        }
    }

    /// Executes the query and returns the final result.
    pub async fn execute(&self) -> Result<QueryResult, String> {
        info!(target: "sneldb::query_orchestrator", "Starting query execution");

        // Step 1: Discover segments in parallel (if RLTE planning needed)
        let picked_zones_map = if RlteCoordinator::should_plan(self.cmd) {
            let shard_data = self.discover_segments().await;
            self.plan_with_rlte(&shard_data).await
        } else {
            None
        };

        // Step 2: Dispatch to shards with optimized command building
        let shard_results = self.dispatch_to_shards(picked_zones_map.as_ref()).await;

        // Step 3: Merge results
        self.merge_results(shard_results).await
    }

    /// Discovers segments across all shards in parallel.
    async fn discover_segments(
        &self,
    ) -> std::collections::HashMap<usize, super::segment_discovery::ShardData> {
        let shard_info: Vec<(usize, std::path::PathBuf)> = self
            .shard_manager
            .all_shards()
            .iter()
            .map(|s| (s.id, s.base_dir.clone()))
            .collect();

        SegmentDiscovery::discover_all(shard_info).await
    }

    /// Performs RLTE planning if applicable.
    async fn plan_with_rlte(
        &self,
        shard_data: &std::collections::HashMap<usize, super::segment_discovery::ShardData>,
    ) -> Option<std::collections::HashMap<usize, crate::command::types::PickedZones>> {
        let base_dirs = SegmentDiscovery::extract_base_dirs(shard_data);
        let segments = SegmentDiscovery::extract_segment_map(shard_data);

        match RlteCoordinator::plan(self.cmd, Arc::clone(&self.registry), &base_dirs, &segments)
            .await
        {
            Some(output) => Some(output.per_shard),
            None => None,
        }
    }

    /// Dispatches the query to all shards and collects results.
    async fn dispatch_to_shards(
        &self,
        picked_zones_map: Option<
            &std::collections::HashMap<usize, crate::command::types::PickedZones>,
        >,
    ) -> Vec<QueryResult> {
        let (tx, mut rx) = channel(4096);

        // Build commands for each shard (zero-copy when possible)
        let builder = ShardCommandBuilder::new(self.cmd, picked_zones_map);

        for shard in self.shard_manager.all_shards() {
            let tx_clone = tx.clone();
            info!(target: "sneldb::query_orchestrator", shard_id = shard.id, "Dispatching to shard");

            let cmd_for_shard = builder.build_for_shard(shard.id);

            let _ = shard
                .tx
                .send(ShardMessage::Query(
                    cmd_for_shard.into_owned(),
                    tx_clone,
                    Arc::clone(&self.registry),
                ))
                .await;
        }

        drop(tx);

        // Collect all results
        let mut results = Vec::new();
        while let Some(result) = rx.recv().await {
            results.push(result);
        }

        debug!(
            target: "sneldb::query_orchestrator",
            result_count = results.len(),
            "Collected results from all shards"
        );

        results
    }

    /// Merges shard results, handling both ORDER BY and non-ORDER BY cases.
    async fn merge_results(&self, shard_results: Vec<QueryResult>) -> Result<QueryResult, String> {
        // Extract ORDER BY info if present
        let order_info = self.extract_order_info();

        if let Some((field, ascending, limit, offset)) = order_info {
            self.merge_with_order(shard_results, &field, ascending, limit, offset)
        } else {
            self.merge_without_order(shard_results)
        }
    }

    /// Extracts ordering information from the command.
    fn extract_order_info(&self) -> Option<(String, bool, Option<u32>, Option<u32>)> {
        if let Command::Query {
            order_by: Some(ob),
            limit,
            offset,
            ..
        } = self.cmd
        {
            Some((ob.field.clone(), !ob.desc, *limit, *offset))
        } else {
            None
        }
    }

    /// Merges results with ORDER BY using k-way merge.
    fn merge_with_order(
        &self,
        shard_results: Vec<QueryResult>,
        field: &str,
        ascending: bool,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> Result<QueryResult, String> {
        // Extract rows from each shard
        let mut per_shard_rows: Vec<Vec<Vec<Value>>> = Vec::new();
        let mut columns_opt: Option<Vec<(String, String)>> = None;

        for result in shard_results {
            if let QueryResult::Selection(sel) = result {
                let table = sel.finalize();
                if columns_opt.is_none() {
                    columns_opt = Some(
                        table
                            .columns
                            .iter()
                            .map(|c| (c.name.clone(), c.logical_type.clone()))
                            .collect(),
                    );
                }
                per_shard_rows.push(table.rows);
            }
        }

        if per_shard_rows.is_empty() {
            return Ok(QueryResult::Selection(
                crate::engine::core::read::result::SelectionResult {
                    columns: vec![],
                    rows: vec![],
                },
            ));
        }

        // Perform k-way merge
        let k_total = limit.unwrap_or(0).saturating_add(offset.unwrap_or(0)) as usize;
        let merge_limit = if k_total == 0 { usize::MAX } else { k_total };

        let merger = KWayMerger::new(&per_shard_rows, field, ascending, merge_limit);
        let merged = merger.merge();

        // Apply pagination
        let final_rows = KWayMerger::apply_pagination(merged, offset, limit);

        // Build result
        let columns = columns_opt.unwrap_or_else(|| {
            vec![
                ("context_id".to_string(), "String".to_string()),
                ("event_type".to_string(), "String".to_string()),
                ("timestamp".to_string(), "Timestamp".to_string()),
                ("payload".to_string(), "Object".to_string()),
            ]
        });

        let selection = crate::engine::core::read::result::SelectionResult {
            columns: columns
                .iter()
                .map(|(name, ty)| crate::engine::core::read::result::ColumnSpec {
                    name: name.clone(),
                    logical_type: ty.clone(),
                })
                .collect(),
            rows: final_rows,
        };

        Ok(QueryResult::Selection(selection))
    }

    /// Merges results without ORDER BY (legacy path).
    fn merge_without_order(&self, shard_results: Vec<QueryResult>) -> Result<QueryResult, String> {
        let mut acc_opt: Option<QueryResult> = None;

        for result in shard_results {
            if let Some(cur) = &mut acc_opt {
                cur.merge(result);
            } else {
                acc_opt = Some(result);
            }
        }

        acc_opt.ok_or_else(|| "No results from shards".to_string())
    }
}
