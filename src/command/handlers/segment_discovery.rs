use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::task;
use tracing::debug;

/// Data about a shard including its base directory and discovered segments.
#[derive(Debug, Clone)]
pub struct ShardData {
    pub shard_id: usize,
    pub base_dir: PathBuf,
    pub segments: Vec<String>,
}

/// Handles parallel discovery of segments across shards.
pub struct SegmentDiscovery;

impl SegmentDiscovery {
    /// Discovers all segments across multiple shards in parallel.
    ///
    /// Uses spawn_blocking to avoid blocking the async runtime during filesystem operations.
    /// Returns a HashMap mapping shard_id to ShardData.
    pub async fn discover_all(shard_info: Vec<(usize, PathBuf)>) -> HashMap<usize, ShardData> {
        debug!(
            target: "sneldb::segment_discovery",
            shard_count = shard_info.len(),
            "Starting parallel segment discovery"
        );

        // Spawn all discovery tasks in parallel
        let handles: Vec<_> = shard_info
            .into_iter()
            .map(|(shard_id, base_dir)| {
                task::spawn_blocking(move || {
                    let segments = Self::discover_segments_sync(&base_dir);
                    ShardData {
                        shard_id,
                        base_dir,
                        segments,
                    }
                })
            })
            .collect();

        // Await all tasks and collect results
        let mut result = HashMap::new();
        for handle in handles {
            match handle.await {
                Ok(shard_data) => {
                    debug!(
                        target: "sneldb::segment_discovery",
                        shard_id = shard_data.shard_id,
                        segment_count = shard_data.segments.len(),
                        "Discovered segments for shard"
                    );
                    result.insert(shard_data.shard_id, shard_data);
                }
                Err(e) => {
                    tracing::warn!(
                        target: "sneldb::segment_discovery",
                        error = ?e,
                        "Failed to discover segments for a shard"
                    );
                }
            }
        }

        debug!(
            target: "sneldb::segment_discovery",
            total_shards = result.len(),
            "Completed parallel segment discovery"
        );

        result
    }

    /// Synchronously discovers segments in a directory.
    ///
    /// This is called within spawn_blocking to avoid blocking the async runtime.
    fn discover_segments_sync(base_dir: &Path) -> Vec<String> {
        let mut segments = Vec::new();

        match std::fs::read_dir(base_dir) {
            Ok(entries) => {
                for entry in entries.flatten() {
                    if let Some(name) = entry.file_name().to_str() {
                        if name.chars().all(|c| c.is_ascii_digit()) {
                            segments.push(name.to_string());
                        }
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    target: "sneldb::segment_discovery",
                    base_dir = ?base_dir,
                    error = ?e,
                    "Failed to read shard directory"
                );
            }
        }

        // Sort for consistent ordering
        segments.sort();
        segments
    }

    /// Extracts segment lists from ShardData for RLTE planning.
    pub fn extract_segment_map(
        shard_data: &HashMap<usize, ShardData>,
    ) -> HashMap<usize, Vec<String>> {
        shard_data
            .iter()
            .map(|(id, data)| (*id, data.segments.clone()))
            .collect()
    }

    /// Extracts base directories from ShardData for RLTE planning.
    pub fn extract_base_dirs(shard_data: &HashMap<usize, ShardData>) -> HashMap<usize, PathBuf> {
        shard_data
            .iter()
            .map(|(id, data)| (*id, data.base_dir.clone()))
            .collect()
    }
}
