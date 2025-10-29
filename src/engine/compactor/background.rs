use crate::engine::core::compaction::handover::CompactionHandover;
use crate::engine::core::{CompactionWorker, IoMonitor, SegmentIndex};
use crate::engine::schema::SchemaRegistry;
use crate::shared::config::CONFIG;
use once_cell::sync::Lazy;
use std::path::PathBuf;
use std::sync::{Arc, RwLock as StdRwLock};
use std::time::Duration;
use sysinfo::{Disks, System};
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tracing::{error, warn};

// Global semaphore to limit compaction across shards concurrently
static GLOBAL_COMPACTION_SEMAPHORE: Lazy<Semaphore> =
    Lazy::new(|| Semaphore::new(CONFIG.engine.compaction_max_shard_concurrency));

pub async fn start_background_compactor(
    shard_id: u32,
    shard_dir: PathBuf,
    segment_ids: Arc<StdRwLock<Vec<String>>>,
    flush_lock: Arc<tokio::sync::Mutex<()>>,
) {
    tokio::spawn(async move {
        let mut sys = System::new_all();
        let disks = Disks::new_with_refreshed_list();
        let mut monitor = IoMonitor::new(&disks);
        let handover = Arc::new(CompactionHandover::new(
            shard_id,
            shard_dir.clone(),
            Arc::clone(&segment_ids),
            Arc::clone(&flush_lock),
        ));

        loop {
            sleep(Duration::from_secs(CONFIG.engine.compaction_interval)).await;

            sys.refresh_all();
            let disks = Disks::new_with_refreshed_list();
            if monitor.is_under_pressure(&disks) {
                warn!(shard_id, "IO pressure detected — skipping compaction");
                continue;
            }

            match SegmentIndex::load(&shard_dir).await {
                Ok(segment_index) => {
                    warn!(shard_id, "Segment index loaded");
                    // Policy-based trigger: run only if there are plans
                    use crate::engine::core::compaction::policy::{
                        CompactionPolicy, KWayCountPolicy,
                    };
                    let policy = KWayCountPolicy::default();
                    let plans = CompactionPolicy::plan(&policy, &segment_index);
                    if !plans.is_empty() {
                        warn!(shard_id, "Background compaction triggered");
                        let registry = Arc::new(tokio::sync::RwLock::new(
                            SchemaRegistry::new().expect("Failed to initialize SchemaRegistry"),
                        ));

                        warn!(
                            "Compaction worker initialized for shard {} and shard_dir {}",
                            shard_id,
                            shard_dir.display()
                        );

                        // Acquire global permit to ensure one shard at a time
                        let permit = GLOBAL_COMPACTION_SEMAPHORE.acquire().await.unwrap();
                        let worker = CompactionWorker::new(
                            shard_id,
                            shard_dir.clone(),
                            Arc::clone(&registry),
                            Arc::clone(&handover),
                        );

                        if let Err(e) = worker.run().await {
                            error!(shard_id, "Background compaction failed: {}", e);
                        }
                        drop(permit);
                    } else {
                        warn!(shard_id, "No compaction needed");
                    }
                }
                Err(e) => {
                    error!(shard_id, "Failed to load SegmentIndex: {}", e);
                }
            }
        }
    });
}
