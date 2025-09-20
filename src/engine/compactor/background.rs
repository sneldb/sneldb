use crate::engine::core::{CompactionWorker, IoMonitor, SegmentIndex};
use crate::engine::schema::SchemaRegistry;
use crate::shared::config::CONFIG;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use sysinfo::{Disks, System};
use tokio::time::sleep;
use tracing::{error, info, warn};

pub async fn start_background_compactor(shard_id: u32, shard_dir: PathBuf) {
    tokio::spawn(async move {
        let mut sys = System::new_all();
        let disks = Disks::new_with_refreshed_list();
        let mut monitor = IoMonitor::new(&disks);

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
                    if segment_index.needs_compaction() {
                        warn!(shard_id, "Background compaction triggered");
                        let registry = Arc::new(tokio::sync::RwLock::new(
                            SchemaRegistry::new().expect("Failed to initialize SchemaRegistry"),
                        ));

                        warn!(
                            "Compaction worker initialized for shard {} and shard_dir {}",
                            shard_id,
                            shard_dir.display()
                        );
                        let worker = CompactionWorker::new(
                            shard_id,
                            shard_dir.clone(),
                            Arc::clone(&registry),
                        );

                        if let Err(e) = worker.run().await {
                            error!(shard_id, "Background compaction failed: {}", e);
                        }
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
