use std::path::PathBuf;
use tracing::{info, warn};

/// Responsible for cleaning up obsolete WAL log files after successful segment flushes.
pub struct WalCleaner {
    shard_id: usize,
    wal_dir: PathBuf,
}

impl WalCleaner {
    /// Create a new cleaner for a given shard
    pub fn new(shard_id: usize) -> Self {
        let wal_dir = PathBuf::from(crate::shared::config::CONFIG.wal.dir.clone())
            .join(format!("shard-{}", shard_id));
        Self { shard_id, wal_dir }
    }

    /// Deletes all WAL logs with ID < `keep_from_log_id`.
    /// This should be called after segment flushes.
    pub fn cleanup_up_to(&self, keep_from_log_id: u64) {
        match std::fs::read_dir(&self.wal_dir) {
            Ok(entries) => {
                for entry in entries.flatten() {
                    let file_name = entry.file_name().to_string_lossy().to_string();

                    if let Some(num) = file_name
                        .strip_prefix("wal-")
                        .and_then(|s| s.strip_suffix(".log"))
                    {
                        if let Ok(id) = num.parse::<u64>() {
                            if id < keep_from_log_id {
                                let path = entry.path();
                                match std::fs::remove_file(&path) {
                                    Ok(_) => {
                                        info!(
                                            target: "wal_cleaner::cleanup_up_to",
                                            shard_id = self.shard_id,
                                            ?path,
                                            deleted_id = id,
                                            keep_from = keep_from_log_id,
                                            "Deleted obsolete WAL file"
                                        );
                                    }
                                    Err(e) => {
                                        warn!(
                                            target: "wal_cleaner::cleanup_up_to",
                                            shard_id = self.shard_id,
                                            ?path,
                                            deleted_id = id,
                                            keep_from = keep_from_log_id,
                                            error = %e,
                                            "Failed to delete WAL file"
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                warn!(
                    target: "wal_cleaner::cleanup_up_to",
                    shard_id = self.shard_id,
                    wal_dir = %self.wal_dir.display(),
                    error = %e,
                    "Failed to read WAL directory"
                );
            }
        }
    }
}
