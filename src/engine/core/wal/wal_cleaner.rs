use crate::engine::core::wal::wal_archiver::WalArchiver;
use std::path::PathBuf;
use tracing::{error, info, warn};

/// Responsible for cleaning up obsolete WAL log files after successful segment flushes.
/// In conservative mode, archives WAL files before deletion.
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

    /// Create a new cleaner for a given shard with a custom WAL directory
    pub fn with_wal_dir(shard_id: usize, wal_dir: PathBuf) -> Self {
        Self { shard_id, wal_dir }
    }

    /// Deletes all WAL logs with ID < `keep_from_log_id`.
    /// In conservative mode, archives WAL files before deletion.
    /// This should be called after segment flushes.
    pub fn cleanup_up_to(&self, keep_from_log_id: u64) {
        // Check if conservative mode is enabled
        let conservative_mode = crate::shared::config::CONFIG.wal.conservative_mode;

        if conservative_mode {
            info!(
                target: "wal_cleaner::cleanup_up_to",
                shard_id = self.shard_id,
                keep_from = keep_from_log_id,
                "Conservative mode enabled, archiving WAL files before deletion"
            );

            // Archive WAL files before deletion
            let archiver = WalArchiver::new(self.shard_id);
            let archive_results = archiver.archive_logs_up_to(keep_from_log_id);

            // Count successes and failures
            let success_count = archive_results.iter().filter(|r| r.is_ok()).count();
            let failure_count = archive_results.iter().filter(|r| r.is_err()).count();

            if failure_count > 0 {
                error!(
                    target: "wal_cleaner::cleanup_up_to",
                    shard_id = self.shard_id,
                    success_count,
                    failure_count,
                    "Some WAL files failed to archive, skipping cleanup to preserve data"
                );
                return;
            }

            info!(
                target: "wal_cleaner::cleanup_up_to",
                shard_id = self.shard_id,
                archived_count = success_count,
                "All WAL files archived successfully, proceeding with cleanup"
            );
        }

        // Proceed with deletion (either after archiving or directly)
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
                                            conservative_mode,
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
