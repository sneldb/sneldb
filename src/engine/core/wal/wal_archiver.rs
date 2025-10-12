use crate::engine::core::wal::wal_archive::WalArchive;
use crate::shared::config::CONFIG;
use std::path::{Path, PathBuf};
use tracing::{error, info, warn};

/// Responsible for archiving WAL log files before deletion
pub struct WalArchiver {
    shard_id: usize,
    wal_dir: PathBuf,
    archive_dir: PathBuf,
    compression_level: i32,
    compression_algorithm: String,
}

impl WalArchiver {
    /// Create a new archiver for a given shard
    pub fn new(shard_id: usize) -> Self {
        let wal_dir = PathBuf::from(CONFIG.wal.dir.clone()).join(format!("shard-{}", shard_id));
        let archive_dir =
            PathBuf::from(CONFIG.wal.archive_dir.clone()).join(format!("shard-{}", shard_id));

        Self {
            shard_id,
            wal_dir,
            archive_dir,
            compression_level: CONFIG.wal.compression_level,
            compression_algorithm: CONFIG.wal.compression_algorithm.clone(),
        }
    }

    /// Create a new archiver with custom directories (useful for testing)
    pub fn with_dirs(
        shard_id: usize,
        wal_dir: PathBuf,
        archive_dir: PathBuf,
        compression_level: i32,
    ) -> Self {
        Self {
            shard_id,
            wal_dir,
            archive_dir,
            compression_level,
            compression_algorithm: "zstd".to_string(),
        }
    }

    /// Archive a single WAL log file
    pub fn archive_log(&self, log_id: u64) -> std::io::Result<PathBuf> {
        let wal_path = self.wal_dir.join(format!("wal-{:05}.log", log_id));

        if !wal_path.exists() {
            warn!(
                target: "wal_archiver::archive_log",
                shard_id = self.shard_id,
                log_id,
                ?wal_path,
                "WAL file does not exist, skipping archive"
            );
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "WAL file not found",
            ));
        }

        info!(
            target: "wal_archiver::archive_log",
            shard_id = self.shard_id,
            log_id,
            ?wal_path,
            "Archiving WAL file"
        );

        // Create archive from WAL file
        let archive = WalArchive::from_wal_file(
            &wal_path,
            self.shard_id,
            log_id,
            self.compression_algorithm.clone(),
            self.compression_level,
        )?;

        // Write archive to file
        let archive_path = archive.write_to_file(&self.archive_dir)?;

        info!(
            target: "wal_archiver::archive_log",
            shard_id = self.shard_id,
            log_id,
            ?archive_path,
            entry_count = archive.header.entry_count,
            "WAL file archived successfully"
        );

        Ok(archive_path)
    }

    /// Archive multiple WAL log files with IDs < keep_from_log_id
    pub fn archive_logs_up_to(
        &self,
        keep_from_log_id: u64,
    ) -> Vec<Result<PathBuf, std::io::Error>> {
        info!(
            target: "wal_archiver::archive_logs_up_to",
            shard_id = self.shard_id,
            keep_from = keep_from_log_id,
            "Starting batch archive"
        );

        let mut results = Vec::new();

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
                                results.push(self.archive_log(id));
                            }
                        }
                    }
                }
            }
            Err(e) => {
                error!(
                    target: "wal_archiver::archive_logs_up_to",
                    shard_id = self.shard_id,
                    error = %e,
                    "Failed to read WAL directory"
                );
            }
        }

        let success_count = results.iter().filter(|r| r.is_ok()).count();
        let error_count = results.iter().filter(|r| r.is_err()).count();

        info!(
            target: "wal_archiver::archive_logs_up_to",
            shard_id = self.shard_id,
            success_count,
            error_count,
            "Batch archive complete"
        );

        results
    }

    /// List all archived WAL files for this shard
    pub fn list_archives(&self) -> std::io::Result<Vec<PathBuf>> {
        if !self.archive_dir.exists() {
            return Ok(Vec::new());
        }

        let mut archives: Vec<PathBuf> = std::fs::read_dir(&self.archive_dir)?
            .flatten()
            .map(|e| e.path())
            .filter(|p| {
                p.extension()
                    .and_then(|ext| ext.to_str())
                    .map(|ext| ext == "zst")
                    .unwrap_or(false)
            })
            .collect();

        archives.sort();
        Ok(archives)
    }

    /// Get the archive directory path
    pub fn archive_dir(&self) -> &Path {
        &self.archive_dir
    }
}
