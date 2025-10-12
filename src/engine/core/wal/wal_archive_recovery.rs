use crate::engine::core::WalEntry;
use crate::engine::core::wal::wal_archive::WalArchive;
use std::path::{Path, PathBuf};
use tracing::{error, info, warn};

/// Handles recovery of WAL entries from archived files
pub struct WalArchiveRecovery {
    pub archive_dir: PathBuf,
    pub shard_id: usize,
}

impl WalArchiveRecovery {
    /// Create a new archive recovery helper
    pub fn new(shard_id: usize, archive_dir: PathBuf) -> Self {
        Self {
            archive_dir,
            shard_id,
        }
    }

    /// List all archived WAL files in chronological order
    pub fn list_archives(&self) -> std::io::Result<Vec<PathBuf>> {
        if !self.archive_dir.exists() {
            info!(
                target: "wal_archive_recovery::list_archives",
                shard_id = self.shard_id,
                "Archive directory does not exist"
            );
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

        info!(
            target: "wal_archive_recovery::list_archives",
            shard_id = self.shard_id,
            count = archives.len(),
            "Found archived WAL files"
        );

        Ok(archives)
    }

    /// Recover entries from a single archive file
    pub fn recover_from_archive(&self, archive_path: &Path) -> std::io::Result<Vec<WalEntry>> {
        info!(
            target: "wal_archive_recovery::recover_from_archive",
            shard_id = self.shard_id,
            ?archive_path,
            "Recovering entries from archive"
        );

        let archive = WalArchive::read_from_file(archive_path)?;

        info!(
            target: "wal_archive_recovery::recover_from_archive",
            shard_id = self.shard_id,
            ?archive_path,
            entry_count = archive.header.entry_count,
            log_id = archive.header.log_id,
            "Successfully recovered entries from archive"
        );

        Ok(archive.body.entries)
    }

    /// Recover all entries from all archives in chronological order
    pub fn recover_all(&self) -> std::io::Result<Vec<WalEntry>> {
        let archives = self.list_archives()?;
        let mut all_entries = Vec::new();

        for archive_path in archives {
            match self.recover_from_archive(&archive_path) {
                Ok(entries) => {
                    all_entries.extend(entries);
                }
                Err(e) => {
                    error!(
                        target: "wal_archive_recovery::recover_all",
                        shard_id = self.shard_id,
                        ?archive_path,
                        error = %e,
                        "Failed to recover from archive, skipping"
                    );
                }
            }
        }

        info!(
            target: "wal_archive_recovery::recover_all",
            shard_id = self.shard_id,
            total_entries = all_entries.len(),
            "Recovered all entries from archives"
        );

        Ok(all_entries)
    }

    /// Get metadata from an archive without loading all entries
    pub fn get_archive_info(&self, archive_path: &Path) -> std::io::Result<ArchiveInfo> {
        let archive = WalArchive::read_from_file(archive_path)?;

        Ok(ArchiveInfo {
            path: archive_path.to_path_buf(),
            shard_id: archive.header.shard_id,
            log_id: archive.header.log_id,
            entry_count: archive.header.entry_count,
            start_timestamp: archive.header.start_timestamp,
            end_timestamp: archive.header.end_timestamp,
            created_at: archive.header.created_at,
            compression: archive.header.compression.clone(),
            compression_level: archive.header.compression_level,
            version: archive.header.version,
        })
    }

    /// List metadata for all archives
    pub fn list_archive_info(&self) -> Vec<ArchiveInfo> {
        let archives = match self.list_archives() {
            Ok(a) => a,
            Err(e) => {
                error!(
                    target: "wal_archive_recovery::list_archive_info",
                    shard_id = self.shard_id,
                    error = %e,
                    "Failed to list archives"
                );
                return Vec::new();
            }
        };

        archives
            .iter()
            .filter_map(|path| match self.get_archive_info(path) {
                Ok(info) => Some(info),
                Err(e) => {
                    warn!(
                        target: "wal_archive_recovery::list_archive_info",
                        ?path,
                        error = %e,
                        "Failed to read archive info"
                    );
                    None
                }
            })
            .collect()
    }

    /// Export archive to JSON format for inspection
    pub fn export_to_json(&self, archive_path: &Path, output_path: &Path) -> std::io::Result<()> {
        info!(
            target: "wal_archive_recovery::export_to_json",
            shard_id = self.shard_id,
            ?archive_path,
            ?output_path,
            "Exporting archive to JSON"
        );

        let archive = WalArchive::read_from_file(archive_path)?;
        let json = serde_json::to_string_pretty(&archive.body.entries)?;

        std::fs::write(output_path, json)?;

        info!(
            target: "wal_archive_recovery::export_to_json",
            shard_id = self.shard_id,
            entry_count = archive.header.entry_count,
            "Archive exported to JSON successfully"
        );

        Ok(())
    }
}

/// Metadata about an archived WAL file
#[derive(Debug, Clone)]
pub struct ArchiveInfo {
    pub path: PathBuf,
    pub shard_id: usize,
    pub log_id: u64,
    pub entry_count: u64,
    pub start_timestamp: u64,
    pub end_timestamp: u64,
    pub created_at: u64,
    pub compression: String,
    pub compression_level: i32,
    pub version: u8,
}

impl ArchiveInfo {
    /// Format archive info as a human-readable string
    pub fn format(&self) -> String {
        let start_time = crate::shared::time::format_timestamp(self.start_timestamp);
        let end_time = crate::shared::time::format_timestamp(self.end_timestamp);
        let created = crate::shared::time::format_timestamp(self.created_at);

        format!(
            "Archive: {}\n  Shard: {}, Log ID: {:05}\n  Entries: {}\n  Time range: {} to {}\n  Created: {}\n  Compression: {} (level {})\n  Version: {}",
            self.path.display(),
            self.shard_id,
            self.log_id,
            self.entry_count,
            start_time,
            end_time,
            created,
            self.compression,
            self.compression_level,
            self.version
        )
    }
}
