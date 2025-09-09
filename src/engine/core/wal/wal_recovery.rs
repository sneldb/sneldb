use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use crate::engine::core::event::event::Event;
use crate::engine::core::wal::wal_entry::WalEntry;
use crate::engine::shard::context::ShardContext;
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub struct WalRecovery {
    pub base_dir: PathBuf,
    pub shard_id: usize,
}

impl WalRecovery {
    pub fn new(shard_id: usize, base_dir: &PathBuf) -> Self {
        Self {
            base_dir: base_dir.clone(),
            shard_id,
        }
    }

    /// Replays all WAL logs in order and restores events into the shard's memtable.
    pub fn recover(&self, ctx: &mut ShardContext) -> std::io::Result<()> {
        let wal_dir = self.base_dir.clone();

        let wal_files = Self::list_sorted_log_files(&wal_dir)?;
        info!(
            target: "wal_recovery::recover",
            shard_id = self.shard_id,
            file_count = wal_files.len(),
            "WAL files found for recovery"
        );

        for file in wal_files {
            self.replay_log_file(ctx, &file)?;
        }

        info!(
            target: "wal_recovery::recover",
            shard_id = self.shard_id,
            memtable_len = ctx.memtable.len(),
            "WAL recovery completed"
        );

        Ok(())
    }

    fn list_sorted_log_files(dir: &Path) -> std::io::Result<Vec<PathBuf>> {
        let mut files: Vec<PathBuf> = fs::read_dir(dir)?
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().map(|ext| ext == "log").unwrap_or(false))
            .collect();

        files.sort();
        Ok(files)
    }

    fn replay_log_file(&self, ctx: &mut ShardContext, path: &Path) -> std::io::Result<()> {
        info!(
            target: "wal_recovery::replay_log_file",
            shard_id = self.shard_id,
            ?path,
            "Replaying WAL file"
        );

        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let mut line_count = 0;

        for line_result in reader.lines() {
            match line_result {
                Ok(line) => match serde_json::from_str::<WalEntry>(&line) {
                    Ok(entry) => {
                        let event = Event {
                            timestamp: entry.timestamp,
                            context_id: entry.context_id,
                            event_type: entry.event_type,
                            payload: entry.payload,
                        };

                        if let Err(e) = ctx.memtable.insert(event) {
                            return Err(std::io::Error::new(std::io::ErrorKind::Other, e));
                        }

                        line_count += 1;
                    }
                    Err(err) => {
                        warn!(
                            target: "wal_recovery::replay_log_file",
                            shard_id = self.shard_id,
                            ?path,
                            %err,
                            "Skipping invalid WAL entry"
                        );
                    }
                },
                Err(err) => {
                    warn!(
                        target: "wal_recovery::replay_log_file",
                        shard_id = self.shard_id,
                        ?path,
                        %err,
                        "Skipping malformed WAL line"
                    );
                }
            }
        }

        info!(
            target: "wal_recovery::replay_log_file",
            shard_id = self.shard_id,
            ?path,
            events_recovered = line_count,
            "Finished replaying WAL file"
        );

        Ok(())
    }
}
