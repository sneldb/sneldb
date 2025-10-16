use std::path::{Path, PathBuf};
use tokio::sync::mpsc::{self, Sender};

use crate::engine::core::{InnerWalWriter, WalEntry};
use crate::shared::config::CONFIG;
use tracing::{debug, error, info};

pub enum WalMessage {
    Entry(WalEntry),
    Shutdown,
}

#[derive(Debug, Clone)]
pub struct WalHandle {
    sender: Option<Sender<WalMessage>>,
    base_dir: PathBuf,
    shard_id: usize,
}

impl WalHandle {
    pub fn new(shard_id: usize, base_dir: &Path) -> std::io::Result<WalHandle> {
        let wal_dir = base_dir.to_path_buf();
        std::fs::create_dir_all(&wal_dir)?;
        info!(
            target: "wal_handle::new",
            shard_id, wal_dir = ?wal_dir,
            "WAL directory created"
        );

        Ok(WalHandle {
            sender: None,
            base_dir: wal_dir,
            shard_id,
        })
    }

    pub async fn append(&self, entry: WalEntry) {
        if let Some(sender) = &self.sender {
            debug!(
                target: "wal_handle::append",
                shard_id = self.shard_id, entry = ?entry,
                "Appending entry to WAL"
            );
            let _ = sender.send(WalMessage::Entry(entry)).await;
        } else {
            error!(
                target: "wal_handle::append",
                shard_id = self.shard_id,
                "Attempted to append to WAL with no active writer"
            );
        }
    }

    pub async fn shutdown(&self) {
        if let Some(sender) = &self.sender {
            info!(
                target: "wal_handle::shutdown",
                shard_id = self.shard_id,
                "Sending WAL shutdown message"
            );
            let _ = sender.send(WalMessage::Shutdown).await;
        }
    }

    pub fn spawn_wal_thread(&self) -> std::io::Result<WalHandle> {
        let (tx, mut rx) = mpsc::channel(4096);
        let shard_id = self.shard_id;
        let base_dir = self.base_dir.clone();

        info!(
            target: "wal_handle::spawn_wal_thread",
            shard_id, path = %base_dir.display(),
            "Starting WAL thread"
        );

        tokio::spawn(async move {
            debug!(target: "wal_handle::spawn_wal_thread", shard_id, "Initializing WAL writer");
            let mut writer = match InnerWalWriter::new(base_dir.clone()) {
                Ok(w) => w,
                Err(e) => {
                    error!(
                        target: "wal_handle::spawn_wal_thread",
                        shard_id, err = ?e,
                        "Failed to initialize WAL writer"
                    );
                    return;
                }
            };

            if let Err(e) = writer.start_next_log_file() {
                error!(
                    target: "wal_handle::spawn_wal_thread",
                    shard_id, err = ?e,
                    "Failed to start WAL log file"
                );
                return;
            }

            while let Some(msg) = rx.recv().await {
                match msg {
                    WalMessage::Entry(entry) => {
                        if let Err(err) = writer.append_immediate(&entry) {
                            error!(
                                target: "wal_handle::spawn_wal_thread",
                                shard_id, err = ?err,
                                "WAL append failed"
                            );
                        }

                        debug!(
                            target: "wal_handle::spawn_wal_thread",
                            shard_id, entries_written = writer.entries_written,
                            "Entry appended to WAL"
                        );

                        let capacity = CONFIG.engine.fill_factor * CONFIG.engine.event_per_zone;
                        if writer.entries_written >= capacity as u64 {
                            if let Err(err) = writer.rotate_log_file() {
                                error!(
                                    target: "wal_handle::spawn_wal_thread",
                                    shard_id, err = ?err,
                                    "WAL rotation failed"
                                );
                            } else {
                                info!(
                                    target: "wal_handle::spawn_wal_thread",
                                    shard_id, new_log_id = writer.current_log_id,
                                    "WAL log rotated"
                                );
                            }
                        }
                    }
                    WalMessage::Shutdown => {
                        info!(
                            target: "wal_handle::spawn_wal_thread",
                            shard_id,
                            "WAL shutdown received, flushing and closing"
                        );
                        let _ = writer.flush_and_close();
                        break;
                    }
                }
            }

            info!(target: "wal_handle::spawn_wal_thread", shard_id, "WAL thread exited");
        });

        Ok(WalHandle {
            sender: Some(tx),
            base_dir: self.base_dir.clone(),
            shard_id: self.shard_id,
        })
    }
}
