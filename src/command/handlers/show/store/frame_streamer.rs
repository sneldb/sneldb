use std::path::PathBuf;
use std::sync::Arc;

use tokio::task::JoinHandle;

use crate::command::handlers::show::errors::{ShowError, ShowResult};
use crate::engine::core::read::flow::{BatchSender, ColumnBatch};
use crate::engine::materialize::{MaterializedStore, StoredFrameMeta};

pub struct StoredFrameStreamer {
    store_path: PathBuf,
    frames: Vec<StoredFrameMeta>,
}

impl StoredFrameStreamer {
    pub fn new(store_path: PathBuf) -> ShowResult<Self> {
        let store = MaterializedStore::open(&store_path)
            .map_err(|err| ShowError::new(format!("Failed to open materialized store: {err}")))?;
        let frames = store.frames().to_vec();
        Ok(Self { store_path, frames })
    }

    pub fn frame_count(&self) -> usize {
        self.frames.len()
    }

    pub fn spawn_stream_task(&self, sender: BatchSender) -> Option<JoinHandle<()>> {
        if self.frames.is_empty() {
            return None;
        }

        let frames = self.frames.clone();
        let store_path = self.store_path.clone();

        Some(tokio::spawn(async move {
            let store = match MaterializedStore::open(&store_path) {
                Ok(store) => Arc::new(store),
                Err(err) => {
                    tracing::error!(
                        target: "sneldb::show",
                        error = %err,
                        "Failed to open store for streaming"
                    );
                    return;
                }
            };

            let read_tasks: Vec<_> = frames
                .into_iter()
                .map(|meta| {
                    let store = Arc::clone(&store);
                    tokio::task::spawn_blocking(move || -> Result<Arc<ColumnBatch>, String> {
                        store.read_frame(&meta).map_err(|err| format!("{err:?}"))
                    })
                })
                .collect();

            for task in read_tasks {
                match task.await {
                    Ok(Ok(batch)) => {
                        if sender.send(batch).await.is_err() {
                            break;
                        }
                    }
                    Ok(Err(err)) => {
                        tracing::error!(
                            target: "sneldb::show",
                            error = %err,
                            "Failed to read stored frame"
                        );
                        break;
                    }
                    Err(err) => {
                        tracing::error!(
                            target: "sneldb::show",
                            error = %err,
                            "Task join error"
                        );
                        break;
                    }
                }
            }
        }))
    }
}
