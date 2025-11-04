use crate::engine::core::read::flow::{BatchReceiver, BatchSchema, ColumnBatch};
use std::sync::Arc;
use tokio::task::JoinHandle;

/// A streaming result type that holds schema, receiver, and background tasks.
///
/// This stream receives batches from multiple shards and manages the lifecycle
/// of background tasks that populate the stream. When dropped, all background
/// tasks are aborted to prevent resource leaks.
pub struct QueryBatchStream {
    schema: Arc<BatchSchema>,
    receiver: BatchReceiver,
    tasks: Vec<JoinHandle<()>>,
}

impl QueryBatchStream {
    /// Creates a new `QueryBatchStream`.
    ///
    /// # Arguments
    /// * `schema` - The schema of the batches that will be received
    /// * `receiver` - The channel receiver for batches
    /// * `tasks` - Background tasks that populate the stream (will be aborted on drop)
    pub(crate) fn new(
        schema: Arc<BatchSchema>,
        receiver: BatchReceiver,
        tasks: Vec<JoinHandle<()>>,
    ) -> Self {
        Self {
            schema,
            receiver,
            tasks,
        }
    }

    /// Returns the schema of the batches in this stream.
    pub fn schema(&self) -> Arc<BatchSchema> {
        Arc::clone(&self.schema)
    }

    /// Receives the next batch from the stream.
    ///
    /// Returns `None` when the stream is exhausted.
    pub async fn recv(&mut self) -> Option<Arc<ColumnBatch>> {
        self.receiver.recv().await
    }
}

impl Drop for QueryBatchStream {
    fn drop(&mut self) {
        while let Some(task) = self.tasks.pop() {
            task.abort();
        }
    }
}
