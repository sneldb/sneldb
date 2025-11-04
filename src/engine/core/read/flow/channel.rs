use std::sync::Arc;

use tokio::sync::mpsc;

use super::batch::ColumnBatch;
use super::metrics::FlowMetrics;

#[derive(Clone)]
pub struct BatchSender {
    inner: mpsc::Sender<Arc<ColumnBatch>>,
    metrics: Arc<FlowMetrics>,
}

impl BatchSender {
    pub async fn send(
        &self,
        batch: Arc<ColumnBatch>,
    ) -> Result<(), mpsc::error::SendError<Arc<ColumnBatch>>> {
        let rows = batch.len() as u64;
        match self.inner.send(batch).await {
            Ok(()) => {
                self.metrics.on_send_success(rows);
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    pub fn try_send(
        &self,
        batch: Arc<ColumnBatch>,
    ) -> Result<(), mpsc::error::TrySendError<Arc<ColumnBatch>>> {
        let rows = batch.len() as u64;
        match self.inner.try_send(batch) {
            Ok(()) => {
                self.metrics.on_send_success(rows);
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full(batch)) => {
                self.metrics.record_backpressure();
                Err(mpsc::error::TrySendError::Full(batch))
            }
            Err(mpsc::error::TrySendError::Closed(batch)) => {
                Err(mpsc::error::TrySendError::Closed(batch))
            }
        }
    }

    pub fn capacity(&self) -> usize {
        self.inner.max_capacity()
    }
}

pub struct BatchReceiver {
    inner: mpsc::Receiver<Arc<ColumnBatch>>,
    metrics: Arc<FlowMetrics>,
}

impl BatchReceiver {
    pub async fn recv(&mut self) -> Option<Arc<ColumnBatch>> {
        if let Some(batch) = self.inner.recv().await {
            let len = batch.len() as u64;
            self.metrics.on_receive(len);
            Some(batch)
        } else {
            None
        }
    }

    pub fn close(&mut self) {
        self.inner.close();
    }
}

pub struct FlowChannel;

impl FlowChannel {
    pub fn bounded(capacity: usize, metrics: Arc<FlowMetrics>) -> (BatchSender, BatchReceiver) {
        let (tx, rx) = mpsc::channel(capacity);

        (
            BatchSender {
                inner: tx,
                metrics: Arc::clone(&metrics),
            },
            BatchReceiver { inner: rx, metrics },
        )
    }
}
