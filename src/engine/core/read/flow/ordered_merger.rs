use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

use serde_json::Value;
use tokio::task::JoinHandle;
use tracing::error;

use super::{BatchPool, BatchReceiver, BatchSchema, BatchSender, ColumnBatch};

/// Coordinates ordered merging of shard batch streams into a single ordered stream.
pub struct OrderedStreamMerger;

impl OrderedStreamMerger {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        schema: Arc<BatchSchema>,
        receivers: Vec<BatchReceiver>,
        order_index: usize,
        ascending: bool,
        offset: usize,
        limit: Option<usize>,
        sender: BatchSender,
        batch_size: usize,
    ) -> Result<JoinHandle<()>, String> {
        if receivers.is_empty() {
            return Err("no receivers for ordered merge".to_string());
        }

        let pool = BatchPool::new(batch_size).map_err(|e| e.to_string())?;

        let streams: Vec<RowStream> = receivers.into_iter().map(RowStream::new).collect();

        let merger = MergerState {
            schema,
            streams,
            order_index,
            ascending,
            offset,
            limit,
            sender,
            pool,
        };

        Ok(tokio::spawn(async move {
            if let Err(err) = merger.run().await {
                error!(
                    target = "sneldb::ordered_merger",
                    error = %err,
                    "Ordered stream merger failed"
                );
            }
        }))
    }
}

struct MergerState {
    schema: Arc<BatchSchema>,
    streams: Vec<RowStream>,
    order_index: usize,
    ascending: bool,
    offset: usize,
    limit: Option<usize>,
    sender: BatchSender,
    pool: BatchPool,
}

impl MergerState {
    async fn run(mut self) -> Result<(), String> {
        let mut heap = BinaryHeap::new();

        for (idx, stream) in self.streams.iter_mut().enumerate() {
            if let Some(row) = stream
                .next_row()
                .await
                .map_err(|e| format!("stream {} failed: {}", idx, e))?
            {
                heap.push(HeapItem {
                    shard_idx: idx,
                    row,
                    order_index: self.order_index,
                    ascending: self.ascending,
                });
            }
        }

        let mut skipped = self.offset;
        let limit = self.limit.unwrap_or(usize::MAX);
        let mut emitted = 0usize;

        let mut builder = self.pool.acquire(Arc::clone(&self.schema));

        while let Some(mut item) = heap.pop() {
            if emitted >= limit {
                break;
            }

            if skipped > 0 {
                skipped -= 1;
            } else {
                builder.push_row(&item.row).map_err(|e| e.to_string())?;
                emitted += 1;

                if builder.is_full() {
                    let batch = builder.finish().map_err(|e| e.to_string())?;
                    if self.sender.send(Arc::new(batch)).await.is_err() {
                        return Ok(());
                    }
                    builder = self.pool.acquire(Arc::clone(&self.schema));
                }
            }

            if emitted >= limit {
                break;
            }

            if let Some(row) = self.streams[item.shard_idx]
                .next_row()
                .await
                .map_err(|e| format!("stream {} failed: {}", item.shard_idx, e))?
            {
                item.row = row;
                heap.push(item);
            }
        }

        if builder.len() > 0 {
            let batch = builder.finish().map_err(|e| e.to_string())?;
            let _ = self.sender.send(Arc::new(batch)).await;
        }

        Ok(())
    }
}

struct RowStream {
    receiver: BatchReceiver,
    current_batch: Option<Arc<ColumnBatch>>,
    row_idx: usize,
}

impl RowStream {
    fn new(receiver: BatchReceiver) -> Self {
        Self {
            receiver,
            current_batch: None,
            row_idx: 0,
        }
    }

    async fn next_row(&mut self) -> Result<Option<Vec<Value>>, String> {
        loop {
            if let Some(batch) = self.current_batch.as_ref() {
                if self.row_idx < batch.len() {
                    let row_refs = batch.row(self.row_idx).map_err(|e| e.to_string())?;
                    let mut row = Vec::with_capacity(row_refs.len());
                    for value in row_refs {
                        row.push(value.clone());
                    }
                    self.row_idx += 1;
                    if self.row_idx >= batch.len() {
                        self.current_batch = None;
                        self.row_idx = 0;
                    }
                    return Ok(Some(row));
                } else {
                    self.current_batch = None;
                    self.row_idx = 0;
                }
            }

            match self.receiver.recv().await {
                Some(batch) => {
                    if batch.is_empty() {
                        continue;
                    }
                    self.current_batch = Some(batch);
                    self.row_idx = 0;
                }
                None => return Ok(None),
            }
        }
    }
}

struct HeapItem {
    shard_idx: usize,
    row: Vec<Value>,
    order_index: usize,
    ascending: bool,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.shard_idx == other.shard_idx && self.row == other.row
    }
}

impl Eq for HeapItem {}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        let lhs = &self.row[self.order_index];
        let rhs = &other.row[self.order_index];
        let ord = compare_json_values(lhs, rhs).then_with(|| other.shard_idx.cmp(&self.shard_idx));
        if self.ascending { ord.reverse() } else { ord }
    }
}

fn compare_json_values(a: &Value, b: &Value) -> Ordering {
    if let (Some(va), Some(vb)) = (a.as_u64(), b.as_u64()) {
        return va.cmp(&vb);
    }
    if let (Some(va), Some(vb)) = (a.as_i64(), b.as_i64()) {
        return va.cmp(&vb);
    }
    if let (Some(va), Some(vb)) = (a.as_f64(), b.as_f64()) {
        return va.partial_cmp(&vb).unwrap_or(Ordering::Equal);
    }
    if let (Some(va), Some(vb)) = (a.as_bool(), b.as_bool()) {
        return va.cmp(&vb);
    }
    if let (Some(va), Some(vb)) = (a.as_str(), b.as_str()) {
        return va.cmp(vb);
    }
    a.to_string().cmp(&b.to_string())
}
