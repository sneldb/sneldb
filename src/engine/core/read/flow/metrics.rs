use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct FlowMetrics {
    total_sent_batches: AtomicU64,
    total_sent_rows: AtomicU64,
    total_received_batches: AtomicU64,
    total_received_rows: AtomicU64,
    pending_batches: AtomicU64,
    backpressure_events: AtomicU64,
    peak_pending: AtomicU64,
}

impl FlowMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn on_send_success(&self, rows: u64) {
        self.total_sent_batches.fetch_add(1, Ordering::Relaxed);
        self.total_sent_rows.fetch_add(rows, Ordering::Relaxed);
        self.pending_inc();
    }

    pub fn record_backpressure(&self) {
        self.backpressure_events.fetch_add(1, Ordering::Relaxed);
    }

    pub fn on_receive(&self, rows: u64) {
        self.total_received_batches.fetch_add(1, Ordering::Relaxed);
        self.total_received_rows.fetch_add(rows, Ordering::Relaxed);
        self.pending_dec();
    }

    pub fn total_sent_rows(&self) -> u64 {
        self.total_sent_rows.load(Ordering::Relaxed)
    }

    pub fn total_received_rows(&self) -> u64 {
        self.total_received_rows.load(Ordering::Relaxed)
    }

    pub fn total_sent_batches(&self) -> u64 {
        self.total_sent_batches.load(Ordering::Relaxed)
    }

    pub fn total_received_batches(&self) -> u64 {
        self.total_received_batches.load(Ordering::Relaxed)
    }

    pub fn pending_batches(&self) -> u64 {
        self.pending_batches.load(Ordering::Relaxed)
    }

    pub fn peak_pending_batches(&self) -> u64 {
        self.peak_pending.load(Ordering::Relaxed)
    }

    pub fn backpressure_events(&self) -> u64 {
        self.backpressure_events.load(Ordering::Relaxed)
    }

    fn pending_inc(&self) {
        let pending = self.pending_batches.fetch_add(1, Ordering::Relaxed) + 1;
        loop {
            let current_peak = self.peak_pending.load(Ordering::Relaxed);
            if pending <= current_peak {
                break;
            }
            if self
                .peak_pending
                .compare_exchange(current_peak, pending, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }

    fn pending_dec(&self) {
        self.pending_batches.fetch_sub(1, Ordering::Relaxed);
    }
}
