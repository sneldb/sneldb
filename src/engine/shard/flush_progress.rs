use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
pub struct FlushProgress {
    submitted: AtomicU64,
    completed: AtomicU64,
}

impl FlushProgress {
    pub fn new() -> Self {
        Self {
            submitted: AtomicU64::new(0),
            completed: AtomicU64::new(0),
        }
    }

    pub fn next_id(&self) -> u64 {
        self.submitted.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub fn snapshot(&self) -> u64 {
        self.submitted.load(Ordering::SeqCst)
    }

    pub fn mark_completed(&self, id: u64) {
        let mut current = self.completed.load(Ordering::SeqCst);
        while id > current {
            match self
                .completed
                .compare_exchange(current, id, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    pub fn completed(&self) -> u64 {
        self.completed.load(Ordering::SeqCst)
    }
}
