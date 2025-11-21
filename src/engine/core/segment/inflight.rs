use std::collections::HashSet;
use std::sync::{Arc, RwLock};

/// Tracks segments that are currently in-flight (being flushed) so readers can
/// fall back to safer strategies until indexes are durable.
#[derive(Debug, Clone, Default)]
pub struct InflightSegments {
    inner: Arc<RwLock<HashSet<String>>>,
}

impl InflightSegments {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn guard<S: Into<String>>(&self, segment_id: S) -> InflightGuard {
        let segment_id = segment_id.into();
        self.insert(&segment_id);
        InflightGuard {
            tracker: self.clone(),
            segment_id,
            active: true,
        }
    }

    pub fn insert(&self, segment_id: &str) {
        let mut guard = self.inner.write().expect("inflight lock poisoned");
        guard.insert(segment_id.to_string());
    }

    pub fn remove(&self, segment_id: &str) {
        let mut guard = self.inner.write().expect("inflight lock poisoned");
        guard.remove(segment_id);
    }

    pub fn contains(&self, segment_id: &str) -> bool {
        self.inner
            .read()
            .expect("inflight lock poisoned")
            .contains(segment_id)
    }
}

/// Guard that automatically removes the segment from the in-flight set when dropped.
pub struct InflightGuard {
    tracker: InflightSegments,
    segment_id: String,
    active: bool,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        if self.active {
            self.tracker.remove(&self.segment_id);
        }
    }
}

impl InflightGuard {
    pub fn disarm(mut self) {
        self.active = false;
    }
}
