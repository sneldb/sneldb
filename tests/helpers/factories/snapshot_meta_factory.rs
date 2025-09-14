use crate::engine::core::snapshot::snapshot_meta::SnapshotMeta;

/// Factory for creating `SnapshotMeta` instances in tests.
pub struct SnapshotMetaFactory {
    uid: String,
    context_id: String,
    from_ts: u64,
    to_ts: u64,
}

impl SnapshotMetaFactory {
    pub fn new() -> Self {
        Self {
            uid: "uid-default".to_string(),
            context_id: "ctx-1".to_string(),
            from_ts: 100,
            to_ts: 200,
        }
    }

    pub fn with_uid(mut self, uid: &str) -> Self {
        self.uid = uid.to_string();
        self
    }

    pub fn with_context_id(mut self, context_id: &str) -> Self {
        self.context_id = context_id.to_string();
        self
    }

    pub fn with_from_ts(mut self, from_ts: u64) -> Self {
        self.from_ts = from_ts;
        if self.to_ts < self.from_ts {
            self.to_ts = self.from_ts;
        }
        self
    }

    pub fn with_to_ts(mut self, to_ts: u64) -> Self {
        self.to_ts = to_ts.max(self.from_ts);
        self
    }

    pub fn with_range(mut self, from_ts: u64, to_ts: u64) -> Self {
        self.from_ts = from_ts;
        self.to_ts = to_ts.max(from_ts);
        self
    }

    pub fn create(self) -> SnapshotMeta {
        SnapshotMeta {
            uid: self.uid,
            context_id: self.context_id,
            from_ts: self.from_ts,
            to_ts: self.to_ts,
        }
    }

    pub fn create_list(self, count: usize) -> Vec<SnapshotMeta> {
        (0..count)
            .map(|i| SnapshotMeta {
                uid: format!("{}-{}", self.uid, i + 1),
                context_id: format!("{}-{}", self.context_id, i + 1),
                from_ts: self.from_ts + (i as u64) * 10,
                to_ts: self.to_ts + (i as u64) * 10,
            })
            .collect()
    }
}
