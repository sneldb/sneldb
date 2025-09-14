use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotMeta {
    pub uid: String,
    pub context_id: String,
    pub from_ts: u64,
    pub to_ts: u64,
}

impl SnapshotMeta {
    /// Creates a new `SnapshotMeta`, normalizing the range so that from_ts <= to_ts.
    pub fn new(
        uid: impl Into<String>,
        context_id: impl Into<String>,
        from_ts: u64,
        to_ts: u64,
    ) -> Self {
        let (from_ts, to_ts) = if to_ts < from_ts {
            (to_ts, from_ts)
        } else {
            (from_ts, to_ts)
        };
        Self {
            uid: uid.into(),
            context_id: context_id.into(),
            from_ts,
            to_ts,
        }
    }

    /// Duration (inclusive range) in timestamp units.
    pub fn duration(&self) -> u64 {
        self.to_ts.saturating_sub(self.from_ts)
    }

    /// Returns true if the given timestamp is within [from_ts, to_ts].
    pub fn contains_ts(&self, ts: u64) -> bool {
        ts >= self.from_ts && ts <= self.to_ts
    }

    /// Returns true if the time ranges overlap (ignores uid/context).
    pub fn overlaps_time(&self, other: &Self) -> bool {
        self.from_ts <= other.to_ts && other.from_ts <= self.to_ts
    }

    /// Returns true if the time ranges are adjacent (touching end-to-start).
    pub fn is_adjacent_time(&self, other: &Self) -> bool {
        self.to_ts + 1 == other.from_ts || other.to_ts + 1 == self.from_ts
    }

    /// If uid and context_id match and the ranges overlap or are adjacent, returns a merged range.
    pub fn merge_with_if_compatible(&self, other: &Self) -> Option<Self> {
        if self.uid != other.uid || self.context_id != other.context_id {
            return None;
        }
        if self.overlaps_time(other) || self.is_adjacent_time(other) {
            let from_ts = self.from_ts.min(other.from_ts);
            let to_ts = self.to_ts.max(other.to_ts);
            return Some(Self {
                uid: self.uid.clone(),
                context_id: self.context_id.clone(),
                from_ts,
                to_ts,
            });
        }
        None
    }
}
