use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct HighWaterMark {
    pub timestamp: u64,
    pub event_id: u64,
}

impl HighWaterMark {
    pub fn new(timestamp: u64, event_id: u64) -> Self {
        Self {
            timestamp,
            event_id,
        }
    }

    pub fn advance(&mut self, timestamp: u64, event_id: u64) {
        if (timestamp, event_id) > (self.timestamp, self.event_id) {
            self.timestamp = timestamp;
            self.event_id = event_id;
        }
    }

    pub fn satisfies(&self, timestamp: u64, event_id: u64) -> bool {
        (timestamp, event_id) > (self.timestamp, self.event_id)
    }

    pub fn is_zero(&self) -> bool {
        self.timestamp == 0 && self.event_id == 0
    }
}
