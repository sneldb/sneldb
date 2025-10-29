use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const CUSTOM_EPOCH_MILLIS: u64 = 1_609_459_200_000; // 2020-12-01T00:00:00Z
const TIMESTAMP_BITS: u64 = 42;
const SHARD_ID_BITS: u64 = 10;
const SEQUENCE_BITS: u64 = 12;

const SEQUENCE_MASK: u16 = (1 << SEQUENCE_BITS) - 1;
const SHARD_ID_MASK: u64 = (1 << SHARD_ID_BITS) - 1;

/// Opaque identifier assigned to every stored event.
#[derive(
    Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct EventId(u64);

impl EventId {
    #[inline]
    pub fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    #[inline]
    pub fn raw(self) -> u64 {
        self.0
    }

    #[inline]
    pub fn is_zero(self) -> bool {
        self.0 == 0
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<EventId> for u64 {
    #[inline]
    fn from(value: EventId) -> u64 {
        value.0
    }
}

impl From<u64> for EventId {
    #[inline]
    fn from(value: u64) -> Self {
        Self(value)
    }
}

/// Monotonic, shard-aware identifier generator inspired by Snowflake IDs.
#[derive(Debug, Default, Clone)]
pub struct EventIdGenerator {
    last_millis: u64,
    sequence: u16,
}

impl EventIdGenerator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn next(&mut self, shard_id: u16) -> EventId {
        let mut millis = current_millis();

        if millis < self.last_millis {
            // Clock moved backwards - treat as same millisecond bucket to avoid reordering.
            millis = self.last_millis;
        }

        if millis == self.last_millis {
            self.sequence = self.sequence.wrapping_add(1) & SEQUENCE_MASK;
            if self.sequence == 0 {
                millis = wait_next_millis(self.last_millis);
            }
        } else {
            self.sequence = 0;
        }

        self.last_millis = millis;

        let shard_component = (shard_id as u64) & SHARD_ID_MASK;
        let timestamp_component =
            millis.saturating_sub(CUSTOM_EPOCH_MILLIS) & ((1 << TIMESTAMP_BITS) - 1);
        let seq_component = self.sequence as u64;

        let raw = (timestamp_component << (SHARD_ID_BITS + SEQUENCE_BITS))
            | (shard_component << SEQUENCE_BITS)
            | seq_component;
        EventId::from_raw(raw)
    }
}

fn current_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as u64
}

fn wait_next_millis(last: u64) -> u64 {
    let mut now = current_millis();
    while now <= last {
        std::thread::yield_now();
        now = current_millis();
    }
    now
}
