use super::event_id::{EventId, EventIdGenerator};

const TIMESTAMP_BITS: u64 = 42;
const SHARD_ID_BITS: u64 = 10;
const SEQUENCE_BITS: u64 = 12;
const CUSTOM_EPOCH_MILLIS: u64 = 1_609_459_200_000;
const SHARD_MASK: u64 = (1 << SHARD_ID_BITS) - 1;

fn shard_bits(raw: u64) -> u64 {
    (raw >> SEQUENCE_BITS) & SHARD_MASK
}

fn timestamp_millis(raw: u64) -> u64 {
    let timestamp_component = raw >> (SHARD_ID_BITS + SEQUENCE_BITS);
    (timestamp_component & ((1 << TIMESTAMP_BITS) - 1)) + CUSTOM_EPOCH_MILLIS
}

#[test]
fn event_id_roundtrip() {
    let raw = 42_u64;
    let id = EventId::from_raw(raw);
    assert_eq!(id.raw(), raw);
    assert!(!id.is_zero());

    let zero = EventId::default();
    assert!(zero.is_zero());
}

#[test]
fn generator_encodes_shard_and_monotonicity() {
    let mut generator = EventIdGenerator::new();

    let first = generator.next(7).raw();
    let second = generator.next(7).raw();
    let third = generator.next(7).raw();

    assert!(first < second && second < third, "Event IDs must be strictly increasing");
    assert_eq!(shard_bits(first), 7);
    assert_eq!(shard_bits(second), 7);
    assert_eq!(shard_bits(third), 7);
}

#[test]
fn generator_timestamp_never_decreases() {
    let mut generator = EventIdGenerator::new();

    let mut previous_timestamp = 0_u64;
    for _ in 0..128 {
        let raw = generator.next(1).raw();
        let ts = timestamp_millis(raw);
        assert!(ts >= previous_timestamp);
        previous_timestamp = ts;
    }
}
