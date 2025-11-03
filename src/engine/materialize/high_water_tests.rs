use super::high_water::HighWaterMark;

#[test]
fn new_sets_timestamp_and_event_id() {
    let hw = HighWaterMark::new(42, 7);
    assert_eq!(hw.timestamp, 42);
    assert_eq!(hw.event_id, 7);
}

#[test]
fn advance_updates_when_greater() {
    let mut hw = HighWaterMark::new(100, 10);
    hw.advance(200, 20);
    assert_eq!(hw.timestamp, 200);
    assert_eq!(hw.event_id, 20);
}

#[test]
fn advance_ignores_when_not_greater() {
    let mut hw = HighWaterMark::new(200, 20);
    hw.advance(100, 30);
    assert_eq!(hw.timestamp, 200);
    assert_eq!(hw.event_id, 20);
    hw.advance(200, 10);
    assert_eq!(hw.timestamp, 200);
    assert_eq!(hw.event_id, 20);
}

#[test]
fn satisfies_checks_lexicographic_order() {
    let hw = HighWaterMark::new(100, 10);
    assert!(hw.satisfies(200, 0));
    assert!(hw.satisfies(100, 11));
    assert!(!hw.satisfies(100, 10));
    assert!(!hw.satisfies(50, 100));
}

#[test]
fn is_zero_detects_zero_mark() {
    assert!(HighWaterMark::default().is_zero());
    assert!(
        HighWaterMark {
            timestamp: 0,
            event_id: 0
        }
        .is_zero()
    );
    assert!(!HighWaterMark::new(1, 0).is_zero());
    assert!(!HighWaterMark::new(0, 1).is_zero());
}
