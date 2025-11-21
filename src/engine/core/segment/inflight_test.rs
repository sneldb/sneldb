use super::inflight::InflightSegments;

#[test]
fn guard_registers_segment_until_drop() {
    let tracker = InflightSegments::new();

    {
        let _guard = tracker.guard("00001");
        assert!(tracker.contains("00001"), "guard should insert segment");
    }

    assert!(
        !tracker.contains("00001"),
        "segment must be removed once guard drops"
    );
}

#[test]
fn disarm_prevents_automatic_removal() {
    let tracker = InflightSegments::new();

    let guard = tracker.guard("seg-disarm");
    guard.disarm();
    // guard moved/dropped here

    assert!(
        tracker.contains("seg-disarm"),
        "disarmed guard should leave segment in inflight set"
    );

    tracker.remove("seg-disarm");
    assert!(
        !tracker.contains("seg-disarm"),
        "manual removal should still work after disarm"
    );
}

#[test]
fn manual_insert_and_remove_work_without_guard() {
    let tracker = InflightSegments::new();
    tracker.insert("manual-seg");
    assert!(tracker.contains("manual-seg"));

    tracker.remove("manual-seg");
    assert!(!tracker.contains("manual-seg"));
}

#[test]
fn multiple_guards_for_same_segment_drop_cleanly() {
    let tracker = InflightSegments::new();
    let first = tracker.guard("shared-seg");
    assert!(tracker.contains("shared-seg"));

    {
        let _second = tracker.guard("shared-seg");
        assert!(
            tracker.contains("shared-seg"),
            "segment remains inflight while any guard is active"
        );
    }

    drop(first);
    assert!(
        !tracker.contains("shared-seg"),
        "segment should be removed when final guard drops"
    );
}
