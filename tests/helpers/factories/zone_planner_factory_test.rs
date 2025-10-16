use crate::shared::config::CONFIG;
use crate::test_helpers::factories::{EventFactory, ZonePlannerFactory};

#[test]
fn plans_zones_correctly_from_events() {
    // Generate 3 mock events
    let events = EventFactory::new().create_list(3);

    // Create the factory and run plan
    let planner = ZonePlannerFactory::new(events.clone(), "test_event").with_segment_id(42);
    let zones = planner.plan();
    let event_per_zone = CONFIG.engine.event_per_zone;
    let expected_zones = (events.len() + event_per_zone - 1) / event_per_zone;

    // Validate output
    assert_eq!(zones.len(), expected_zones);

    let zone = &zones[0];
    assert_eq!(zone.uid, "test_event");
    assert_eq!(zone.segment_id, 42);
    assert_eq!(zone.start_index, 0);
    let expected_end = std::cmp::min(event_per_zone, events.len()) - 1;
    assert_eq!(zone.end_index, expected_end);
}
