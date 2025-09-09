use crate::engine::core::{Event, ZonePlanner};
use crate::shared::config::CONFIG;
use crate::test_helpers::factories::{EventFactory, SchemaRegistryFactory};

#[tokio::test]
async fn test_zone_planner_splits_events_into_correct_zones() {
    // Given
    let event_type = "user_created";
    let schema_factory = SchemaRegistryFactory::new();
    schema_factory
        .define_with_fields(event_type, &[("user_id", "string")])
        .await
        .unwrap();

    let registry = schema_factory.registry();
    let uid = registry.read().await.get_uid(event_type).unwrap();
    let segment_id = 7;

    // Force config to a small zone size for testing (e.g. 3 per zone)
    let zone_size = CONFIG.engine.event_per_zone;

    let total_events = zone_size * 2 + 1; // guarantees 3 zones
    let events: Vec<Event> = EventFactory::new()
        .with("event_type", event_type)
        .create_list(total_events);

    // When
    let planner = ZonePlanner::new(&uid, segment_id);
    let zones = planner.plan(&events).expect("Failed to plan zones");

    // Then
    assert_eq!(
        zones.len(),
        3,
        "Expected 3 zones for {} events",
        total_events
    );

    let mut expected_start = 0;
    for (i, zone) in zones.iter().enumerate() {
        let expected_end = (expected_start + zone_size - 1).min(total_events - 1);

        assert_eq!(zone.id, i as u32);
        assert_eq!(zone.start_index, expected_start);
        assert_eq!(zone.end_index, expected_end);
        assert_eq!(zone.uid, uid);
        assert_eq!(zone.segment_id, segment_id);
        assert_eq!(zone.event_type, event_type);

        let expected_len = expected_end - expected_start + 1;
        assert_eq!(
            zone.events.len(),
            expected_len,
            "Zone {} has wrong event count",
            i
        );

        expected_start = expected_end + 1;
    }
}

#[tokio::test]
async fn test_zone_planner_fails_on_empty_input() {
    let planner = ZonePlanner::new("some_uid", 1);
    let result = planner.plan(&[]);
    assert!(result.is_err(), "Expected error on empty input");
}
