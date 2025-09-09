use crate::test_helpers::factory::Factory;

#[cfg(test)]
#[test]
fn test_zone_plan_factory() {
    let event = Factory::event()
        .with("event_type", "login")
        .with("context_id", "ctx1")
        .with("payload", serde_json::json!({ "age": 25 }))
        .create();

    let events = vec![event];

    let zone = Factory::zone_plan()
        .with("events", serde_json::to_value(&events).unwrap())
        .create();

    assert_eq!(zone.events.len(), 1);
    assert_eq!(zone.events[0].event_type, "login");
}
