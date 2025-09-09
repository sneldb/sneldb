use crate::test_helpers::factory::Factory;

#[cfg(test)]
#[test]
fn test_zone_row_factory() {
    let row = Factory::zone_row()
        .with_context_id("ctx42")
        .with_timestamp("987654")
        .with_event_type("signup")
        .with_payload_field("plan", "enterprise")
        .with_segment_id(123)
        .with_zone_id(7)
        .create();

    assert_eq!(row.context_id, "ctx42");
    assert_eq!(row.timestamp, "987654");
    assert_eq!(row.event_type, "signup");
    assert_eq!(row.segment_id, 123);
    assert_eq!(row.zone_id, 7);
    assert_eq!(row.payload["plan"], "enterprise");
}
