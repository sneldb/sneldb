use crate::test_helpers::factory::Factory;
use std::collections::HashMap;

#[test]
fn test_candidate_zone_factory() {
    let zone = Factory::candidate_zone()
        .with("zone_id", 42)
        .with("segment_id", "seg-002")
        .with_values(HashMap::from([(
            "plan".to_string(),
            vec!["pro".to_string(), "premium".to_string()],
        )]))
        .create();

    assert_eq!(zone.zone_id, 42);
    assert_eq!(zone.segment_id, "seg-002");
    assert_eq!(zone.values["plan"].len(), 2);
}
