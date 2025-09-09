use crate::engine::core::CandidateZone;
use crate::shared::config::CONFIG;
use std::collections::HashMap;

#[test]
fn creates_new_candidate_zone() {
    let zone = CandidateZone::new(5, "segment-007".to_string());
    assert_eq!(zone.zone_id, 5);
    assert_eq!(zone.segment_id, "segment-007");
    assert!(zone.values.is_empty());
}

#[test]
fn sets_zone_values_correctly() {
    let mut zone = CandidateZone::new(2, "segment-003".to_string());
    let mut values = HashMap::new();
    values.insert("event_type".to_string(), vec!["user".to_string()]);
    values.insert("region".to_string(), vec!["eu".to_string()]);
    zone.set_values(values.clone());
    assert_eq!(zone.values, values);
}

#[test]
fn creates_all_zones_for_segment_according_to_fill_factor() {
    let zones = CandidateZone::create_all_zones_for_segment("segment-999");
    assert_eq!(zones.len(), CONFIG.engine.fill_factor());
    assert_eq!(zones[0].zone_id, 0);
    assert_eq!(
        zones.last().unwrap().zone_id,
        (CONFIG.engine.fill_factor() - 1) as u32
    );
    assert!(zones.iter().all(|z| z.segment_id == "segment-999"));
}

#[test]
fn deduplicates_duplicate_zones() {
    let z1 = CandidateZone::new(1, "s1".to_string());
    let z2 = CandidateZone::new(2, "s1".to_string());
    let z3 = CandidateZone::new(1, "s1".to_string()); // duplicate of z1

    let result = CandidateZone::uniq(vec![z1.clone(), z2.clone(), z3]);
    assert_eq!(result.len(), 2);
    assert!(result.contains(&z1));
    assert!(result.contains(&z2));
}
