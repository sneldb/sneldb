use super::plan_outcome::PlanOutcome;
use crate::command::types::PickedZones;
use std::collections::HashMap;

#[test]
fn without_zones_creates_outcome_with_none() {
    let outcome = PlanOutcome::without_zones();
    assert!(outcome.picked_zones.is_none());
}

#[test]
fn with_zones_creates_outcome_with_zones() {
    let mut zones = HashMap::new();
    zones.insert(
        0,
        PickedZones {
            uid: "uid1".to_string(),
            field: "timestamp".to_string(),
            asc: true,
            cutoff: "1000".to_string(),
            k: 10,
            zones: vec![("zone1".to_string(), 1)],
        },
    );

    let outcome = PlanOutcome::with_zones(zones.clone());
    assert!(outcome.picked_zones.is_some());
    assert_eq!(outcome.picked_zones.as_ref().unwrap().len(), 1);
    assert_eq!(
        outcome.picked_zones.as_ref().unwrap().get(&0).unwrap().uid,
        "uid1"
    );
}

#[test]
fn with_zones_handles_empty_hashmap() {
    let zones = HashMap::new();
    let outcome = PlanOutcome::with_zones(zones);
    assert!(outcome.picked_zones.is_some());
    assert_eq!(outcome.picked_zones.as_ref().unwrap().len(), 0);
}

#[test]
fn with_zones_stores_multiple_shards() {
    let mut zones = HashMap::new();
    zones.insert(
        0,
        PickedZones {
            uid: "uid0".to_string(),
            field: "timestamp".to_string(),
            asc: true,
            cutoff: "1000".to_string(),
            k: 10,
            zones: vec![("zone1".to_string(), 1)],
        },
    );
    zones.insert(
        1,
        PickedZones {
            uid: "uid1".to_string(),
            field: "timestamp".to_string(),
            asc: true,
            cutoff: "2000".to_string(),
            k: 20,
            zones: vec![("zone2".to_string(), 2), ("zone3".to_string(), 3)],
        },
    );

    let outcome = PlanOutcome::with_zones(zones);
    assert_eq!(outcome.picked_zones.as_ref().unwrap().len(), 2);
    assert!(outcome.picked_zones.as_ref().unwrap().contains_key(&0));
    assert!(outcome.picked_zones.as_ref().unwrap().contains_key(&1));
}
