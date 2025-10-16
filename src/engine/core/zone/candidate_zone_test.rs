use crate::engine::core::CandidateZone;
use crate::engine::core::ColumnValues;
use crate::engine::core::read::cache::DecompressedBlock;
use crate::shared::config::CONFIG;
use std::collections::HashMap;
use std::sync::Arc;

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
    let empty_block = Arc::new(DecompressedBlock::from_bytes(Vec::new()));
    values.insert(
        "event_type".to_string(),
        ColumnValues::new(Arc::clone(&empty_block), Vec::new()),
    );
    values.insert(
        "region".to_string(),
        ColumnValues::new(Arc::clone(&empty_block), Vec::new()),
    );
    zone.set_values(values.clone());
    assert_eq!(zone.values.len(), 2);
}

#[test]
fn creates_all_zones_for_segment_according_to_fill_factor() {
    let zones = CandidateZone::create_all_zones_for_segment("segment-999");
    assert_eq!(zones.len(), CONFIG.engine.fill_factor);
    assert_eq!(zones[0].zone_id, 0);
    assert_eq!(
        zones.last().unwrap().zone_id,
        (CONFIG.engine.fill_factor - 1) as u32
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

#[test]
fn zone_values_access_via_column_values() {
    let mut zone = CandidateZone::new(3, "segment-vals".to_string());
    // Build encoded bytes: [u16 len][bytes] per value
    let mut bytes: Vec<u8> = Vec::new();
    let mut ranges: Vec<(usize, usize)> = Vec::new();
    for v in ["east", "west"].iter() {
        let v_bytes = v.as_bytes();
        let len = v_bytes.len() as u16;
        let start = bytes.len();
        bytes.extend_from_slice(&len.to_le_bytes());
        bytes.extend_from_slice(v_bytes);
        ranges.push((start + 2, v_bytes.len()));
    }
    let block = Arc::new(DecompressedBlock::from_bytes(bytes));
    let mut values = HashMap::new();
    values.insert("region".to_string(), ColumnValues::new(block, ranges));
    zone.set_values(values);

    // Access via stored ColumnValues
    let region = zone.values.get("region").unwrap();
    assert_eq!(region.len(), 2);
    assert_eq!(region.get_str_at(0), Some("east"));
    assert_eq!(region.get_str_at(1), Some("west"));
}
