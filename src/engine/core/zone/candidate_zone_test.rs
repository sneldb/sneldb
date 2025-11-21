use crate::engine::core::CandidateZone;
use crate::engine::core::ColumnValues;
use crate::engine::core::ZoneMeta;
use crate::engine::core::read::cache::DecompressedBlock;
use crate::engine::core::read::cache::QueryCaches;
use crate::shared::config::CONFIG;
use crate::test_helpers::factories::zone_meta_factory::ZoneMetaFactory;
use std::collections::HashMap;
use std::fs::create_dir_all;
use std::sync::Arc;
use std::time::Duration;

#[test]
fn creates_new_candidate_zone() {
    let zone = CandidateZone::new(5, "00007".to_string());
    assert_eq!(zone.zone_id, 5);
    assert_eq!(zone.segment_id, "00007");
    assert!(zone.values.is_empty());
}

#[test]
fn sets_zone_values_correctly() {
    let mut zone = CandidateZone::new(2, "00003".to_string());
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
    let zones = CandidateZone::create_all_zones_for_segment("00999");
    assert_eq!(zones.len(), CONFIG.engine.fill_factor);
    assert_eq!(zones[0].zone_id, 0);
    assert_eq!(
        zones.last().unwrap().zone_id,
        (CONFIG.engine.fill_factor - 1) as u32
    );
    assert!(zones.iter().all(|z| z.segment_id == "00999"));
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
    let mut zone = CandidateZone::new(3, "00042".to_string());
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

#[test]
fn create_all_zones_from_meta_cached_uses_per_query_cache() {
    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = "00011";
    let uid = "uid_cached";
    let seg_dir = base_dir.join(segment_id);
    create_dir_all(&seg_dir).unwrap();

    // Write .zones with two metas (zone ids implied by length)
    let m1 = ZoneMetaFactory::new()
        .with("zone_id", 0)
        .with("uid", uid)
        .with("segment_id", 1u64)
        .create();
    let m2 = ZoneMetaFactory::new()
        .with("zone_id", 1)
        .with("uid", uid)
        .with("segment_id", 1u64)
        .create();
    ZoneMeta::save(uid, &[m1.clone(), m2.clone()], &seg_dir).unwrap();

    let caches = QueryCaches::new(base_dir.clone());

    let zones1 = CandidateZone::create_all_zones_for_segment_from_meta_cached(
        &base_dir,
        segment_id,
        uid,
        Some(&caches),
    );
    assert_eq!(zones1.len(), 2);

    // Rewrite .zones with a third meta; per-query memoization should still return 2
    std::thread::sleep(Duration::from_millis(20));
    let m3 = ZoneMetaFactory::new()
        .with("zone_id", 2)
        .with("uid", uid)
        .with("segment_id", 1u64)
        .create();
    ZoneMeta::save(uid, &[m1, m2, m3], &seg_dir).unwrap();

    let zones2 = CandidateZone::create_all_zones_for_segment_from_meta_cached(
        &base_dir,
        segment_id,
        uid,
        Some(&caches),
    );
    assert_eq!(zones2.len(), 2, "expected cached length within same query");

    // New query caches should observe the new length (3)
    let caches2 = QueryCaches::new(base_dir.clone());
    let zones3 = CandidateZone::create_all_zones_for_segment_from_meta_cached(
        &base_dir,
        segment_id,
        uid,
        Some(&caches2),
    );
    assert_eq!(zones3.len(), 3);
}

#[test]
fn create_all_zones_from_meta_cached_falls_back_to_fill_factor_when_missing() {
    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = "00998";
    let uid = "uid_missing";

    // No .zones file present; expect fill_factor fallback regardless of caches
    let caches = QueryCaches::new(base_dir.clone());
    let zones = CandidateZone::create_all_zones_for_segment_from_meta_cached(
        &base_dir,
        segment_id,
        uid,
        Some(&caches),
    );
    assert_eq!(zones.len(), CONFIG.engine.fill_factor);
}

#[test]
fn set_uid_stores_uid_correctly() {
    let mut zone = CandidateZone::new(0, "00001".to_string());
    assert_eq!(zone.uid(), None, "New zone should have no UID");

    zone.set_uid("test-uid-123");
    assert_eq!(
        zone.uid(),
        Some("test-uid-123"),
        "UID should be set correctly"
    );
}

#[test]
fn set_uid_accepts_string_and_str() {
    let mut zone1 = CandidateZone::new(0, "00001".to_string());
    let mut zone2 = CandidateZone::new(1, "00001".to_string());

    zone1.set_uid("str-uid");
    zone2.set_uid(String::from("string-uid"));

    assert_eq!(zone1.uid(), Some("str-uid"));
    assert_eq!(zone2.uid(), Some("string-uid"));
}

#[test]
fn clear_uid_removes_uid() {
    let mut zone = CandidateZone::new(0, "00001".to_string());
    zone.set_uid("test-uid");
    assert_eq!(zone.uid(), Some("test-uid"));

    zone.clear_uid();
    assert_eq!(zone.uid(), None, "UID should be cleared");
}

#[test]
fn uid_accessor_returns_correct_reference() {
    let mut zone = CandidateZone::new(0, "00001".to_string());
    zone.set_uid("my-uid");

    let uid_ref = zone.uid();
    assert_eq!(uid_ref, Some("my-uid"));

    // Verify it's a reference, not owned
    let uid_ref2 = zone.uid();
    assert_eq!(uid_ref2, Some("my-uid"));
    assert_eq!(zone.uid(), Some("my-uid"), "UID should persist");
}

#[test]
fn create_all_zones_from_meta_sets_uid_on_all_zones() {
    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = "00012";
    let uid = "test-uid-meta";
    let seg_dir = base_dir.join(segment_id);
    std::fs::create_dir_all(&seg_dir).unwrap();

    // Create zone metadata
    let m1 = ZoneMetaFactory::new()
        .with("zone_id", 0)
        .with("uid", uid)
        .with("segment_id", 1u64)
        .create();
    let m2 = ZoneMetaFactory::new()
        .with("zone_id", 1)
        .with("uid", uid)
        .with("segment_id", 1u64)
        .create();
    ZoneMeta::save(uid, &[m1, m2], &seg_dir).unwrap();

    let zones = CandidateZone::create_all_zones_for_segment_from_meta(
        &base_dir,
        segment_id,
        uid,
    );

    assert_eq!(zones.len(), 2);
    for zone in &zones {
        assert_eq!(
            zone.uid(),
            Some(uid),
            "All zones should have UID set from metadata"
        );
    }
}
