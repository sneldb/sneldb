use crate::engine::core::CandidateZone;
use crate::engine::core::ZoneIndex;
use std::collections::BTreeMap;
use tempfile::tempdir;

#[test]
fn test_zone_index_roundtrip() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("index.bin");

    let mut index = ZoneIndex::default();
    index.insert("signup", "ctx42", 10);
    index.insert("signup", "ctx99", 20);
    index.insert("login", "ctx42", 5);

    index.write_to_path(&path).unwrap();
    let loaded = ZoneIndex::load_from_path(&path).unwrap();

    assert_eq!(index.index, loaded.index);
}
#[test]
fn test_zone_index_write_and_load_from_path() {
    let mut index = ZoneIndex::default();
    index.insert("signup", "ctx42", 0);
    index.insert("signup", "ctx42", 1);
    index.insert("signup", "ctx99", 2);

    let dir = tempdir().unwrap();
    let path = dir.path().join("zone.idx");

    index.write_to_path(&path).unwrap();
    assert!(path.exists());

    let loaded = ZoneIndex::load_from_path(&path).unwrap();

    assert_eq!(loaded.index["signup"]["ctx42"], vec![0, 1]);
    assert_eq!(loaded.index["signup"]["ctx99"], vec![2]);
}

#[test]
fn test_find_candidate_zones_with_context_id() {
    let mut index = ZoneIndex::default();
    index.insert("login", "ctxA", 1);
    index.insert("login", "ctxA", 2);

    let candidates = index.find_candidate_zones("login", Some("ctxA"), "001");
    let mut ids: Vec<u32> = candidates.iter().map(|c| c.zone_id).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2]);
    assert!(candidates.iter().all(|c| c.segment_id == "001"));
}

#[test]
fn test_find_candidate_zones_without_context_id() {
    let mut index = ZoneIndex::default();
    index.insert("signup", "ctxX", 5);
    index.insert("signup", "ctxY", 7);
    // overlap across contexts and duplicates within the same context
    index.insert("signup", "ctxX", 7);
    index.insert("signup", "ctxY", 7);

    let mut result = index.find_candidate_zones("signup", None, "002");
    result.sort_by_key(|c| c.zone_id);

    // Expect unique ids only
    assert_eq!(result.len(), 2);
    assert_eq!(result[0], CandidateZone::new(5, "002".into()));
    assert_eq!(result[1], CandidateZone::new(7, "002".into()));
}

#[test]
fn test_find_candidate_zones_with_unknown_event_type() {
    let index = ZoneIndex::default();

    let candidates = index.find_candidate_zones("nonexistent", None, "003");
    assert!(candidates.is_empty());
}

#[test]
fn test_find_candidate_zones_with_unknown_context_id() {
    let mut index = ZoneIndex::default();
    index.insert("payment", "known_ctx", 9);

    let candidates = index.find_candidate_zones("payment", Some("unknown_ctx"), "004");
    assert!(candidates.is_empty());
}

#[test]
fn test_find_candidate_zones_skips_empty_zone_lists() {
    let mut index = ZoneIndex::default();
    index.index.insert(
        "event".into(),
        BTreeMap::from([("ctx1".into(), vec![]), ("ctx2".into(), vec![11])]),
    );

    let result = index.find_candidate_zones("event", None, "005");

    assert_eq!(result.len(), 1);
    assert_eq!(result[0], CandidateZone::new(11, "005".into()));
}

#[test]
fn test_zone_index_find_candidates_after_roundtrip() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("zone.idx");

    // Step 1: Create and populate a ZoneIndex
    let mut original = ZoneIndex::default();
    original.insert("signup", "ctx42", 10);
    original.insert("signup", "ctx42", 20); // Should be ignored, only first used
    original.insert("signup", "ctx99", 30);
    original.insert("login", "ctxA", 42);

    // Step 2: Write to disk
    original.write_to_path(&path).expect("Write failed");
    assert!(path.exists());

    // Step 3: Load back from disk
    let reloaded = ZoneIndex::load_from_path(&path).expect("Load failed");

    // Step 4: Check that find_candidate_zones still works
    let mut signup_candidates = reloaded.find_candidate_zones("signup", None, "001");
    signup_candidates.sort_by_key(|c| c.zone_id);

    assert_eq!(signup_candidates.len(), 3);
    assert_eq!(signup_candidates[0], CandidateZone::new(10, "001".into()));
    assert_eq!(signup_candidates[1], CandidateZone::new(20, "001".into()));
    assert_eq!(signup_candidates[2], CandidateZone::new(30, "001".into()));

    let mut ctx42_candidates = reloaded.find_candidate_zones("signup", Some("ctx42"), "002");
    ctx42_candidates.sort_by_key(|c| c.zone_id);
    assert_eq!(ctx42_candidates.len(), 2);
    assert_eq!(ctx42_candidates[0], CandidateZone::new(10, "002".into()));
    assert_eq!(ctx42_candidates[1], CandidateZone::new(20, "002".into()));

    let login_candidates = reloaded.find_candidate_zones("login", Some("ctxA"), "003");
    assert_eq!(login_candidates.len(), 1);
    assert_eq!(login_candidates[0], CandidateZone::new(42, "003".into()));

    let unknown_event = reloaded.find_candidate_zones("not_exists", None, "099");
    assert!(unknown_event.is_empty());

    let unknown_ctx = reloaded.find_candidate_zones("signup", Some("does_not_exist"), "099");
    assert!(unknown_ctx.is_empty());
}
