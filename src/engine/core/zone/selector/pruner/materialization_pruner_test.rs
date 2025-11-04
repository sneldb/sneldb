use tempfile::tempdir;

use crate::engine::core::CandidateZone;
use crate::engine::core::ZoneMeta;
use crate::engine::core::read::cache::QueryCaches;
use crate::engine::core::zone::selector::pruner::materialization_pruner::MaterializationPruner;
use crate::test_helpers::factory::Factory;

#[test]
fn filters_zones_created_before_materialization() {
    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let seg1 = shard_dir.join("001");
    std::fs::create_dir_all(&seg1).unwrap();

    let uid = "test_uid";
    let materialization_created_at = 1500u64;

    // Create zones with different created_at timestamps
    let zone_0 = Factory::zone_meta()
        .with("zone_id", 0)
        .with("uid", uid)
        .with("segment_id", 1u64)
        .with("start_row", 0)
        .with("end_row", 99)
        .with("timestamp_min", 1_000_000u64)
        .with("timestamp_max", 1_000_999u64)
        .with("created_at", 1000u64) // Before materialization
        .create();

    let zone_1 = Factory::zone_meta()
        .with("zone_id", 1)
        .with("uid", uid)
        .with("segment_id", 1u64)
        .with("start_row", 100)
        .with("end_row", 199)
        .with("timestamp_min", 1_001_000u64)
        .with("timestamp_max", 1_001_999u64)
        .with("created_at", 2000u64) // After materialization
        .create();

    ZoneMeta::save(uid, &[zone_0.clone(), zone_1.clone()], &seg1).unwrap();

    let base_dir = shard_dir.to_path_buf();
    let pruner = MaterializationPruner::new(&base_dir, None, materialization_created_at);

    let candidate_zones = vec![
        CandidateZone::new(0, "001".to_string()),
        CandidateZone::new(1, "001".to_string()),
    ];

    let result = pruner.apply(&candidate_zones, uid);

    // Only zone 1 should remain (created_at=2000 > 1500)
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].zone_id, 1);
    assert_eq!(result[0].segment_id, "001");
}

#[test]
fn filters_zones_with_equal_created_at() {
    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let seg1 = shard_dir.join("001");
    std::fs::create_dir_all(&seg1).unwrap();

    let uid = "test_uid";
    let materialization_created_at = 1500u64;

    let zone_0 = Factory::zone_meta()
        .with("zone_id", 0)
        .with("uid", uid)
        .with("segment_id", 1u64)
        .with("start_row", 0)
        .with("end_row", 99)
        .with("timestamp_min", 1_000_000u64)
        .with("timestamp_max", 1_000_999u64)
        .with("created_at", materialization_created_at) // Equal to materialization
        .create();

    let zone_1 = Factory::zone_meta()
        .with("zone_id", 1)
        .with("uid", uid)
        .with("segment_id", 1u64)
        .with("start_row", 100)
        .with("end_row", 199)
        .with("timestamp_min", 1_001_000u64)
        .with("timestamp_max", 1_001_999u64)
        .with("created_at", materialization_created_at + 1) // One unit after
        .create();

    ZoneMeta::save(uid, &[zone_0.clone(), zone_1.clone()], &seg1).unwrap();

    let base_dir = shard_dir.to_path_buf();
    let pruner = MaterializationPruner::new(&base_dir, None, materialization_created_at);

    let candidate_zones = vec![
        CandidateZone::new(0, "001".to_string()),
        CandidateZone::new(1, "001".to_string()),
    ];

    let result = pruner.apply(&candidate_zones, uid);

    // Zone 0 should be filtered (created_at == materialization_created_at)
    // Zone 1 should remain (created_at > materialization_created_at)
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].zone_id, 1);
}

#[test]
fn returns_empty_for_empty_input() {
    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");

    let base_dir = shard_dir.to_path_buf();
    let pruner = MaterializationPruner::new(&base_dir, None, 1000u64);

    let result = pruner.apply(&[], "test_uid");

    assert_eq!(result.len(), 0);
}

#[test]
fn handles_missing_zone_metadata_file() {
    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let seg1 = shard_dir.join("001");
    std::fs::create_dir_all(&seg1).unwrap();

    let uid = "test_uid";

    let base_dir = shard_dir.to_path_buf();
    let pruner = MaterializationPruner::new(&base_dir, None, 1000u64);

    let candidate_zones = vec![CandidateZone::new(0, "001".to_string())];

    // When zone metadata file doesn't exist, pruner should fail open (include all zones)
    let result = pruner.apply(&candidate_zones, uid);

    assert_eq!(result.len(), 1, "Should fail open when metadata is missing");
    assert_eq!(result[0].zone_id, 0);
}

#[test]
fn handles_invalid_zone_id() {
    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let seg1 = shard_dir.join("001");
    std::fs::create_dir_all(&seg1).unwrap();

    let uid = "test_uid";

    // Create metadata with only zone 0
    let zone_0 = Factory::zone_meta()
        .with("zone_id", 0)
        .with("uid", uid)
        .with("segment_id", 1u64)
        .with("start_row", 0)
        .with("end_row", 99)
        .with("timestamp_min", 1_000_000u64)
        .with("timestamp_max", 1_000_999u64)
        .with("created_at", 2000u64)
        .create();

    ZoneMeta::save(uid, &[zone_0.clone()], &seg1).unwrap();

    let base_dir = shard_dir.to_path_buf();
    let pruner = MaterializationPruner::new(&base_dir, None, 1000u64);

    // Request zone 5 which doesn't exist in metadata
    let candidate_zones = vec![CandidateZone::new(5, "001".to_string())];

    // Should fail open (include zone even if ID is out of bounds)
    let result = pruner.apply(&candidate_zones, uid);

    assert_eq!(result.len(), 1, "Should fail open for invalid zone ID");
    assert_eq!(result[0].zone_id, 5);
}

#[test]
fn handles_multiple_segments() {
    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let seg1 = shard_dir.join("001");
    let seg2 = shard_dir.join("002");
    std::fs::create_dir_all(&seg1).unwrap();
    std::fs::create_dir_all(&seg2).unwrap();

    let uid = "test_uid";
    let materialization_created_at = 1500u64;

    // Segment 1: zone 0 created before, zone 1 created after
    let zone_0_seg1 = Factory::zone_meta()
        .with("zone_id", 0)
        .with("uid", uid)
        .with("segment_id", 1u64)
        .with("start_row", 0)
        .with("end_row", 99)
        .with("timestamp_min", 1_000_000u64)
        .with("timestamp_max", 1_000_999u64)
        .with("created_at", 1000u64)
        .create();

    let zone_1_seg1 = Factory::zone_meta()
        .with("zone_id", 1)
        .with("uid", uid)
        .with("segment_id", 1u64)
        .with("start_row", 100)
        .with("end_row", 199)
        .with("timestamp_min", 1_001_000u64)
        .with("timestamp_max", 1_001_999u64)
        .with("created_at", 2000u64)
        .create();

    // Segment 2: zone 0 created after
    let zone_0_seg2 = Factory::zone_meta()
        .with("zone_id", 0)
        .with("uid", uid)
        .with("segment_id", 2u64)
        .with("start_row", 0)
        .with("end_row", 99)
        .with("timestamp_min", 2_000_000u64)
        .with("timestamp_max", 2_000_999u64)
        .with("created_at", 2000u64)
        .create();

    ZoneMeta::save(uid, &[zone_0_seg1, zone_1_seg1], &seg1).unwrap();
    ZoneMeta::save(uid, &[zone_0_seg2], &seg2).unwrap();

    let base_dir = shard_dir.to_path_buf();
    let pruner = MaterializationPruner::new(&base_dir, None, materialization_created_at);

    let candidate_zones = vec![
        CandidateZone::new(0, "001".to_string()),
        CandidateZone::new(1, "001".to_string()),
        CandidateZone::new(0, "002".to_string()),
    ];

    let result = pruner.apply(&candidate_zones, uid);

    // Only zones created after materialization should remain
    assert_eq!(result.len(), 2);
    assert!(
        result
            .iter()
            .any(|z| z.zone_id == 1 && z.segment_id == "001")
    );
    assert!(
        result
            .iter()
            .any(|z| z.zone_id == 0 && z.segment_id == "002")
    );
}

#[test]
fn handles_multiple_zones_per_segment() {
    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let seg1 = shard_dir.join("001");
    std::fs::create_dir_all(&seg1).unwrap();

    let uid = "test_uid";
    let materialization_created_at = 1500u64;

    // Create 5 zones with varying created_at timestamps
    let zones = vec![
        Factory::zone_meta()
            .with("zone_id", 0)
            .with("uid", uid)
            .with("segment_id", 1u64)
            .with("start_row", 0)
            .with("end_row", 99)
            .with("timestamp_min", 1_000_000u64)
            .with("timestamp_max", 1_000_999u64)
            .with("created_at", 1000u64) // Before
            .create(),
        Factory::zone_meta()
            .with("zone_id", 1)
            .with("uid", uid)
            .with("segment_id", 1u64)
            .with("start_row", 100)
            .with("end_row", 199)
            .with("timestamp_min", 1_001_000u64)
            .with("timestamp_max", 1_001_999u64)
            .with("created_at", 2000u64) // After
            .create(),
        Factory::zone_meta()
            .with("zone_id", 2)
            .with("uid", uid)
            .with("segment_id", 1u64)
            .with("start_row", 200)
            .with("end_row", 299)
            .with("timestamp_min", 1_002_000u64)
            .with("timestamp_max", 1_002_999u64)
            .with("created_at", 1400u64) // Before
            .create(),
        Factory::zone_meta()
            .with("zone_id", 3)
            .with("uid", uid)
            .with("segment_id", 1u64)
            .with("start_row", 300)
            .with("end_row", 399)
            .with("timestamp_min", 1_003_000u64)
            .with("timestamp_max", 1_003_999u64)
            .with("created_at", 1600u64) // After
            .create(),
        Factory::zone_meta()
            .with("zone_id", 4)
            .with("uid", uid)
            .with("segment_id", 1u64)
            .with("start_row", 400)
            .with("end_row", 499)
            .with("timestamp_min", 1_004_000u64)
            .with("timestamp_max", 1_004_999u64)
            .with("created_at", 1500u64) // Equal (should be filtered)
            .create(),
    ];

    ZoneMeta::save(uid, &zones, &seg1).unwrap();

    let base_dir = shard_dir.to_path_buf();
    let pruner = MaterializationPruner::new(&base_dir, None, materialization_created_at);

    let candidate_zones: Vec<CandidateZone> = (0..5)
        .map(|i| CandidateZone::new(i, "001".to_string()))
        .collect();

    let result = pruner.apply(&candidate_zones, uid);

    // Only zones 1 and 3 should remain (created_at > 1500)
    assert_eq!(result.len(), 2);
    assert!(result.iter().any(|z| z.zone_id == 1));
    assert!(result.iter().any(|z| z.zone_id == 3));
}

#[test]
fn handles_cache_miss_gracefully() {
    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let seg1 = shard_dir.join("001");
    std::fs::create_dir_all(&seg1).unwrap();

    let uid = "test_uid";

    let zone_0 = Factory::zone_meta()
        .with("zone_id", 0)
        .with("uid", uid)
        .with("segment_id", 1u64)
        .with("start_row", 0)
        .with("end_row", 99)
        .with("timestamp_min", 1_000_000u64)
        .with("timestamp_max", 1_000_999u64)
        .with("created_at", 2000u64)
        .create();

    ZoneMeta::save(uid, &[zone_0.clone()], &seg1).unwrap();

    // Create a cache that doesn't have this segment (simulating cache miss)
    let base_dir = shard_dir.to_path_buf();
    let cache = QueryCaches::new(base_dir.clone());
    let pruner = MaterializationPruner::new(&base_dir, Some(&cache), 1000u64);

    let candidate_zones = vec![CandidateZone::new(0, "001".to_string())];

    // Should fallback to direct file load when cache misses
    let result = pruner.apply(&candidate_zones, uid);

    // Zone should be included since created_at=2000 > 1000
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].zone_id, 0);
}

#[test]
fn handles_very_large_created_at_values() {
    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let seg1 = shard_dir.join("001");
    std::fs::create_dir_all(&seg1).unwrap();

    let uid = "test_uid";
    let materialization_created_at = 1_700_000_000_000u64; // Unix timestamp in milliseconds

    let zone_0 = Factory::zone_meta()
        .with("zone_id", 0)
        .with("uid", uid)
        .with("segment_id", 1u64)
        .with("start_row", 0)
        .with("end_row", 99)
        .with("timestamp_min", 1_000_000u64)
        .with("timestamp_max", 1_000_999u64)
        .with("created_at", 1_700_000_000_001u64) // One millisecond after
        .create();

    let zone_1 = Factory::zone_meta()
        .with("zone_id", 1)
        .with("uid", uid)
        .with("segment_id", 1u64)
        .with("start_row", 100)
        .with("end_row", 199)
        .with("timestamp_min", 1_001_000u64)
        .with("timestamp_max", 1_001_999u64)
        .with("created_at", 1_700_000_000_000u64) // Equal
        .create();

    ZoneMeta::save(uid, &[zone_0.clone(), zone_1.clone()], &seg1).unwrap();

    let base_dir = shard_dir.to_path_buf();
    let pruner = MaterializationPruner::new(&base_dir, None, materialization_created_at);

    let candidate_zones = vec![
        CandidateZone::new(0, "001".to_string()),
        CandidateZone::new(1, "001".to_string()),
    ];

    let result = pruner.apply(&candidate_zones, uid);

    // Only zone 0 should remain (created_at > materialization_created_at)
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].zone_id, 0);
}

#[test]
fn handles_zero_created_at_timestamp() {
    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let seg1 = shard_dir.join("001");
    std::fs::create_dir_all(&seg1).unwrap();

    let uid = "test_uid";
    let materialization_created_at = 100u64;

    // Zone with created_at = 0 (default for old zones)
    let zone_0 = Factory::zone_meta()
        .with("zone_id", 0)
        .with("uid", uid)
        .with("segment_id", 1u64)
        .with("start_row", 0)
        .with("end_row", 99)
        .with("timestamp_min", 1_000_000u64)
        .with("timestamp_max", 1_000_999u64)
        .with("created_at", 0u64)
        .create();

    let zone_1 = Factory::zone_meta()
        .with("zone_id", 1)
        .with("uid", uid)
        .with("segment_id", 1u64)
        .with("start_row", 100)
        .with("end_row", 199)
        .with("timestamp_min", 1_001_000u64)
        .with("timestamp_max", 1_001_999u64)
        .with("created_at", 200u64)
        .create();

    ZoneMeta::save(uid, &[zone_0.clone(), zone_1.clone()], &seg1).unwrap();

    let base_dir = shard_dir.to_path_buf();
    let pruner = MaterializationPruner::new(&base_dir, None, materialization_created_at);

    let candidate_zones = vec![
        CandidateZone::new(0, "001".to_string()),
        CandidateZone::new(1, "001".to_string()),
    ];

    let result = pruner.apply(&candidate_zones, uid);

    // Zone 0 (created_at=0 <= 100) should be filtered
    // Zone 1 (created_at=200 > 100) should remain
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].zone_id, 1);
}

#[test]
fn handles_all_zones_created_before_materialization() {
    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let seg1 = shard_dir.join("001");
    std::fs::create_dir_all(&seg1).unwrap();

    let uid = "test_uid";
    let materialization_created_at = 5000u64;

    let zones = vec![
        Factory::zone_meta()
            .with("zone_id", 0)
            .with("uid", uid)
            .with("segment_id", 1u64)
            .with("start_row", 0)
            .with("end_row", 99)
            .with("timestamp_min", 1_000_000u64)
            .with("timestamp_max", 1_000_999u64)
            .with("created_at", 1000u64)
            .create(),
        Factory::zone_meta()
            .with("zone_id", 1)
            .with("uid", uid)
            .with("segment_id", 1u64)
            .with("start_row", 100)
            .with("end_row", 199)
            .with("timestamp_min", 1_001_000u64)
            .with("timestamp_max", 1_001_999u64)
            .with("created_at", 2000u64)
            .create(),
        Factory::zone_meta()
            .with("zone_id", 2)
            .with("uid", uid)
            .with("segment_id", 1u64)
            .with("start_row", 200)
            .with("end_row", 299)
            .with("timestamp_min", 1_002_000u64)
            .with("timestamp_max", 1_002_999u64)
            .with("created_at", 3000u64)
            .create(),
    ];

    ZoneMeta::save(uid, &zones, &seg1).unwrap();

    let base_dir = shard_dir.to_path_buf();
    let pruner = MaterializationPruner::new(&base_dir, None, materialization_created_at);

    let candidate_zones: Vec<CandidateZone> = (0..3)
        .map(|i| CandidateZone::new(i, "001".to_string()))
        .collect();

    let result = pruner.apply(&candidate_zones, uid);

    // All zones created before materialization should be filtered out
    assert_eq!(result.len(), 0);
}

#[test]
fn handles_all_zones_created_after_materialization() {
    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let seg1 = shard_dir.join("001");
    std::fs::create_dir_all(&seg1).unwrap();

    let uid = "test_uid";
    let materialization_created_at = 500u64;

    let zones = vec![
        Factory::zone_meta()
            .with("zone_id", 0)
            .with("uid", uid)
            .with("segment_id", 1u64)
            .with("start_row", 0)
            .with("end_row", 99)
            .with("timestamp_min", 1_000_000u64)
            .with("timestamp_max", 1_000_999u64)
            .with("created_at", 1000u64)
            .create(),
        Factory::zone_meta()
            .with("zone_id", 1)
            .with("uid", uid)
            .with("segment_id", 1u64)
            .with("start_row", 100)
            .with("end_row", 199)
            .with("timestamp_min", 1_001_000u64)
            .with("timestamp_max", 1_001_999u64)
            .with("created_at", 2000u64)
            .create(),
        Factory::zone_meta()
            .with("zone_id", 2)
            .with("uid", uid)
            .with("segment_id", 1u64)
            .with("start_row", 200)
            .with("end_row", 299)
            .with("timestamp_min", 1_002_000u64)
            .with("timestamp_max", 1_002_999u64)
            .with("created_at", 3000u64)
            .create(),
    ];

    ZoneMeta::save(uid, &zones, &seg1).unwrap();

    let base_dir = shard_dir.to_path_buf();
    let pruner = MaterializationPruner::new(&base_dir, None, materialization_created_at);

    let candidate_zones: Vec<CandidateZone> = (0..3)
        .map(|i| CandidateZone::new(i, "001".to_string()))
        .collect();

    let result = pruner.apply(&candidate_zones, uid);

    // All zones created after materialization should be retained
    assert_eq!(result.len(), 3);
    assert!(result.iter().any(|z| z.zone_id == 0));
    assert!(result.iter().any(|z| z.zone_id == 1));
    assert!(result.iter().any(|z| z.zone_id == 2));
}
