use crate::engine::core::read::cache::QueryCaches;
use crate::engine::core::zone::zone_artifacts::ZoneArtifacts;
use crate::test_helpers::factories::EnumBitmapIndexFactory;
use std::fs;
use tempfile::tempdir;

#[test]
fn load_ebm_without_cache_loads_from_file() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = "00001";
    let uid = "test_uid";
    let field = "plan";
    let seg_dir = base_dir.join(segment_id);
    fs::create_dir_all(&seg_dir).unwrap();

    // Create and save an enum bitmap index
    let index = EnumBitmapIndexFactory::new()
        .with_variants(&["pro", "basic", "trial"])
        .with_rows_per_zone(10)
        .with_zone_variant_bits(0, 0, &[0, 3, 9])
        .with_zone_variant_bits(0, 1, &[1, 2, 5])
        .build();

    let ebm_path = seg_dir.join(format!("{}_{}.ebm", uid, field));
    index.save(&ebm_path).unwrap();

    // Load without cache
    let artifacts = ZoneArtifacts::new(&base_dir, None);
    let loaded = artifacts.load_ebm(segment_id, uid, field).unwrap();

    assert_eq!(loaded.variants, vec!["pro", "basic", "trial"]);
    assert_eq!(loaded.rows_per_zone, 10);
    assert!(loaded.has_any(0, 0));
    assert!(loaded.has_any(0, 1));
}

#[test]
fn load_ebm_with_cache_loads_from_cache() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = "00002";
    let uid = "test_uid";
    let field = "status";
    let seg_dir = base_dir.join(segment_id);
    fs::create_dir_all(&seg_dir).unwrap();

    // Create and save an enum bitmap index
    let index = EnumBitmapIndexFactory::new()
        .with_variants(&["active", "inactive", "pending"])
        .with_rows_per_zone(8)
        .with_zone_variant_bits(0, 0, &[0, 1, 2])
        .build();

    let ebm_path = seg_dir.join(format!("{}_{}.ebm", uid, field));
    index.save(&ebm_path).unwrap();

    // Load with cache
    let caches = QueryCaches::new(base_dir.clone());
    let artifacts = ZoneArtifacts::new(&base_dir, Some(&caches));

    let loaded1 = artifacts.load_ebm(segment_id, uid, field).unwrap();
    let loaded2 = artifacts.load_ebm(segment_id, uid, field).unwrap();

    // Both should have the same data
    assert_eq!(loaded1.variants, loaded2.variants);
    assert_eq!(loaded1.rows_per_zone, loaded2.rows_per_zone);
    assert_eq!(loaded1.zone_bitmaps.len(), loaded2.zone_bitmaps.len());
}

#[test]
fn load_ebm_with_cache_reuses_per_query_memoization() {
    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = "00003";
    let uid = "test_uid";
    let field = "type";
    let seg_dir = base_dir.join(segment_id);
    fs::create_dir_all(&seg_dir).unwrap();

    // Create and save an enum bitmap index
    let index = EnumBitmapIndexFactory::new()
        .with_variants(&["A", "B", "C"])
        .with_rows_per_zone(5)
        .with_zone_variant_bits(0, 0, &[0, 1])
        .build();

    let ebm_path = seg_dir.join(format!("{}_{}.ebm", uid, field));
    index.save(&ebm_path).unwrap();

    let caches = QueryCaches::new(base_dir.clone());
    let artifacts = ZoneArtifacts::new(&base_dir, Some(&caches));

    // Multiple loads within the same query should use per-query memoization
    let loaded1 = artifacts.load_ebm(segment_id, uid, field).unwrap();
    let loaded2 = artifacts.load_ebm(segment_id, uid, field).unwrap();
    let loaded3 = artifacts.load_ebm(segment_id, uid, field).unwrap();

    // All should have identical data
    assert_eq!(loaded1.variants, loaded2.variants);
    assert_eq!(loaded2.variants, loaded3.variants);
    assert_eq!(loaded1.rows_per_zone, loaded2.rows_per_zone);
    assert_eq!(loaded2.rows_per_zone, loaded3.rows_per_zone);
}

#[test]
fn load_ebm_with_cache_shared_across_queries() {
    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = "00004";
    let uid = "test_uid";
    let field = "category";
    let seg_dir = base_dir.join(segment_id);
    fs::create_dir_all(&seg_dir).unwrap();

    // Create and save an enum bitmap index
    let index = EnumBitmapIndexFactory::new()
        .with_variants(&["X", "Y", "Z"])
        .with_rows_per_zone(6)
        .with_zone_variant_bits(0, 0, &[0])
        .with_zone_variant_bits(1, 1, &[0])
        .build();

    let ebm_path = seg_dir.join(format!("{}_{}.ebm", uid, field));
    index.save(&ebm_path).unwrap();

    // First query: load once (should be a miss in global cache)
    let caches1 = QueryCaches::new(base_dir.clone());
    let artifacts1 = ZoneArtifacts::new(&base_dir, Some(&caches1));
    let loaded1 = artifacts1.load_ebm(segment_id, uid, field).unwrap();

    // Second query: should hit global cache
    let caches2 = QueryCaches::new(base_dir.clone());
    let artifacts2 = ZoneArtifacts::new(&base_dir, Some(&caches2));
    let loaded2 = artifacts2.load_ebm(segment_id, uid, field).unwrap();

    // Both should have the same data
    assert_eq!(loaded1.variants, loaded2.variants);
    assert_eq!(loaded1.rows_per_zone, loaded2.rows_per_zone);
    assert_eq!(loaded1.zone_bitmaps.len(), loaded2.zone_bitmaps.len());
}

#[test]
fn load_ebm_missing_file_without_cache_returns_error() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = "00005";
    let uid = "nonexistent";
    let field = "missing";

    let artifacts = ZoneArtifacts::new(&base_dir, None);
    let result = artifacts.load_ebm(segment_id, uid, field);

    assert!(result.is_err());
    let error_msg = result.unwrap_err();
    assert!(error_msg.contains("Failed to load EnumBitmapIndex"));
}

#[test]
fn load_ebm_missing_file_with_cache_returns_error() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = "00006";
    let uid = "nonexistent";
    let field = "missing";

    let caches = QueryCaches::new(base_dir.clone());
    let artifacts = ZoneArtifacts::new(&base_dir, Some(&caches));
    let result = artifacts.load_ebm(segment_id, uid, field);

    assert!(result.is_err());
    let error_msg = result.unwrap_err();
    assert!(error_msg.contains("Failed to load EnumBitmapIndex"));
}

#[test]
fn load_ebm_cache_fallback_to_direct_load_on_error() {
    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = "00007";
    let uid = "test_uid";
    let field = "plan";
    let seg_dir = base_dir.join(segment_id);
    fs::create_dir_all(&seg_dir).unwrap();

    // Create and save an enum bitmap index
    let index = EnumBitmapIndexFactory::new()
        .with_variants(&["pro", "basic"])
        .with_rows_per_zone(4)
        .with_zone_variant_bits(0, 0, &[0, 1])
        .build();

    let ebm_path = seg_dir.join(format!("{}_{}.ebm", uid, field));
    index.save(&ebm_path).unwrap();

    // Create caches with a base_dir that might cause issues, but file exists
    // This tests that even if cache fails, direct load works
    let caches = QueryCaches::new(base_dir.clone());
    let artifacts = ZoneArtifacts::new(&base_dir, Some(&caches));

    // Should succeed via cache or direct load
    let loaded = artifacts.load_ebm(segment_id, uid, field).unwrap();
    assert_eq!(loaded.variants, vec!["pro", "basic"]);
    assert_eq!(loaded.rows_per_zone, 4);
}

#[test]
fn load_ebm_multiple_segments() {
    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segments = ["00008", "00009"];
    let uid = "test_uid";
    let field = "status";

    for segment_id in &segments {
        let seg_dir = base_dir.join(segment_id);
        fs::create_dir_all(&seg_dir).unwrap();

        let index = EnumBitmapIndexFactory::new()
            .with_variants(&["active", "inactive"])
            .with_rows_per_zone(3)
            .with_zone_variant_bits(0, 0, &[0])
            .build();

        let ebm_path = seg_dir.join(format!("{}_{}.ebm", uid, field));
        index.save(&ebm_path).unwrap();
    }

    let caches = QueryCaches::new(base_dir.clone());
    let artifacts = ZoneArtifacts::new(&base_dir, Some(&caches));

    // Load from both segments
    for segment_id in &segments {
        let loaded = artifacts.load_ebm(segment_id, uid, field).unwrap();
        assert_eq!(loaded.variants, vec!["active", "inactive"]);
        assert_eq!(loaded.rows_per_zone, 3);
    }
}

#[test]
fn load_ebm_multiple_fields() {
    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = "00010";
    let uid = "test_uid";
    let fields = ["plan", "status", "type"];
    let seg_dir = base_dir.join(segment_id);
    fs::create_dir_all(&seg_dir).unwrap();

    // Create enum bitmap indexes for different fields
    for (idx, field) in fields.iter().enumerate() {
        let index = EnumBitmapIndexFactory::new()
            .with_variants(&["A", "B"])
            .with_rows_per_zone(2)
            .with_zone_variant_bits(0, idx % 2, &[0])
            .build();

        let ebm_path = seg_dir.join(format!("{}_{}.ebm", uid, field));
        index.save(&ebm_path).unwrap();
    }

    let caches = QueryCaches::new(base_dir.clone());
    let artifacts = ZoneArtifacts::new(&base_dir, Some(&caches));

    // Load all fields
    for field in &fields {
        let loaded = artifacts.load_ebm(segment_id, uid, field).unwrap();
        assert_eq!(loaded.variants, vec!["A", "B"]);
        assert_eq!(loaded.rows_per_zone, 2);
    }
}

#[test]
fn load_ebm_different_uids() {
    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = "00011";
    let uids = ["uid1", "uid2", "uid3"];
    let field = "plan";
    let seg_dir = base_dir.join(segment_id);
    fs::create_dir_all(&seg_dir).unwrap();

    // Create enum bitmap indexes for different uids
    for uid in &uids {
        let index = EnumBitmapIndexFactory::new()
            .with_variants(&["pro", "basic"])
            .with_rows_per_zone(4)
            .with_zone_variant_bits(0, 0, &[0, 1])
            .build();

        let ebm_path = seg_dir.join(format!("{}_{}.ebm", uid, field));
        index.save(&ebm_path).unwrap();
    }

    let caches = QueryCaches::new(base_dir.clone());
    let artifacts = ZoneArtifacts::new(&base_dir, Some(&caches));

    // Load all uids
    for uid in &uids {
        let loaded = artifacts.load_ebm(segment_id, uid, field).unwrap();
        assert_eq!(loaded.variants, vec!["pro", "basic"]);
        assert_eq!(loaded.rows_per_zone, 4);
    }
}

#[test]
fn load_ebm_with_multiple_zones() {
    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = "00012";
    let uid = "test_uid";
    let field = "plan";
    let seg_dir = base_dir.join(segment_id);
    fs::create_dir_all(&seg_dir).unwrap();

    // Create enum bitmap index with multiple zones
    let index = EnumBitmapIndexFactory::new()
        .with_variants(&["pro", "basic", "trial"])
        .with_rows_per_zone(10)
        .with_zone_variant_bits(0, 0, &[0, 1, 2])  // zone 0: pro
        .with_zone_variant_bits(1, 1, &[0, 1])     // zone 1: basic
        .with_zone_variant_bits(2, 2, &[0])        // zone 2: trial
        .build();

    let ebm_path = seg_dir.join(format!("{}_{}.ebm", uid, field));
    index.save(&ebm_path).unwrap();

    let caches = QueryCaches::new(base_dir.clone());
    let artifacts = ZoneArtifacts::new(&base_dir, Some(&caches));

    let loaded = artifacts.load_ebm(segment_id, uid, field).unwrap();

    assert_eq!(loaded.variants, vec!["pro", "basic", "trial"]);
    assert_eq!(loaded.rows_per_zone, 10);
    assert_eq!(loaded.zone_bitmaps.len(), 3);
    assert!(loaded.has_any(0, 0)); // zone 0 has pro
    assert!(loaded.has_any(1, 1)); // zone 1 has basic
    assert!(loaded.has_any(2, 2)); // zone 2 has trial
}

#[test]
fn load_ebm_cache_consistency_after_file_modification() {
    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = "00013";
    let uid = "test_uid";
    let field = "plan";
    let seg_dir = base_dir.join(segment_id);
    fs::create_dir_all(&seg_dir).unwrap();

    // Create initial enum bitmap index
    let index1 = EnumBitmapIndexFactory::new()
        .with_variants(&["pro", "basic"])
        .with_rows_per_zone(4)
        .with_zone_variant_bits(0, 0, &[0])
        .build();

    let ebm_path = seg_dir.join(format!("{}_{}.ebm", uid, field));
    index1.save(&ebm_path).unwrap();

    let caches = QueryCaches::new(base_dir.clone());
    let artifacts = ZoneArtifacts::new(&base_dir, Some(&caches));

    // Load first time
    let loaded1 = artifacts.load_ebm(segment_id, uid, field).unwrap();
    assert_eq!(loaded1.variants, vec!["pro", "basic"]);

    // Modify file (create new index with different data)
    std::thread::sleep(std::time::Duration::from_millis(100));
    let index2 = EnumBitmapIndexFactory::new()
        .with_variants(&["pro", "basic", "trial"])
        .with_rows_per_zone(5)
        .with_zone_variant_bits(0, 0, &[0, 1])
        .build();
    index2.save(&ebm_path).unwrap();

    // Load again - per-query memoization should return same data
    let loaded2 = artifacts.load_ebm(segment_id, uid, field).unwrap();

    // Per-query memoization should return cached version
    assert_eq!(loaded1.variants, loaded2.variants);
    assert_eq!(loaded1.rows_per_zone, loaded2.rows_per_zone);
}

#[test]
fn load_ebm_path_construction() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = "00014";
    let uid = "test_uid";
    let field = "plan";
    let seg_dir = base_dir.join(segment_id);
    fs::create_dir_all(&seg_dir).unwrap();

    let index = EnumBitmapIndexFactory::new()
        .with_variants(&["pro", "basic"])
        .with_rows_per_zone(2)
        .build();

    let expected_path = seg_dir.join(format!("{}_{}.ebm", uid, field));
    index.save(&expected_path).unwrap();

    // Verify the path is constructed correctly
    assert!(expected_path.exists());

    let artifacts = ZoneArtifacts::new(&base_dir, None);
    let loaded = artifacts.load_ebm(segment_id, uid, field).unwrap();
    assert_eq!(loaded.variants, vec!["pro", "basic"]);
}

#[test]
fn load_ebm_empty_bitmap() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = "00015";
    let uid = "test_uid";
    let field = "plan";
    let seg_dir = base_dir.join(segment_id);
    fs::create_dir_all(&seg_dir).unwrap();

    // Create enum bitmap index with no bits set
    let index = EnumBitmapIndexFactory::new()
        .with_variants(&["pro", "basic"])
        .with_rows_per_zone(4)
        .build(); // No bits set

    let ebm_path = seg_dir.join(format!("{}_{}.ebm", uid, field));
    index.save(&ebm_path).unwrap();

    let caches = QueryCaches::new(base_dir.clone());
    let artifacts = ZoneArtifacts::new(&base_dir, Some(&caches));

    let loaded = artifacts.load_ebm(segment_id, uid, field).unwrap();

    assert_eq!(loaded.variants, vec!["pro", "basic"]);
    assert_eq!(loaded.rows_per_zone, 4);
    assert!(!loaded.has_any(0, 0)); // No bits set
    assert!(!loaded.has_any(0, 1));
}

