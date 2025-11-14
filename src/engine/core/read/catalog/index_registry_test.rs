use crate::engine::core::read::catalog::{IndexKind, IndexRegistry, SegmentIndexCatalog};

fn write_icx(
    base_dir: &std::path::Path,
    segment_id: &str,
    uid: &str,
    catalog: SegmentIndexCatalog,
) {
    let seg_dir = base_dir.join(segment_id);
    std::fs::create_dir_all(&seg_dir).expect("failed to create segment dir");
    let icx_path = seg_dir.join(format!("{}.icx", uid));
    catalog
        .save(&icx_path)
        .expect("failed to save SegmentIndexCatalog");
}

#[test]
fn index_registry_empty_returns_no_kinds() {
    let reg = IndexRegistry::new();

    assert!(!reg.has_catalog("seg-x"));

    let field_kinds = reg.available_for("seg-x", "any");
    assert!(
        field_kinds.is_empty(),
        "expected empty kinds for missing catalog"
    );

    let global_kinds = reg.available_global("seg-x");
    assert!(
        global_kinds.is_empty(),
        "expected empty global kinds for missing catalog"
    );
}

#[test]
fn index_registry_insert_and_lookup() {
    let mut reg = IndexRegistry::new();

    let uid = "u".to_string();
    let segment_id = "seg-1".to_string();
    let mut cat = SegmentIndexCatalog::new(uid, segment_id.clone());

    // Field kinds
    let et_kinds = IndexKind::ZONE_XOR_INDEX | IndexKind::ENUM_BITMAP;
    cat.set_field_kind("event_type", et_kinds);
    let ts_kinds = IndexKind::TS_CALENDAR | IndexKind::TS_ZTI;
    cat.set_field_kind("timestamp", ts_kinds);

    // Global kinds
    let global = IndexKind::ZONE_INDEX | IndexKind::RLTE;
    cat.add_global_kind(global);

    reg.insert_catalog(cat);

    assert!(reg.has_catalog(&segment_id));

    let k_et = reg.available_for(&segment_id, "event_type");
    assert_eq!(k_et, et_kinds);
    let k_ts = reg.available_for(&segment_id, "timestamp");
    assert_eq!(k_ts, ts_kinds);
    let k_unknown = reg.available_for(&segment_id, "unknown");
    assert!(k_unknown.is_empty());

    let g = reg.available_global(&segment_id);
    assert_eq!(g, global);
}

#[test]
fn index_registry_loads_only_existing_icx() {
    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();

    let uid = "uid-load";
    // Prepare segments: A (has icx), B (missing), C (has icx)
    let mut cat_a = SegmentIndexCatalog::new(uid.to_string(), "A".to_string());
    cat_a.set_field_kind("event_type", IndexKind::ZONE_XOR_INDEX);
    cat_a.add_global_kind(IndexKind::ZONE_INDEX);
    write_icx(&base_dir, "A", uid, cat_a.clone());

    // B intentionally missing

    let mut cat_c = SegmentIndexCatalog::new(uid.to_string(), "C".to_string());
    cat_c.set_field_kind("plan", IndexKind::ENUM_BITMAP);
    cat_c.add_global_kind(IndexKind::ZONE_INDEX | IndexKind::RLTE);
    write_icx(&base_dir, "C", uid, cat_c.clone());

    let mut reg = IndexRegistry::new();
    let segs = vec!["A".to_string(), "B".to_string(), "C".to_string()];
    reg.load_for_segments(&base_dir, &segs, uid);

    // A present
    assert!(reg.has_catalog("A"));
    assert_eq!(
        reg.available_for("A", "event_type"),
        IndexKind::ZONE_XOR_INDEX
    );
    assert_eq!(reg.available_global("A"), IndexKind::ZONE_INDEX);

    // B missing
    assert!(!reg.has_catalog("B"));
    assert!(reg.available_for("B", "any").is_empty());
    assert!(reg.available_global("B").is_empty());

    // C present
    assert!(reg.has_catalog("C"));
    assert_eq!(reg.available_for("C", "plan"), IndexKind::ENUM_BITMAP);
    assert_eq!(
        reg.available_global("C"),
        IndexKind::ZONE_INDEX | IndexKind::RLTE
    );
}

#[test]
fn index_registry_load_is_idempotent_and_skips_existing_entries() {
    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let uid = "uid-skip";

    // On-disk catalog for segment D contains field "disk_only"
    let mut disk_cat = SegmentIndexCatalog::new(uid.to_string(), "D".to_string());
    disk_cat.set_field_kind("disk_only", IndexKind::ENUM_BITMAP);
    write_icx(&base_dir, "D", uid, disk_cat);

    let mut reg = IndexRegistry::new();

    // Pre-insert an in-memory catalog for D containing field "mem_only"
    let mut mem_cat = SegmentIndexCatalog::new(uid.to_string(), "D".to_string());
    mem_cat.set_field_kind("mem_only", IndexKind::XOR_FIELD_FILTER);
    reg.insert_catalog(mem_cat);

    // Call load_for_segments twice; the pre-existing entry should be left intact
    let segs = vec!["D".to_string()];
    reg.load_for_segments(&base_dir, &segs, uid);
    reg.load_for_segments(&base_dir, &segs, uid);

    // Validate: mem_only present (from in-memory), disk_only absent (skipped load due to existing entry)
    assert!(reg.has_catalog("D"));
    assert_eq!(
        reg.available_for("D", "mem_only"),
        IndexKind::XOR_FIELD_FILTER
    );
    assert!(reg.available_for("D", "disk_only").is_empty());
}
