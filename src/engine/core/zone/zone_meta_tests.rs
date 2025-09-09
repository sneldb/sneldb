use crate::engine::core::ZoneMeta;
use crate::test_helpers::factory::Factory;

#[test]
fn test_zone_meta_build_from_zone_plan() {
    let zone_plan = Factory::zone_plan().create();

    let meta = ZoneMeta::build(&zone_plan).unwrap();

    assert_eq!(meta.zone_id, zone_plan.id);
    assert_eq!(meta.uid, zone_plan.uid);
    assert_eq!(meta.segment_id, zone_plan.segment_id);
    assert_eq!(meta.start_row, zone_plan.start_index as u32);
    assert_eq!(meta.end_row, zone_plan.end_index as u32);
    assert!(meta.timestamp_min <= meta.timestamp_max);
}

#[test]
fn test_zone_meta_build_all_skips_empty_plans() {
    let good_plan = Factory::zone_plan().create();
    let empty_plan = Factory::zone_plan()
        .with("events", serde_json::json!([]))
        .create();

    let metas = ZoneMeta::build_all(&[good_plan, empty_plan]);

    assert_eq!(metas.len(), 1);
}

#[test]
fn test_zone_meta_sort_by_timestamp_min() {
    let events = serde_json::to_value(Factory::event().create_list(2)).unwrap();
    let events2 = serde_json::to_value(Factory::event().create_list(2)).unwrap();

    let mut metas = vec![
        Factory::zone_plan().with("events", events).create(),
        Factory::zone_plan().with("events", events2).create(),
    ]
    .into_iter()
    .map(|zp| ZoneMeta::build(&zp).unwrap())
    .collect::<Vec<_>>();

    let sorted = ZoneMeta::sort_by(&mut metas, "timestamp_min");

    assert!(sorted[0].timestamp_min < sorted[1].timestamp_min);
}

#[test]
fn test_zone_meta_save_and_load_round_trip() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let path = tmp_dir.path().to_path_buf();

    let zone_plan = Factory::zone_plan().create();
    let meta = ZoneMeta::build(&zone_plan).unwrap();

    ZoneMeta::save(&zone_plan.uid, &[meta.clone()], &path).unwrap();

    let file_path = path.join(format!("{}.zones", zone_plan.uid));
    assert!(file_path.exists());

    let loaded = ZoneMeta::load(&file_path).unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0], meta);
}
