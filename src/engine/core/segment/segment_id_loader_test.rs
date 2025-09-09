use std::fs::File;
use std::sync::{Arc, RwLock};
use tempfile::tempdir;

use crate::engine::core::segment::segment_id_loader::SegmentIdLoader;

#[test]
fn test_segment_id_loader_load_and_next_id() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let path = temp_dir.path();

    // Create dummy segment files
    let file_names = vec![
        "segment-00001",
        "segment-00002",
        "segment-00003",
        "not-a-segment",
        "segment-invalid",
    ];

    for name in &file_names {
        let file_path = path.join(name);
        File::create(&file_path).expect("Failed to create test file");
    }

    // Test `load`
    let loader = SegmentIdLoader::new(path.to_path_buf());
    let segment_ids = loader.load();

    assert_eq!(
        segment_ids,
        vec![
            "segment-00001".to_string(),
            "segment-00002".to_string(),
            "segment-00003".to_string(),
            "segment-invalid".to_string(), // it's included by your filter logic
        ]
    );

    // Test `next_id`
    let shared_ids = Arc::new(RwLock::new(segment_ids));
    let next = SegmentIdLoader::next_id(&shared_ids);

    // Should ignore "segment-invalid" and parse the largest numeric ID + 1
    assert_eq!(next, 4);
}

#[test]
fn test_segment_id_loader_empty_dir() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let path = temp_dir.path();

    let loader = SegmentIdLoader::new(path.to_path_buf());
    let segment_ids = loader.load();

    assert_eq!(segment_ids, Vec::<String>::new());

    let shared_ids = Arc::new(RwLock::new(segment_ids));
    let next = SegmentIdLoader::next_id(&shared_ids);

    assert_eq!(next, 0);
}
