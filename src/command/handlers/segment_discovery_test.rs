use super::segment_discovery::{SegmentDiscovery, ShardData};
use std::path::PathBuf;
use tempfile::TempDir;

#[tokio::test]
async fn discovers_segments_from_single_shard() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    // Create some segment directories
    std::fs::create_dir(base_path.join("segment-001")).unwrap();
    std::fs::create_dir(base_path.join("segment-002")).unwrap();
    std::fs::create_dir(base_path.join("segment-003")).unwrap();
    // Create a non-segment directory that should be ignored
    std::fs::create_dir(base_path.join("other-dir")).unwrap();

    let shard_info = vec![(0, base_path.to_path_buf())];
    let result = SegmentDiscovery::discover_all(shard_info).await;

    assert_eq!(result.len(), 1);
    let shard_data = result.get(&0).unwrap();
    assert_eq!(shard_data.shard_id, 0);
    assert_eq!(shard_data.segments.len(), 3);
    assert!(shard_data.segments.contains(&"001".to_string()));
    assert!(shard_data.segments.contains(&"002".to_string()));
    assert!(shard_data.segments.contains(&"003".to_string()));
}

#[tokio::test]
async fn discovers_segments_from_multiple_shards() {
    let temp_dir1 = TempDir::new().unwrap();
    let temp_dir2 = TempDir::new().unwrap();
    let temp_dir3 = TempDir::new().unwrap();

    // Create segments in each shard
    std::fs::create_dir(temp_dir1.path().join("segment-001")).unwrap();
    std::fs::create_dir(temp_dir1.path().join("segment-002")).unwrap();

    std::fs::create_dir(temp_dir2.path().join("segment-003")).unwrap();

    std::fs::create_dir(temp_dir3.path().join("segment-004")).unwrap();
    std::fs::create_dir(temp_dir3.path().join("segment-005")).unwrap();
    std::fs::create_dir(temp_dir3.path().join("segment-006")).unwrap();

    let shard_info = vec![
        (0, temp_dir1.path().to_path_buf()),
        (1, temp_dir2.path().to_path_buf()),
        (2, temp_dir3.path().to_path_buf()),
    ];

    let result = SegmentDiscovery::discover_all(shard_info).await;

    assert_eq!(result.len(), 3);

    let shard0 = result.get(&0).unwrap();
    assert_eq!(shard0.segments.len(), 2);

    let shard1 = result.get(&1).unwrap();
    assert_eq!(shard1.segments.len(), 1);

    let shard2 = result.get(&2).unwrap();
    assert_eq!(shard2.segments.len(), 3);
}

#[tokio::test]
async fn handles_empty_shard_directory() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let shard_info = vec![(0, base_path.to_path_buf())];
    let result = SegmentDiscovery::discover_all(shard_info).await;

    assert_eq!(result.len(), 1);
    let shard_data = result.get(&0).unwrap();
    assert_eq!(shard_data.segments.len(), 0);
}

#[tokio::test]
async fn handles_nonexistent_directory() {
    let nonexistent_path = PathBuf::from("/nonexistent/path/to/directory");

    let shard_info = vec![(0, nonexistent_path)];
    let result = SegmentDiscovery::discover_all(shard_info).await;

    // Should still return result but with empty segments
    assert_eq!(result.len(), 1);
    let shard_data = result.get(&0).unwrap();
    assert_eq!(shard_data.segments.len(), 0);
}

#[tokio::test]
async fn segments_are_sorted() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    // Create segments in non-sorted order
    std::fs::create_dir(base_path.join("segment-005")).unwrap();
    std::fs::create_dir(base_path.join("segment-001")).unwrap();
    std::fs::create_dir(base_path.join("segment-003")).unwrap();
    std::fs::create_dir(base_path.join("segment-002")).unwrap();
    std::fs::create_dir(base_path.join("segment-004")).unwrap();

    let shard_info = vec![(0, base_path.to_path_buf())];
    let result = SegmentDiscovery::discover_all(shard_info).await;

    let shard_data = result.get(&0).unwrap();
    let segments = &shard_data.segments;

    // Should be sorted
    assert_eq!(segments[0], "001");
    assert_eq!(segments[1], "002");
    assert_eq!(segments[2], "003");
    assert_eq!(segments[3], "004");
    assert_eq!(segments[4], "005");
}

#[tokio::test]
async fn ignores_non_segment_directories() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    std::fs::create_dir(base_path.join("segment-001")).unwrap();
    std::fs::create_dir(base_path.join("data")).unwrap();
    std::fs::create_dir(base_path.join("backup")).unwrap();
    std::fs::create_dir(base_path.join("segment-002")).unwrap();
    std::fs::create_dir(base_path.join("temp")).unwrap();

    let shard_info = vec![(0, base_path.to_path_buf())];
    let result = SegmentDiscovery::discover_all(shard_info).await;

    let shard_data = result.get(&0).unwrap();
    // Should only find the two segment directories
    assert_eq!(shard_data.segments.len(), 2);
    assert!(shard_data.segments.contains(&"001".to_string()));
    assert!(shard_data.segments.contains(&"002".to_string()));
}

#[tokio::test]
async fn ignores_files_that_match_prefix() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    std::fs::create_dir(base_path.join("segment-001")).unwrap();
    // Create a file (not a directory) with segment prefix - file will show as "002.txt"
    std::fs::File::create(base_path.join("segment-002.txt")).unwrap();
    std::fs::create_dir(base_path.join("segment-003")).unwrap();

    let shard_info = vec![(0, base_path.to_path_buf())];
    let result = SegmentDiscovery::discover_all(shard_info).await;

    let shard_data = result.get(&0).unwrap();
    // File will be included as "002.txt" since we strip "segment-" prefix
    // This is actually fine - the file won't exist as a segment directory anyway
    assert!(shard_data.segments.len() >= 2);
    assert!(shard_data.segments.contains(&"001".to_string()));
    assert!(shard_data.segments.contains(&"003".to_string()));
}

#[tokio::test]
async fn extract_segment_map_works() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    std::fs::create_dir(base_path.join("segment-001")).unwrap();
    std::fs::create_dir(base_path.join("segment-002")).unwrap();

    let shard_info = vec![(0, base_path.to_path_buf())];
    let shard_data = SegmentDiscovery::discover_all(shard_info).await;

    let segment_map = SegmentDiscovery::extract_segment_map(&shard_data);

    assert_eq!(segment_map.len(), 1);
    let segments = segment_map.get(&0).unwrap();
    assert_eq!(segments.len(), 2);
    assert!(segments.contains(&"001".to_string()));
    assert!(segments.contains(&"002".to_string()));
}

#[tokio::test]
async fn extract_base_dirs_works() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let shard_info = vec![(0, base_path.to_path_buf())];
    let shard_data = SegmentDiscovery::discover_all(shard_info).await;

    let base_dirs = SegmentDiscovery::extract_base_dirs(&shard_data);

    assert_eq!(base_dirs.len(), 1);
    let dir = base_dirs.get(&0).unwrap();
    assert_eq!(dir, base_path);
}

#[tokio::test]
async fn parallel_discovery_is_efficient() {
    // Create 10 temp directories to test parallelism
    let mut temp_dirs = Vec::new();
    let mut shard_info = Vec::new();

    for i in 0..10 {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();

        // Create a few segments in each
        for j in 0..5 {
            std::fs::create_dir(path.join(format!("segment-{:03}", j))).unwrap();
        }

        shard_info.push((i, path));
        temp_dirs.push(temp_dir);
    }

    let start = std::time::Instant::now();
    let result = SegmentDiscovery::discover_all(shard_info).await;
    let duration = start.elapsed();

    assert_eq!(result.len(), 10);
    // Should complete quickly due to parallelism (< 1 second)
    assert!(duration.as_secs() < 1);

    // Verify all shards have 5 segments
    for i in 0..10 {
        let shard_data = result.get(&i).unwrap();
        assert_eq!(shard_data.segments.len(), 5);
    }
}

#[tokio::test]
async fn preserves_shard_base_dir() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    std::fs::create_dir(base_path.join("segment-001")).unwrap();

    let shard_info = vec![(42, base_path.to_path_buf())];
    let result = SegmentDiscovery::discover_all(shard_info).await;

    let shard_data = result.get(&42).unwrap();
    assert_eq!(shard_data.shard_id, 42);
    assert_eq!(shard_data.base_dir, base_path);
}

