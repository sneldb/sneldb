use super::verifier::SegmentVerifier;
use crate::test_helpers::factories::SchemaRegistryFactory;
use std::fs;
use tempfile::tempdir;

#[tokio::test]
async fn test_verify_returns_false_for_nonexistent_segment() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let verifier = SegmentVerifier::new(base_dir, 0);

    let result = verifier.verify(999).await;
    assert!(!result, "Verification should fail for non-existent segment");
}

#[tokio::test]
async fn test_verify_returns_false_when_directory_exists_but_no_index() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = 1;

    // Create segment directory but no index file
    let segment_dir = base_dir.join(format!("{:05}", segment_id));
    fs::create_dir_all(&segment_dir).unwrap();

    let verifier = SegmentVerifier::new(base_dir, 0);
    let result = verifier.verify(segment_id).await;

    assert!(
        !result,
        "Verification should fail when index file is missing"
    );
}

#[tokio::test]
async fn test_verify_succeeds_for_empty_segment() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = 2;

    let segment_dir = base_dir.join(format!("{:05}", segment_id));
    fs::create_dir_all(&segment_dir).unwrap();

    // Create shard-level segment index file
    let index_path = base_dir.join("segments.idx");
    fs::write(&index_path, b"").unwrap();

    let verifier = SegmentVerifier::new(base_dir, 0);
    let result = verifier.verify(segment_id).await;

    assert!(
        result,
        "Empty segment with valid index should verify successfully"
    );
}

#[tokio::test]
async fn test_verify_succeeds_for_valid_segment_with_data() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = 3;

    let segment_dir = base_dir.join(format!("{:05}", segment_id));
    fs::create_dir_all(&segment_dir).unwrap();

    // Setup schema and create a valid segment
    let schema_factory = SchemaRegistryFactory::new();
    schema_factory
        .define_with_fields("test_event", &[("field1", "string")])
        .await
        .unwrap();

    let registry = schema_factory.registry();
    let uid = registry.read().await.get_uid("test_event").unwrap();

    // Create zone directory
    let zone_dir = segment_dir.join(&uid);
    fs::create_dir_all(&zone_dir).unwrap();

    // Create a dummy zone file
    let zone_file = zone_dir.join("zone_0.dat");
    fs::write(&zone_file, b"dummy data").unwrap();

    // Create shard-level segment index file
    let index_path = base_dir.join("segments.idx");
    fs::write(&index_path, b"valid index data").unwrap();

    let verifier = SegmentVerifier::new(base_dir, 0);
    let result = verifier.verify(segment_id).await;

    assert!(result, "Valid segment with data should verify successfully");
}

#[tokio::test]
async fn test_verify_with_retry_succeeds_on_first_attempt() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = 4;

    let segment_dir = base_dir.join(format!("{:05}", segment_id));
    fs::create_dir_all(&segment_dir).unwrap();

    // Create shard-level segment index file
    let index_path = base_dir.join("segments.idx");
    fs::write(&index_path, b"").unwrap();

    let verifier = SegmentVerifier::new(base_dir, 0);
    let result = verifier.verify_with_retry(segment_id, 3, 10).await;

    assert!(result, "Should succeed on first attempt");
}

#[tokio::test]
async fn test_verify_with_retry_fails_after_max_attempts() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = 999; // Non-existent

    let verifier = SegmentVerifier::new(base_dir, 0);

    let start = tokio::time::Instant::now();
    let result = verifier.verify_with_retry(segment_id, 3, 50).await;
    let elapsed = start.elapsed();

    assert!(!result, "Should fail after all attempts");
    assert!(
        elapsed.as_millis() >= 100,
        "Should have retried with delays (expected ~100ms, got {}ms)",
        elapsed.as_millis()
    );
}

#[tokio::test]
async fn test_verify_with_retry_succeeds_on_second_attempt() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = 5;

    let segment_dir = base_dir.join(format!("{:05}", segment_id));

    let verifier = SegmentVerifier::new(base_dir.clone(), 0);

    // Spawn verification in background
    let verify_handle =
        tokio::spawn(async move { verifier.verify_with_retry(segment_id, 5, 50).await });

    // Wait a bit, then create the segment and shard index
    tokio::time::sleep(tokio::time::Duration::from_millis(60)).await;

    fs::create_dir_all(&segment_dir).unwrap();
    let index_path = base_dir.join("segments.idx");
    fs::write(&index_path, b"").unwrap();

    let result = verify_handle.await.unwrap();
    assert!(result, "Should succeed on retry after segment is created");
}

#[tokio::test]
async fn test_verify_succeeds_when_index_file_exists() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = 6;

    let segment_dir = base_dir.join(format!("{:05}", segment_id));
    fs::create_dir_all(&segment_dir).unwrap();

    // Create shard-level index file (verifier only checks existence, not contents)
    let index_path = base_dir.join("segments.idx");
    fs::write(&index_path, b"any data").unwrap();

    let verifier = SegmentVerifier::new(base_dir, 0);
    let result = verifier.verify(segment_id).await;

    assert!(result, "Verification should succeed when index file exists");
}

#[tokio::test]
async fn test_verify_multiple_segments_independently() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let verifier = SegmentVerifier::new(base_dir.clone(), 0);

    // Create segment 1 directory
    let seg1_dir = base_dir.join("00001");
    fs::create_dir_all(&seg1_dir).unwrap();

    // Don't create segment 2

    // Create segment 3 directory
    let seg3_dir = base_dir.join("00003");
    fs::create_dir_all(&seg3_dir).unwrap();

    // Create shard-level segment index (shared by all segments)
    let index_path = base_dir.join("segments.idx");
    fs::write(&index_path, b"").unwrap();

    assert!(verifier.verify(1).await, "Segment 1 should verify");
    assert!(!verifier.verify(2).await, "Segment 2 should not verify");
    assert!(verifier.verify(3).await, "Segment 3 should verify");
}

#[tokio::test]
async fn test_concurrent_verification_of_same_segment() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = 7;

    let segment_dir = base_dir.join(format!("{:05}", segment_id));
    fs::create_dir_all(&segment_dir).unwrap();
    let index_path = base_dir.join("segments.idx");
    fs::write(&index_path, b"").unwrap();

    let mut handles = vec![];

    for _ in 0..10 {
        let base_dir_clone = base_dir.clone();
        let handle = tokio::spawn(async move {
            let verifier = SegmentVerifier::new(base_dir_clone, 0);
            verifier.verify(segment_id).await
        });
        handles.push(handle);
    }

    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result, "Concurrent verification should succeed");
    }
}

#[tokio::test]
async fn test_verify_with_zero_retries_attempts_once() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = 999;

    let verifier = SegmentVerifier::new(base_dir, 0);

    let start = tokio::time::Instant::now();
    let result = verifier.verify_with_retry(segment_id, 1, 50).await;
    let elapsed = start.elapsed();

    assert!(!result, "Should fail");
    assert!(
        elapsed.as_millis() < 50,
        "Should not retry with max_attempts=1"
    );
}

#[tokio::test]
async fn test_verify_handles_permission_denied_gracefully() {
    // This test might not work on all systems, so we'll skip if permissions can't be set
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = 8;

    let segment_dir = base_dir.join(format!("{:05}", segment_id));
    fs::create_dir_all(&segment_dir).unwrap();

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        // Remove read permissions
        let mut perms = fs::metadata(&segment_dir).unwrap().permissions();
        perms.set_mode(0o000);
        let _ = fs::set_permissions(&segment_dir, perms);

        let verifier = SegmentVerifier::new(base_dir.clone(), 0);
        let result = verifier.verify(segment_id).await;

        // Restore permissions for cleanup
        let mut perms = fs::metadata(&segment_dir).unwrap().permissions();
        perms.set_mode(0o755);
        let _ = fs::set_permissions(&segment_dir, perms);

        assert!(
            !result,
            "Verification should fail gracefully for permission denied"
        );
    }

    #[cfg(not(unix))]
    {
        // On non-Unix systems, just pass the test
        let verifier = SegmentVerifier::new(base_dir, 0);
        let _ = verifier.verify(segment_id).await;
    }
}
