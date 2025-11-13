use super::group_key::GroupKey;
use super::group_key_cache::GroupKeyCache;

fn make_group_key(prehash: u64, bucket: Option<u64>, groups: Vec<String>) -> GroupKey {
    GroupKey {
        prehash,
        bucket,
        groups: groups
            .iter()
            .map(|s| {
                if let Ok(i) = s.parse::<i64>() {
                    super::group_key::GroupValue::Int(i)
                } else {
                    super::group_key::GroupValue::Str(s.clone())
                }
            })
            .collect(),
        groups_str: Some(groups),
    }
}

#[test]
fn group_key_cache_new_creates_empty_cache() {
    let cache = GroupKeyCache::new(10);
    // Cache is private, but we can test behavior through get_or_insert
    let mut cache = cache;
    let key = make_group_key(1, None, vec!["test".to_string()]);
    let result = cache.get_or_insert(1, || key.clone());
    assert_eq!(result.prehash, 1);
}

#[test]
fn group_key_cache_get_or_insert_caches_key() {
    let mut cache = GroupKeyCache::new(10);
    let key1 = make_group_key(100, None, vec!["US".to_string()]);

    // First insert should compute the key
    let mut compute_count = 0;
    let result1 = cache.get_or_insert(100, || {
        compute_count += 1;
        key1.clone()
    });
    assert_eq!(compute_count, 1);
    let mut result1_mut = result1;
    assert_eq!(result1_mut.groups_str(), &vec!["US".to_string()]);

    // Second call with same prehash should return cached value
    let result2 = cache.get_or_insert(100, || {
        compute_count += 1;
        make_group_key(100, None, vec!["DE".to_string()])
    });
    assert_eq!(compute_count, 1); // Should not recompute
    let mut result2_mut = result2;
    assert_eq!(result2_mut.groups_str(), &vec!["US".to_string()]); // Should return cached value
}

#[test]
fn group_key_cache_different_prehashes_are_cached_separately() {
    let mut cache = GroupKeyCache::new(10);
    let key1 = make_group_key(100, None, vec!["US".to_string()]);
    let key2 = make_group_key(200, Some(1000), vec!["DE".to_string()]);

    let result1 = cache.get_or_insert(100, || key1.clone());
    let result2 = cache.get_or_insert(200, || key2.clone());

    let mut result1_mut = result1;
    let mut result2_mut = result2.clone();
    assert_eq!(result1_mut.groups_str(), &vec!["US".to_string()]);
    assert_eq!(result2_mut.groups_str(), &vec!["DE".to_string()]);
    assert_eq!(result2.bucket, Some(1000));
}

#[test]
fn group_key_cache_evicts_when_full() {
    let mut cache = GroupKeyCache::new(2);
    let key1 = make_group_key(100, None, vec!["US".to_string()]);
    let key2 = make_group_key(200, None, vec!["DE".to_string()]);
    let key3 = make_group_key(300, None, vec!["FR".to_string()]);

    // Fill cache to capacity
    cache.get_or_insert(100, || key1.clone());
    cache.get_or_insert(200, || key2.clone());

    // This should trigger eviction and clear the cache
    let mut compute_count = 0;
    cache.get_or_insert(300, || {
        compute_count += 1;
        key3.clone()
    });
    assert_eq!(compute_count, 1);

    // After eviction, previous keys should not be cached
    let mut compute_count2 = 0;
    cache.get_or_insert(100, || {
        compute_count2 += 1;
        make_group_key(100, None, vec!["US_NEW".to_string()])
    });
    assert_eq!(compute_count2, 1); // Should recompute since cache was cleared
}

#[test]
fn group_key_cache_with_bucket_values() {
    let mut cache = GroupKeyCache::new(10);
    let key = make_group_key(500, Some(86400), vec!["country".to_string(), "region".to_string()]);

    let result = cache.get_or_insert(500, || key.clone());
    assert_eq!(result.bucket, Some(86400));
    let mut result_mut = result;
    assert_eq!(result_mut.groups_str().len(), 2);
}

#[test]
fn group_key_cache_max_size_zero_always_evicts() {
    let mut cache = GroupKeyCache::new(0);
    let key = make_group_key(100, None, vec!["test".to_string()]);

    // Should always recompute since cache size is 0
    let mut compute_count = 0;
    cache.get_or_insert(100, || {
        compute_count += 1;
        key.clone()
    });
    assert_eq!(compute_count, 1);

    // Should recompute again
    cache.get_or_insert(100, || {
        compute_count += 1;
        key.clone()
    });
    assert_eq!(compute_count, 2);
}

#[test]
fn group_key_cache_large_capacity() {
    let mut cache = GroupKeyCache::new(1000);

    // Insert many keys
    for i in 0..500 {
        let key = make_group_key(i, None, vec![format!("key_{}", i)]);
        cache.get_or_insert(i, || key.clone());
    }

    // Verify they're all cached
    for i in 0..500 {
        let mut compute_count = 0;
        let result = cache.get_or_insert(i, || {
            compute_count += 1;
            make_group_key(i, None, vec![format!("key_{}_new", i)])
        });
        assert_eq!(compute_count, 0, "Key {} should be cached", i);
        let mut result_mut = result;
        assert_eq!(result_mut.groups_str()[0], format!("key_{}", i));
    }
}

