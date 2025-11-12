use crate::engine::core::read::cache::{
    CacheOutcome, ColumnBlockCacheStats, GlobalColumnBlockCache,
};
use once_cell::sync::Lazy;
use std::sync::Mutex as StdMutex;

static TEST_CACHE_GUARD: Lazy<StdMutex<()>> = Lazy::new(|| StdMutex::new(()));

fn lock_guard() -> std::sync::MutexGuard<'static, ()> {
    TEST_CACHE_GUARD.lock().unwrap_or_else(|p| p.into_inner())
}

#[test]
fn block_cache_hit_then_miss_counters_and_eviction() {
    let _guard = lock_guard();

    let cache = GlobalColumnBlockCache::instance();

    // Reset capacity small to force evictions.
    cache.resize_bytes(1024); // 1 KiB

    // Loader that produces buffers of specific sizes
    let make_loader =
        |size: usize| move || -> Result<Vec<u8>, std::io::Error> { Ok(vec![0u8; size]) };

    // Use a stable fake path; it need not exist since we only use it as a key.
    let fake_path = std::path::Path::new("/tmp/fake.col");

    // Insert two blocks that fit
    let (_b1, _o1) = cache
        .get_or_load(fake_path, 1, make_loader(400))
        .expect("load");
    let (_b2, _o2) = cache
        .get_or_load(fake_path, 2, make_loader(500))
        .expect("load");

    // Third block triggers eviction
    let (_b3, _o3) = cache
        .get_or_load(fake_path, 3, make_loader(600))
        .expect("load");

    let stats: ColumnBlockCacheStats = cache.stats();
    assert!(stats.evictions >= 1, "expected at least one eviction");

    // Access LRU-likely survivor (the last inserted) should be a hit
    let (_b3_again, o3_hit) = cache
        .get_or_load(fake_path, 3, make_loader(600))
        .expect("hit");
    assert!(matches!(o3_hit, CacheOutcome::Hit));
}

#[test]
fn block_cache_singleflight() {
    let _guard = lock_guard();
    let cache = GlobalColumnBlockCache::instance();
    cache.resize_bytes(10 * 1024);

    let path = std::path::Path::new("/tmp/sf.col");

    // Count loader invocations using an Arc counter
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let counter = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for _ in 0..8 {
        let pathc = path.to_path_buf();
        let c = Arc::clone(&counter);
        handles.push(std::thread::spawn(move || {
            GlobalColumnBlockCache::instance()
                .get_or_load(&pathc, 42, move || {
                    c.fetch_add(1, Ordering::SeqCst);
                    std::thread::sleep(std::time::Duration::from_millis(50));
                    Ok(vec![1u8; 256])
                })
                .expect("load")
        }));
    }
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

    // All sizes equal
    for (blk, _o) in &results {
        assert_eq!(blk.size, 256);
    }

    // Only one loader invocation
    assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 1);
}
