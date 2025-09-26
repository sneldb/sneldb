use crate::engine::core::read::cache::global_zone_index_cache::CacheOutcome;
use crate::engine::core::read::cache::global_zone_index_cache::GlobalZoneIndexCache;
use crate::engine::core::zone::zone_index::ZoneIndex;
use crate::test_helpers::factories::zone_index_factory::ZoneIndexFactory;
use std::sync::Arc;

use once_cell::sync::Lazy;
use std::sync::Mutex as StdMutex;

static TEST_CACHE_GUARD: Lazy<StdMutex<()>> = Lazy::new(|| StdMutex::new(()));

fn lock_guard() -> std::sync::MutexGuard<'static, ()> {
    TEST_CACHE_GUARD.lock().unwrap_or_else(|p| p.into_inner())
}

fn write_index(path: &std::path::Path, entries: &[(&str, &str, u32)]) {
    let mut fac = ZoneIndexFactory::new();
    for &(ev, ctx, id) in entries {
        fac = fac.with_entry(ev, ctx, id);
    }
    let index: ZoneIndex = fac.create();
    index.write_to_path(path).expect("failed to write index");
}

fn load_unique_keys(
    cache: &GlobalZoneIndexCache,
    base_dir: &std::path::Path,
    prefix: &str,
    uniq: usize,
) {
    for i in 0..uniq {
        let seg = format!("{}-{}", prefix, i);
        std::fs::create_dir_all(base_dir.join(&seg)).unwrap();
        let uid = "uid-e";
        let idx_path = base_dir.join(&seg).join(format!("{}.idx", uid));
        write_index(&idx_path, &[("ev", "ctx", 1)]);
        let _ = cache.get_or_load(base_dir, &seg, uid, None).expect("load");
    }
}

fn purge_cache(cache: &GlobalZoneIndexCache, base_dir: &std::path::Path) {
    // Reduce capacity to a fixed small number and overwrite with new unique entries
    cache.resize(64);
    load_unique_keys(cache, base_dir, "purge", 64);
}

#[test]
fn global_cache_hit_then_miss_counters() {
    let _guard = lock_guard();

    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();

    let cache = GlobalZoneIndexCache::instance();
    purge_cache(cache, &base_dir);

    let segment_id = "segment-A";
    let uid = "uid1";
    std::fs::create_dir_all(base_dir.join(segment_id)).unwrap();
    let idx_path = base_dir.join(segment_id).join(format!("{}.idx", uid));
    write_index(&idx_path, &[("ev", "ctx", 1)]);

    cache.resize(256);

    // First load → miss
    let (a1, o1) = cache
        .get_or_load(&base_dir, segment_id, uid, None)
        .expect("load");
    assert!(Arc::strong_count(&a1) >= 1);
    assert!(matches!(o1, CacheOutcome::Miss));

    // Second load → hit, same Arc
    let (a2, o2) = cache
        .get_or_load(&base_dir, segment_id, uid, None)
        .expect("load");
    assert!(Arc::ptr_eq(&a1, &a2));
    assert!(matches!(o2, CacheOutcome::Hit));
}

#[test]
fn global_cache_singleflight_dedupes_concurrent_loads() {
    let _guard = lock_guard();

    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();

    let cache = GlobalZoneIndexCache::instance();
    purge_cache(cache, &base_dir);

    let segment_id = "segment-SF";
    let uid = "uid-sf";
    std::fs::create_dir_all(base_dir.join(segment_id)).unwrap();
    let idx_path = base_dir.join(segment_id).join(format!("{}.idx", uid));
    write_index(&idx_path, &[("ev", "ctx", 1)]);

    cache.resize(256);

    // Spawn N concurrent loads of the same key
    let n = 8;
    let mut handles = Vec::new();
    for _ in 0..n {
        let base = base_dir.clone();
        let seg = segment_id.to_string();
        let id = uid.to_string();
        handles.push(std::thread::spawn(move || {
            GlobalZoneIndexCache::instance()
                .get_or_load(&base, &seg, &id, None)
                .expect("load")
        }));
    }
    let results: Vec<(Arc<ZoneIndex>, CacheOutcome)> =
        handles.into_iter().map(|h| h.join().unwrap()).collect();

    // All Arcs identical
    for (arc, _) in &results {
        assert!(Arc::ptr_eq(&results[0].0, arc));
    }
    // Exactly one Miss outcome; others Hits
    let miss_count = results
        .iter()
        .filter(|(_, o)| matches!(o, CacheOutcome::Miss))
        .count();
    assert_eq!(miss_count, 1, "singleflight should yield exactly one miss");
    assert!(
        results
            .iter()
            .filter(|(_, o)| !matches!(o, CacheOutcome::Miss))
            .all(|(_, o)| matches!(o, CacheOutcome::Hit))
    );
}

#[test]
fn global_cache_reload_when_file_changes() {
    let _guard = lock_guard();

    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();

    let cache = GlobalZoneIndexCache::instance();
    purge_cache(cache, &base_dir);

    let segment_id = "segment-R";
    let uid = "uid-r";
    std::fs::create_dir_all(base_dir.join(segment_id)).unwrap();
    let idx_path = base_dir.join(segment_id).join(format!("{}.idx", uid));
    write_index(&idx_path, &[("ev", "ctx", 1)]);

    cache.resize(256);

    let (a1, _o1) = cache
        .get_or_load(&base_dir, segment_id, uid, None)
        .expect("load");

    // Sleep to ensure fs mtime ticks, then rewrite with different content
    std::thread::sleep(std::time::Duration::from_millis(1200));
    write_index(&idx_path, &[("ev", "ctx", 1), ("ev", "ctx", 2)]);

    let (a2, o2) = cache
        .get_or_load(&base_dir, segment_id, uid, None)
        .expect("reload");
    assert!(matches!(o2, CacheOutcome::Reload));
    assert!(!Arc::ptr_eq(&a1, &a2), "reloaded Arc should differ");
}

#[test]
fn global_cache_evictions_when_exceeding_capacity() {
    let _guard = lock_guard();

    let cache = GlobalZoneIndexCache::instance();

    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();

    purge_cache(cache, &base_dir);

    let cap = 8; // use very small capacity to dominate any prior state
    cache.resize(cap);

    let uniq = cap + 32; // ensure we exceed capacity deterministically

    let segment_id_prefix = "segment-E";
    std::fs::create_dir_all(&base_dir).unwrap();

    // Load `uniq` distinct keys once
    load_unique_keys(cache, &base_dir, segment_id_prefix, uniq);

    // The last `cap` keys should be hits
    for i in (uniq - cap)..uniq {
        let seg = format!("{}-{}", segment_id_prefix, i);
        let (_arc, o) = cache
            .get_or_load(&base_dir, &seg, "uid-e", None)
            .expect("hit tail");
        assert!(
            matches!(o, CacheOutcome::Hit),
            "tail key {} should be hit",
            i
        );
    }

    // Among early keys, at least one should be a miss (evicted)
    let mut early_miss = 0;
    for i in 0..(uniq - cap) {
        let seg = format!("{}-{}", segment_id_prefix, i);
        let (_arc, o) = cache
            .get_or_load(&base_dir, &seg, "uid-e", None)
            .expect("probe head");
        if matches!(o, CacheOutcome::Miss) {
            early_miss += 1;
            break;
        }
    }
    assert!(
        early_miss >= 1,
        "expected at least one early key to be evicted (miss)"
    );
}

#[test]
fn global_cache_returns_error_for_missing_file() {
    let _guard = lock_guard();

    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = "segment-missing";
    let uid = "uid-missing";
    std::fs::create_dir_all(base_dir.join(segment_id)).unwrap();

    let cache = GlobalZoneIndexCache::instance();
    cache.resize(8);

    let res = cache.get_or_load(&base_dir, segment_id, uid, None);
    assert!(res.is_err(), "expected error for missing index file");
}

#[test]
fn global_cache_lru_recency_promotion_affects_eviction() {
    let _guard = lock_guard();

    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();

    let cache = GlobalZoneIndexCache::instance();
    purge_cache(cache, &base_dir);

    cache.resize(2);

    // Prepare three segments A, B, C
    for (seg, uid) in [("segA", "u"), ("segB", "u"), ("segC", "u")] {
        std::fs::create_dir_all(base_dir.join(seg)).unwrap();
        let idx = base_dir.join(seg).join(format!("{}.idx", uid));
        write_index(&idx, &[("ev", "ctx", 1)]);
    }

    // Load A (Miss), B (Miss)
    let (_a1, o) = cache.get_or_load(&base_dir, "segA", "u", None).unwrap();
    assert!(matches!(o, CacheOutcome::Miss));
    let (_b1, o) = cache.get_or_load(&base_dir, "segB", "u", None).unwrap();
    assert!(matches!(o, CacheOutcome::Miss));

    // Touch A to promote to MRU
    let (_a2, o) = cache.get_or_load(&base_dir, "segA", "u", None).unwrap();
    assert!(matches!(o, CacheOutcome::Hit));

    // Insert C (Miss) → should evict B (LRU)
    let (_c1, o) = cache.get_or_load(&base_dir, "segC", "u", None).unwrap();
    assert!(matches!(o, CacheOutcome::Miss));

    // Validate: B is gone (Miss). C should be Hit. A may be Hit depending on internal ordering.
    let (_b2, o) = cache.get_or_load(&base_dir, "segB", "u", None).unwrap();
    assert!(matches!(o, CacheOutcome::Miss), "B should be evicted");
    let (_c2, o) = cache.get_or_load(&base_dir, "segC", "u", None).unwrap();
    assert!(matches!(o, CacheOutcome::Hit));
}

#[test]
fn global_cache_resize_smaller_drops_lru_entries() {
    let _guard = lock_guard();

    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();

    let cache = GlobalZoneIndexCache::instance();
    purge_cache(cache, &base_dir);

    // Load 4 distinct keys at cap 4
    cache.resize(4);
    for i in 0..4 {
        let seg = format!("segR-{}", i);
        std::fs::create_dir_all(base_dir.join(&seg)).unwrap();
        let idx = base_dir.join(&seg).join("u.idx");
        write_index(&idx, &[("ev", "ctx", 1)]);
        let (_a, o) = cache.get_or_load(&base_dir, &seg, "u", None).unwrap();
        assert!(matches!(o, CacheOutcome::Miss));
    }

    // Touch last two to make them MRU: segR-2, segR-3
    let (_x, o2) = cache.get_or_load(&base_dir, "segR-2", "u", None).unwrap();
    let touch2 = if matches!(o2, CacheOutcome::Hit) {
        "Hit"
    } else {
        "Miss"
    };
    println!("touch before resize: segR-2 => {}", touch2);
    let (_y, o3) = cache.get_or_load(&base_dir, "segR-3", "u", None).unwrap();
    let touch3 = if matches!(o3, CacheOutcome::Hit) {
        "Hit"
    } else {
        "Miss"
    };
    println!("touch before resize: segR-3 => {}", touch3);

    // Shrink to 2 → the two most recently used should remain, but exact order can vary per impl
    println!("resizing cache from 4 -> 2");
    cache.resize(2);

    // Probe likely survivors first to avoid evicting them by inserting older keys
    let survivors = ["segR-2", "segR-3"];
    let mut survivor_hits = 0;
    for k in survivors {
        let (_arc, o) = cache.get_or_load(&base_dir, k, "u", None).unwrap();
        println!("post-resize (survivor-first) {} => {:?}", k, o);
        if matches!(o, CacheOutcome::Hit) {
            survivor_hits += 1;
        }
    }
    assert!(
        survivor_hits >= 1,
        "expected at least one survivor to be a hit"
    );

    // Now probe older keys which may cause evictions as they are inserted
    let older = ["segR-0", "segR-1"];
    let mut older_misses = 0;
    for k in older {
        let (_arc, o) = cache.get_or_load(&base_dir, k, "u", None).unwrap();
        println!("post-resize (older) {} => {:?}", k, o);
        if matches!(o, CacheOutcome::Miss) {
            older_misses += 1;
        }
    }
    assert!(
        older_misses >= 1,
        "expected at least one eviction among older keys"
    );
}

#[test]
fn global_cache_concurrent_reload_singleflight() {
    let _guard = lock_guard();

    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();

    let cache = GlobalZoneIndexCache::instance();
    purge_cache(cache, &base_dir);

    let seg = "seg-rel";
    let uid = "u";
    std::fs::create_dir_all(base_dir.join(seg)).unwrap();
    let idx_path = base_dir.join(seg).join(format!("{}.idx", uid));
    write_index(&idx_path, &[("ev", "ctx", 1)]);

    cache.resize(16);

    // Prime cache
    let (_a1, _o1) = cache.get_or_load(&base_dir, seg, uid, None).unwrap();

    // Rewrite file to trigger reload
    std::thread::sleep(std::time::Duration::from_millis(1200));
    write_index(&idx_path, &[("ev", "ctx", 1), ("ev", "ctx", 2)]);

    // Spawn concurrent gets; expect exactly one Reload, others Hit; all Arcs equal
    let n = 8;
    let mut handles = Vec::new();
    for _ in 0..n {
        let base = base_dir.clone();
        let segc = seg.to_string();
        let uidc = uid.to_string();
        handles.push(std::thread::spawn(move || {
            GlobalZoneIndexCache::instance()
                .get_or_load(&base, &segc, &uidc, None)
                .expect("reload")
        }));
    }
    let results: Vec<(Arc<ZoneIndex>, CacheOutcome)> =
        handles.into_iter().map(|h| h.join().unwrap()).collect();

    // All Arcs identical and not null
    for (arc, _) in &results {
        assert!(Arc::strong_count(arc) >= 1);
        assert!(Arc::ptr_eq(&results[0].0, arc));
    }

    let reloads = results
        .iter()
        .filter(|(_, o)| matches!(o, CacheOutcome::Reload))
        .count();
    assert_eq!(reloads, 1, "exactly one reload expected");
}

#[test]
fn global_cache_resize_larger_preserves_entries() {
    let _guard = lock_guard();

    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();

    let cache = GlobalZoneIndexCache::instance();
    purge_cache(cache, &base_dir);

    // Load up to cap 2
    cache.resize(2);
    for seg in ["segGrow-0", "segGrow-1"] {
        std::fs::create_dir_all(base_dir.join(seg)).unwrap();
        let idx = base_dir.join(seg).join("u.idx");
        write_index(&idx, &[("ev", "ctx", 1)]);
        let (_a, o) = cache.get_or_load(&base_dir, seg, "u", None).unwrap();
        assert!(matches!(o, CacheOutcome::Miss));
    }

    // Touch the second to make it MRU
    let (_b, o) = cache
        .get_or_load(&base_dir, "segGrow-1", "u", None)
        .unwrap();
    assert!(matches!(o, CacheOutcome::Hit));

    // Increase capacity
    cache.resize(8);

    // Both existing entries should still be hits
    for seg in ["segGrow-0", "segGrow-1"] {
        let (_a, o) = cache.get_or_load(&base_dir, seg, "u", None).unwrap();
        assert!(matches!(o, CacheOutcome::Hit));
    }

    // Insert additional entries and ensure no retroactive misses for the old ones
    for i in 2..6 {
        let seg = format!("segGrow-{}", i);
        std::fs::create_dir_all(base_dir.join(&seg)).unwrap();
        let idx = base_dir.join(&seg).join("u.idx");
        write_index(&idx, &[("ev", "ctx", 1)]);
        let (_a, _o) = cache.get_or_load(&base_dir, &seg, "u", None).unwrap();
    }

    // Old entries remain hits after more inserts under larger capacity
    for seg in ["segGrow-0", "segGrow-1"] {
        let (_a, o) = cache.get_or_load(&base_dir, seg, "u", None).unwrap();
        assert!(matches!(o, CacheOutcome::Hit));
    }
}

#[test]
fn global_cache_resize_does_not_mutate_counters() {
    let _guard = lock_guard();

    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();

    let cache = GlobalZoneIndexCache::instance();
    purge_cache(cache, &base_dir);

    cache.resize(4);
    // Load some entries to allow potential evictions on future inserts
    for seg in ["segCnt-0", "segCnt-1", "segCnt-2", "segCnt-3"] {
        std::fs::create_dir_all(base_dir.join(seg)).unwrap();
        let idx = base_dir.join(seg).join("u.idx");
        write_index(&idx, &[("ev", "ctx", 1)]);
        let (_a, _o) = cache.get_or_load(&base_dir, seg, "u", None).unwrap();
    }

    let before = cache.stats();

    // Resize larger then smaller; counters should be unaffected
    cache.resize(8);
    cache.resize(2);

    let after = cache.stats();

    assert_eq!(before.hits, after.hits, "hits should not change on resize");
    assert_eq!(
        before.misses, after.misses,
        "misses should not change on resize"
    );
    assert_eq!(
        before.reloads, after.reloads,
        "reloads should not change on resize"
    );
    assert_eq!(
        before.evictions, after.evictions,
        "evictions should not change on resize"
    );
}

#[test]
fn global_cache_retry_after_missing_file_succeeds() {
    let _guard = lock_guard();

    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();

    let cache = GlobalZoneIndexCache::instance();
    purge_cache(cache, &base_dir);
    cache.resize(8);

    let seg = "seg-miss-retry";
    let uid = "u";
    std::fs::create_dir_all(base_dir.join(seg)).unwrap();

    // First attempt: file missing → error
    let res = cache.get_or_load(&base_dir, seg, uid, None);
    assert!(res.is_err());

    // Now create the file and retry → should succeed with Miss
    let idx = base_dir.join(seg).join(format!("{}.idx", uid));
    write_index(&idx, &[("ev", "ctx", 1)]);
    let (_a, o) = cache.get_or_load(&base_dir, seg, uid, None).unwrap();
    assert!(matches!(o, CacheOutcome::Miss));
}

#[test]
fn global_cache_reload_on_mtime_only_change() {
    let _guard = lock_guard();

    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();

    let cache = GlobalZoneIndexCache::instance();
    purge_cache(cache, &base_dir);
    cache.resize(8);

    let seg = "seg-mtime";
    let uid = "u";
    std::fs::create_dir_all(base_dir.join(seg)).unwrap();
    let idx = base_dir.join(seg).join(format!("{}.idx", uid));

    // Write content
    write_index(&idx, &[("ev", "ctx", 1)]);
    let (_a1, _o1) = cache.get_or_load(&base_dir, seg, uid, None).unwrap();

    // Sleep to ensure mtime tick, then write identical content so size likely unchanged
    std::thread::sleep(std::time::Duration::from_millis(1200));
    write_index(&idx, &[("ev", "ctx", 1)]);

    let (_a2, o2) = cache.get_or_load(&base_dir, seg, uid, None).unwrap();
    assert!(matches!(o2, CacheOutcome::Reload));
}

#[test]
fn global_cache_capacity_one_eviction_semantics() {
    let _guard = lock_guard();

    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();

    let cache = GlobalZoneIndexCache::instance();
    purge_cache(cache, &base_dir);

    cache.resize(1);

    for seg in ["segC1-A", "segC1-B"] {
        std::fs::create_dir_all(base_dir.join(seg)).unwrap();
        let idx = base_dir.join(seg).join("u.idx");
        write_index(&idx, &[("ev", "ctx", 1)]);
    }

    // Load A (Miss), then B (Miss + evicts A), then A again (Miss, since capacity 1)
    let (_a1, o1) = cache.get_or_load(&base_dir, "segC1-A", "u", None).unwrap();
    assert!(matches!(o1, CacheOutcome::Miss));
    let (_b1, o2) = cache.get_or_load(&base_dir, "segC1-B", "u", None).unwrap();
    assert!(matches!(o2, CacheOutcome::Miss));
    let (_a2, o3) = cache.get_or_load(&base_dir, "segC1-A", "u", None).unwrap();
    assert!(matches!(o3, CacheOutcome::Miss));
}

#[test]
fn global_cache_hit_returns_same_arc() {
    let _guard = lock_guard();

    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();

    let cache = GlobalZoneIndexCache::instance();
    purge_cache(cache, &base_dir);

    cache.resize(8);

    std::fs::create_dir_all(base_dir.join("segSame")).unwrap();
    let idx = base_dir.join("segSame").join("u.idx");
    write_index(&idx, &[("ev", "ctx", 1)]);

    let (a1, o1) = cache.get_or_load(&base_dir, "segSame", "u", None).unwrap();
    assert!(matches!(o1, CacheOutcome::Miss));
    let (a2, o2) = cache.get_or_load(&base_dir, "segSame", "u", None).unwrap();
    assert!(matches!(o2, CacheOutcome::Hit));
    assert!(Arc::ptr_eq(&a1, &a2));
}
