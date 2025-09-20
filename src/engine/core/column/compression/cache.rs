use lru::LruCache;

pub type BlockCacheKey = (String, String, String, u32); // (segment_id, uid, field, zone_id)

pub struct BlockCache {
    inner: LruCache<BlockCacheKey, Vec<u8>>,
    max_bytes: usize,
    current_bytes: usize,
}

impl BlockCache {
    pub fn new(capacity_bytes: usize) -> Self {
        Self { inner: LruCache::unbounded(), max_bytes: capacity_bytes, current_bytes: 0 }
    }

    pub fn get(&mut self, key: &BlockCacheKey) -> Option<Vec<u8>> { self.inner.get(key).cloned() }

    pub fn put(&mut self, key: BlockCacheKey, value: Vec<u8>) {
        let size = value.len();
        self.current_bytes += size;
        self.inner.put(key, value);
        while self.current_bytes > self.max_bytes {
            if let Some((_k, v)) = self.inner.pop_lru() { self.current_bytes -= v.len(); } else { break; }
        }
    }
}



