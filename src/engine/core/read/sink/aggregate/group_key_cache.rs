use super::group_key::GroupKey;
use std::collections::HashMap;

/// Cache for group keys to avoid recomputing identical keys
/// Uses a simple HashMap with size-based eviction
pub(crate) struct GroupKeyCache {
    cache: HashMap<u64, GroupKey>, // keyed by prehash
    max_size: usize,
}

impl GroupKeyCache {
    pub(crate) fn new(max_size: usize) -> Self {
        Self {
            cache: HashMap::with_capacity(max_size),
            max_size,
        }
    }

    pub(crate) fn get_or_insert<F>(&mut self, prehash: u64, f: F) -> GroupKey
    where
        F: FnOnce() -> GroupKey,
    {
        // If max_size is 0, never cache - always recompute
        if self.max_size == 0 {
            return f();
        }

        if let Some(key) = self.cache.get(&prehash) {
            key.clone()
        } else {
            let key = f();
            // Simple eviction: if cache is full, clear it
            if self.cache.len() >= self.max_size {
                self.cache.clear();
            }
            self.cache.insert(prehash, key.clone());
            key
        }
    }
}

