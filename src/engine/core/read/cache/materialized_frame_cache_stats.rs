#[derive(Debug, Clone, Copy)]
pub struct MaterializedFrameCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub current_bytes: usize,
    pub capacity_bytes: usize,
    pub current_items: usize,
}
