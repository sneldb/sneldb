#[derive(Debug, Clone, Copy)]
pub struct ColumnBlockCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub reloads: u64,
    pub evictions: u64,
    pub current_bytes: usize,
    pub capacity_bytes: usize,
}
