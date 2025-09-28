use std::path::PathBuf;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ColumnBlockCacheKey {
    pub path: PathBuf,
    pub zone_id: u32,
}

impl ColumnBlockCacheKey {
    pub fn new(path: PathBuf, zone_id: u32) -> Self {
        Self { path, zone_id }
    }
}
