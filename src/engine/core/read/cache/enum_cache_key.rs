use std::path::PathBuf;

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub struct EnumCacheKey {
    pub path: PathBuf,
}

