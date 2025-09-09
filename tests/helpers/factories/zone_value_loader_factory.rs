use crate::engine::core::ZoneValueLoader;
use std::path::PathBuf;

pub struct ZoneValueLoaderFactory {
    segment_base_dir: PathBuf,
    uid: String,
}

impl ZoneValueLoaderFactory {
    pub fn new() -> Self {
        Self {
            segment_base_dir: PathBuf::from("/tmp/segments"),
            uid: "uid_test".to_string(),
        }
    }

    pub fn with_segment_base_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.segment_base_dir = path.into();
        self
    }

    pub fn with_uid(mut self, uid: &str) -> Self {
        self.uid = uid.to_string();
        self
    }

    pub fn create(self) -> ZoneValueLoader {
        ZoneValueLoader::new(self.segment_base_dir, self.uid)
    }
}
