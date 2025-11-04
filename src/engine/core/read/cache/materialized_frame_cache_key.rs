use std::path::PathBuf;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct MaterializedFrameCacheKey {
    frame_path: PathBuf,
}

impl MaterializedFrameCacheKey {
    pub fn new(frame_path: PathBuf) -> Self {
        Self { frame_path }
    }

    pub fn from_file_name(frame_dir: &std::path::Path, file_name: &str) -> Self {
        Self {
            frame_path: frame_dir.join(file_name),
        }
    }

    pub fn path(&self) -> &PathBuf {
        &self.frame_path
    }
}
