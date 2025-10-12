use std::path::{Path, PathBuf};

/// Returns an absolute version of the provided path without touching the filesystem.
/// Falls back to the original path if the current working directory cannot be resolved.
pub fn absolutize<P: AsRef<Path>>(path: P) -> PathBuf {
    let path = path.as_ref();
    if path.is_absolute() {
        return path.to_path_buf();
    }

    std::env::current_dir()
        .map(|cwd| cwd.join(path))
        .unwrap_or_else(|_| path.to_path_buf())
}
