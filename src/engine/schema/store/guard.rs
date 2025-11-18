use crate::engine::schema::errors::SchemaError;
use fs2::FileExt;
use std::cell::UnsafeCell;
use std::fs::File;
use tracing::warn;

/// RAII guard for file locking that automatically unlocks on drop.
pub struct FileLockGuard {
    file: UnsafeCell<File>,
}

impl FileLockGuard {
    pub fn new_shared(file: File) -> Result<Self, SchemaError> {
        file.try_lock_shared()
            .map_err(|e| SchemaError::IoReadFailed(format!("lock failed: {}", e)))?;
        Ok(Self {
            file: UnsafeCell::new(file),
        })
    }

    pub fn new_exclusive(file: File) -> Result<Self, SchemaError> {
        file.try_lock_exclusive()
            .map_err(|e| SchemaError::IoWriteFailed(format!("lock failed: {}", e)))?;
        Ok(Self {
            file: UnsafeCell::new(file),
        })
    }

    pub fn get_mut(&self) -> &mut File {
        unsafe { &mut *self.file.get() }
    }
}

impl Drop for FileLockGuard {
    fn drop(&mut self) {
        let file_ref = unsafe { &*self.file.get() };
        if let Err(e) = file_ref.unlock() {
            warn!("Failed to unlock schema store file: {}", e);
        }
    }
}
