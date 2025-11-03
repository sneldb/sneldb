use crate::engine::materialize::MaterializationError;
use crate::engine::materialize::catalog::RetentionPolicy;

use super::frame::storage::FrameStorage;
use super::manifest::ManifestState;
use crate::shared::time::now;

pub struct RetentionEnforcer<'a> {
    storage: &'a FrameStorage,
}

impl<'a> RetentionEnforcer<'a> {
    pub fn new(storage: &'a FrameStorage) -> Self {
        Self { storage }
    }

    pub fn apply(
        &self,
        policy: Option<&RetentionPolicy>,
        state: &mut ManifestState,
    ) -> Result<(), MaterializationError> {
        let Some(policy) = policy else {
            return Ok(());
        };

        if policy.is_noop() {
            return Ok(());
        }

        let mut drop_indices: Vec<usize> = Vec::new();

        if let Some(max_rows) = policy.max_rows {
            let mut running_rows: u64 = 0;
            for (idx, frame) in state.frames().iter().enumerate().rev() {
                running_rows = running_rows.saturating_add(frame.row_count as u64);
                if running_rows > max_rows {
                    drop_indices.push(idx);
                }
            }
        }

        if let Some(max_age) = policy.max_age_seconds {
            let cutoff = now().saturating_sub(max_age);
            for (idx, frame) in state.frames().iter().enumerate() {
                if frame.high_water_mark.timestamp < cutoff {
                    drop_indices.push(idx);
                }
            }
        }

        drop_indices.sort_unstable();
        drop_indices.dedup();

        if drop_indices.is_empty() {
            return Ok(());
        }

        for &idx in drop_indices.iter().rev() {
            if let Some(frame) = state.frames().get(idx) {
                self.storage.remove(&frame.file_name);
            }
        }

        state.remove_indices(&drop_indices);
        Ok(())
    }
}
