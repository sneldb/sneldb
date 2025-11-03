use crate::engine::core::read::flow::BatchSchema;
use crate::engine::materialize::store::{MaterializedStore, schema_hash};
use crate::engine::materialize::{MaterializationError, SchemaSnapshot};

pub struct SchemaGuard {
    snapshots: Vec<SchemaSnapshot>,
    schema_hash: u64,
}

impl SchemaGuard {
    pub fn new(snapshots: Vec<SchemaSnapshot>) -> Self {
        let schema_hash = schema_hash(&snapshots);
        Self {
            snapshots,
            schema_hash,
        }
    }

    pub fn expect_store(&self, store: &MaterializedStore) -> Result<(), MaterializationError> {
        if let Some(existing) = store.frames().first() {
            if existing.schema_hash != self.schema_hash {
                return Err(MaterializationError::Corrupt(
                    "Existing materialized store schema mismatch".into(),
                ));
            }
        }
        Ok(())
    }

    pub fn expect_batch(&self, schema: &BatchSchema) -> Result<(), MaterializationError> {
        let columns = schema.columns();
        if columns.len() != self.snapshots.len() {
            return Err(MaterializationError::Corrupt(
                "Batch schema column count mismatch".into(),
            ));
        }

        for (col, snapshot) in columns.iter().zip(self.snapshots.iter()) {
            if col.name != snapshot.name || col.logical_type != snapshot.logical_type {
                return Err(MaterializationError::Corrupt(format!(
                    "Batch schema column mismatch: expected {}:{}, found {}:{}",
                    snapshot.name, snapshot.logical_type, col.name, col.logical_type
                )));
            }
        }
        Ok(())
    }

    pub fn snapshots(&self) -> &[SchemaSnapshot] {
        &self.snapshots
    }

    pub fn schema_hash(&self) -> u64 {
        self.schema_hash
    }
}
