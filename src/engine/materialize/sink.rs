use crate::engine::core::read::flow::{BatchSchema, ColumnBatch};

use super::catalog::RetentionPolicy;
use super::high_water::HighWaterMark;
use super::store::{MaterializedStore, batch_schema_to_snapshots};
use super::{MaterializationError, SchemaSnapshot};

mod guard;
use guard::SchemaGuard;

pub struct MaterializedSink {
    store: MaterializedStore,
    schema_guard: SchemaGuard,
    high_water: HighWaterMark,
    total_rows: u64,
    total_bytes: u64,
    last_rows_appended: u64,
    last_bytes_appended: u64,
}

impl MaterializedSink {
    pub fn new(
        store: MaterializedStore,
        schema: Vec<SchemaSnapshot>,
    ) -> Result<Self, MaterializationError> {
        if schema.is_empty() {
            return Err(MaterializationError::Corrupt(
                "Materialized sink requires non-empty schema".into(),
            ));
        }

        let guard = SchemaGuard::new(schema);
        guard.expect_store(&store)?;

        let mut sink = Self {
            store,
            schema_guard: guard,
            high_water: HighWaterMark::default(),
            total_rows: 0,
            total_bytes: 0,
            last_rows_appended: 0,
            last_bytes_appended: 0,
        };
        sink.bootstrap_from_manifest();
        Ok(sink)
    }

    pub fn from_batch_schema(
        store: MaterializedStore,
        schema: &BatchSchema,
    ) -> Result<Self, MaterializationError> {
        let snapshots = batch_schema_to_snapshots(schema);
        Self::new(store, snapshots)
    }

    pub fn append(&mut self, batch: &ColumnBatch) -> Result<(), MaterializationError> {
        if batch.is_empty() {
            return Ok(());
        }

        self.schema_guard.expect_batch(batch.schema())?;

        let meta = self
            .store
            .append_batch(self.schema_guard.snapshots(), batch)?;
        self.high_water = meta.high_water_mark;
        let rows_added = meta.row_count as u64;
        let bytes_added = meta.compressed_len as u64;
        self.total_rows = self.total_rows.saturating_add(rows_added);
        self.total_bytes = self.total_bytes.saturating_add(bytes_added);
        self.last_rows_appended = rows_added;
        self.last_bytes_appended = bytes_added;
        Ok(())
    }

    pub fn high_water_mark(&self) -> HighWaterMark {
        self.high_water
    }

    pub fn total_rows(&self) -> u64 {
        self.total_rows
    }

    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    pub fn schema(&self) -> &[SchemaSnapshot] {
        self.schema_guard.snapshots()
    }

    pub fn into_store(self) -> MaterializedStore {
        self.store
    }

    pub fn last_rows_appended(&self) -> u64 {
        self.last_rows_appended
    }

    pub fn last_bytes_appended(&self) -> u64 {
        self.last_bytes_appended
    }

    pub fn set_retention_policy(&mut self, policy: RetentionPolicy) {
        self.store.set_retention_policy(policy);
        self.recompute_totals();
        self.last_rows_appended = 0;
        self.last_bytes_appended = 0;
    }

    fn bootstrap_from_manifest(&mut self) {
        if let Some(last) = self.store.frames().last() {
            self.high_water = last.high_water_mark;
        }

        self.recompute_totals();
        self.last_rows_appended = 0;
        self.last_bytes_appended = 0;
    }

    fn recompute_totals(&mut self) {
        let mut rows = 0u64;
        let mut bytes = 0u64;
        for frame in self.store.frames() {
            if frame.schema_hash == self.schema_guard.schema_hash() {
                rows = rows.saturating_add(frame.row_count as u64);
                bytes = bytes.saturating_add(frame.compressed_len as u64);
            }
        }
        self.total_rows = rows;
        self.total_bytes = bytes;
    }
}
