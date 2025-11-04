use std::path::Path;
use std::sync::Arc;

use crate::engine::core::read::flow::ColumnBatch;
use crate::engine::materialize::MaterializationError;
use crate::engine::materialize::catalog::{
    MaterializationTelemetry, RetentionPolicy, SchemaSnapshot,
};

use crate::engine::core::read::cache::GlobalMaterializedFrameCache;

use super::codec::{BatchCodec, Lz4BatchCodec, schema_hash};
use super::frame::metadata::StoredFrameMeta;
use super::frame::storage::FrameStorage;
use super::manifest::{ManifestState, ManifestStore};
use super::retention::RetentionEnforcer;
use super::telemetry::TelemetryTracker;

pub struct MaterializedStore {
    frame_storage: FrameStorage,
    manifest_store: ManifestStore,
    manifest: ManifestState,
    codec: Box<dyn BatchCodec + Send + Sync>,
}

impl MaterializedStore {
    pub fn open(base_dir: impl AsRef<Path>) -> Result<Self, MaterializationError> {
        let root_dir = base_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&root_dir)?;

        let frame_dir_path = root_dir.join("frames");
        let frame_storage = FrameStorage::create(&frame_dir_path)?;

        let manifest_path = root_dir.join("manifest.bin");
        let (manifest_store, manifest_state) = ManifestStore::open(manifest_path)?;

        Ok(Self {
            frame_storage,
            manifest_store,
            manifest: manifest_state,
            codec: Box::new(Lz4BatchCodec::default()),
        })
    }

    pub fn with_codec(mut self, codec: Box<dyn BatchCodec + Send + Sync>) -> Self {
        self.codec = codec;
        self
    }

    pub fn frames(&self) -> &[StoredFrameMeta] {
        self.manifest.frames()
    }

    pub fn append_batch(
        &mut self,
        schema: &[SchemaSnapshot],
        batch: &ColumnBatch,
    ) -> Result<StoredFrameMeta, MaterializationError> {
        if schema.is_empty() {
            return Err(MaterializationError::Corrupt(
                "Materialized store requires non-empty schema".into(),
            ));
        }

        let hash = schema_hash(schema);
        if let Some(existing) = self.manifest.frames().last() {
            if existing.schema_hash != hash {
                return Err(MaterializationError::Corrupt(
                    "Schema hash mismatch across materialized frames".into(),
                ));
            }
        }

        let encoded = self.codec.encode(schema, batch)?;
        let index = self.manifest.next_frame_index();
        let writer = self.frame_storage.writer();
        let meta = writer.write(index, &encoded)?;

        self.manifest.bump_frame_index();
        self.manifest.push_frame(meta.clone());

        let retention_policy = self.manifest.retention_policy().cloned();
        let enforcer = RetentionEnforcer::new(&self.frame_storage);
        enforcer.apply(retention_policy.as_ref(), &mut self.manifest)?;

        self.persist_manifest()?;

        Ok(meta)
    }

    pub fn read_frame(
        &self,
        meta: &StoredFrameMeta,
    ) -> Result<Arc<ColumnBatch>, MaterializationError> {
        let cache = GlobalMaterializedFrameCache::instance();
        let frame_dir = self.frame_dir();

        let (batch_arc, _outcome) = cache.get_or_load(frame_dir, meta, || {
            // Loader closure: read and decode the frame
            let reader = self.frame_storage.reader();
            let frame_data = reader.read(meta)?;
            self.codec.decode(meta, frame_data)
        })?;

        // Return Arc directly - cache keeps a reference, so callers need to clone when unwrapping
        // But at least we avoid decompression on cache hits!
        Ok(batch_arc)
    }

    pub fn frame_dir(&self) -> &Path {
        self.frame_storage.path()
    }

    pub fn telemetry(&self) -> MaterializationTelemetry {
        TelemetryTracker::summarize(&self.manifest).into_catalog()
    }

    pub fn retention_policy(&self) -> Option<&RetentionPolicy> {
        self.manifest.retention_policy()
    }

    pub fn set_retention_policy(&mut self, policy: RetentionPolicy) {
        self.manifest.set_retention_policy(policy);
        let _ = self.persist_manifest();
    }

    pub fn clear_retention_policy(&mut self) {
        self.manifest.clear_retention_policy();
        let _ = self.persist_manifest();
    }

    fn persist_manifest(&self) -> Result<(), MaterializationError> {
        self.manifest_store.persist(&self.manifest)
    }
}
