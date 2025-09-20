// Map alias removed; no longer used after refactor
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

use crate::engine::core::column::compression::compressed_column_index::CompressedColumnIndex;
use crate::engine::core::column::compression::compression_codec::Lz4Codec;
use crate::engine::core::write::column_block_writer::ColumnBlockWriter;
use crate::engine::core::write::column_group_builder::ColumnGroupBuilder;
use crate::engine::core::write::column_paths::ColumnPathResolver;
use crate::engine::core::{UidResolver, WriteJob, ZonePlan};
use crate::engine::errors::StoreError;
use crate::engine::schema::SchemaRegistry;
pub struct ColumnWriter {
    pub segment_dir: PathBuf,
    pub registry: Arc<RwLock<SchemaRegistry>>,
}

impl ColumnWriter {
    pub fn new(segment_dir: PathBuf, registry: Arc<RwLock<SchemaRegistry>>) -> Self {
        Self {
            segment_dir,
            registry,
        }
    }

    pub async fn write_all(&self, zone_plans: &[ZonePlan]) -> Result<(), StoreError> {
        let resolver = UidResolver::from_events(zone_plans, &self.registry).await?;
        let write_jobs = WriteJob::build(zone_plans, &self.segment_dir, &resolver);
        let segment_dir = self.segment_dir.clone();

        tokio::task::spawn_blocking(move || -> Result<(), StoreError> {
            let mut builder = ColumnGroupBuilder::new();
            for job in &write_jobs {
                builder.add(job);
            }
            let groups = builder.finish();

            let codec = Lz4Codec;
            let mut indexes_by_key: std::collections::HashMap<
                (String, String),
                CompressedColumnIndex,
            > = std::collections::HashMap::new();
            // Precompute exact .col paths from jobs to ensure the same paths used in tests
            let mut key_to_path = std::collections::HashMap::new();
            for j in &write_jobs {
                key_to_path.insert(j.key.clone(), j.path.clone());
            }
            let mut block_writer = ColumnBlockWriter::with_paths(segment_dir.clone(), key_to_path);
            let path_resolver = ColumnPathResolver::new(&write_jobs);

            for ((key, zone_id), (buf, offs, values)) in groups {
                let index = indexes_by_key.entry(key.clone()).or_default();
                block_writer.append_zone(index, key.clone(), zone_id, &buf, offs, &codec)?;
                let _ = values; // values used only for compressed data; index built elsewhere
            }

            block_writer.finish()?;
            for (key, index) in indexes_by_key {
                let path = path_resolver.zfc_path_for_key(&key);
                index.write_to_path(&path)?;
            }

            Ok(())
        })
        .await
        .map_err(|e| StoreError::FlushFailed(format!("join error: {e}")))??;

        info!("wrote all columns");
        Ok(())
    }
}

// path helpers moved to column::compressed
