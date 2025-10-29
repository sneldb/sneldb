use crate::engine::core::column::compression::compressed_column_index::CompressedColumnIndex;
use crate::engine::core::column::compression::compression_codec::Lz4Codec;
use crate::engine::core::column::format::PhysicalType;
use crate::engine::core::column::type_catalog::ColumnTypeCatalog;
use crate::engine::core::write::column_block_writer::ColumnBlockWriter;
use crate::engine::core::write::column_group_builder::ColumnGroupBuilder;
use crate::engine::core::write::column_paths::ColumnPathResolver;
use crate::engine::core::{UidResolver, WriteJob, ZonePlan};
use crate::engine::errors::StoreError;
use crate::engine::schema::SchemaRegistry;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;
pub struct ColumnWriter {
    pub segment_dir: PathBuf,
    pub registry: Arc<RwLock<SchemaRegistry>>,
    type_hints: Option<ColumnTypeCatalog>,
}

impl ColumnWriter {
    pub fn new(segment_dir: PathBuf, registry: Arc<RwLock<SchemaRegistry>>) -> Self {
        Self {
            segment_dir,
            registry,
            type_hints: None,
        }
    }

    pub fn with_type_hints(mut self, hints: ColumnTypeCatalog) -> Self {
        self.type_hints = Some(hints);
        self
    }

    fn merge_type_hints(
        &self,
        types_by_key: &mut std::collections::HashMap<(String, String), PhysicalType>,
    ) {
        if let Some(hints) = &self.type_hints {
            use std::collections::hash_map::Entry;
            for (key, phys) in hints.iter() {
                match types_by_key.entry(key.clone()) {
                    Entry::Occupied(mut existing) => {
                        if *existing.get() == PhysicalType::VarBytes {
                            existing.insert(*phys);
                        }
                    }
                    Entry::Vacant(vacant) => {
                        vacant.insert(*phys);
                    }
                }
            }
        }
    }

    pub async fn write_all(&self, zone_plans: &[ZonePlan]) -> Result<(), StoreError> {
        let resolver = UidResolver::from_events(zone_plans, &self.registry).await?;
        let write_jobs = WriteJob::build(zone_plans, &self.segment_dir, &resolver);
        // Build schema-driven physical type mapping for keys present in this flush
        let mut types_by_key: std::collections::HashMap<(String, String), PhysicalType> =
            std::collections::HashMap::new();
        {
            use crate::engine::schema::types::FieldType;
            let reg = self.registry.read().await;
            for j in &write_jobs {
                let (event_type, field) = (&j.key.0, &j.key.1);
                let phys = if field == "timestamp" {
                    PhysicalType::I64
                } else if let Some(schema) = reg.get(event_type) {
                    match schema.field_type(field) {
                        Some(FieldType::I64) => PhysicalType::I64,
                        Some(FieldType::U64) => PhysicalType::U64,
                        Some(FieldType::F64) => PhysicalType::F64,
                        Some(FieldType::Bool) => PhysicalType::Bool,
                        Some(FieldType::Optional(inner)) => match inner.as_ref() {
                            FieldType::I64 => PhysicalType::I64,
                            FieldType::U64 => PhysicalType::U64,
                            FieldType::F64 => PhysicalType::F64,
                            FieldType::Bool => PhysicalType::Bool,
                            _ => PhysicalType::VarBytes,
                        },
                        _ => PhysicalType::VarBytes,
                    }
                } else {
                    PhysicalType::VarBytes
                };
                types_by_key.insert((event_type.clone(), field.clone()), phys);
            }
        }
        self.merge_type_hints(&mut types_by_key);
        let segment_dir = self.segment_dir.clone();

        tokio::task::spawn_blocking(move || -> Result<(), StoreError> {
            let mut builder = ColumnGroupBuilder::with_types(types_by_key);
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
                let row_count = offs.len() as u32;
                block_writer.append_zone(index, key.clone(), zone_id, &buf, row_count, &codec)?;
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
