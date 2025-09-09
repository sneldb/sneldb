use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

use crate::engine::core::{ColumnKey, ColumnOffsets, UidResolver, WriteJob, ZonePlan};
use crate::engine::errors::StoreError;
use crate::engine::schema::SchemaRegistry;
use crate::shared::storage_header::{FileKind, ensure_header_if_new};

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

    pub async fn write_all(&self, zone_plans: &[ZonePlan]) -> Result<ColumnOffsets, StoreError> {
        let resolver = UidResolver::from_events(zone_plans, &self.registry).await?;
        let write_jobs = WriteJob::build(zone_plans, &self.segment_dir, &resolver);

        let write_result =
            tokio::task::spawn_blocking(move || -> Result<ColumnOffsets, StoreError> {
                let mut writers: HashMap<ColumnKey, BufWriter<File>> = HashMap::new();
                let mut current_offsets: HashMap<ColumnKey, u64> = HashMap::new();
                let mut offsets = ColumnOffsets::new();

                for job in write_jobs {
                    let writer = writers.entry(job.key.clone()).or_insert_with(|| {
                        let file = ensure_header_if_new(&job.path, FileKind::SegmentColumn.magic())
                            .expect("Failed to open/create .col file with header");
                        BufWriter::new(file)
                    });

                    let offset = current_offsets.entry(job.key.clone()).or_insert_with(|| {
                        writer
                            .seek(SeekFrom::End(0))
                            .expect("Failed to seek to end of file")
                    });

                    let this_offset = *offset;
                    let bytes = job.value.as_bytes();

                    if bytes.len() > u16::MAX as usize {
                        return Err(StoreError::FlushFailed(format!(
                            "value too long for field {}",
                            job.key.1
                        )));
                    }

                    writer.write_all(&(bytes.len() as u16).to_le_bytes())?;
                    writer.write_all(bytes)?;

                    *offset += 2 + bytes.len() as u64;
                    offsets.insert_offset(job.key, job.zone_id, this_offset, job.value);
                }

                for (key, mut writer) in writers {
                    writer.flush()?;
                    writer.get_ref().sync_data()?;
                    debug!("flushed writer for {:?}", key);
                }

                Ok(offsets)
            })
            .await
            .map_err(|e| StoreError::FlushFailed(format!("join error: {e}")))??;

        info!("wrote all columns");
        Ok(write_result)
    }
}
