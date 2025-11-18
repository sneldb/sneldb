use std::collections::HashMap as Map;
use std::path::PathBuf;
use tokio::fs::File as TokioFile;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

use crate::engine::core::ColumnKey;
use crate::engine::errors::StoreError;
use crate::shared::storage_header::{FileKind, ensure_header_if_new_async};

use crate::engine::core::column::compression::compressed_column_index::{
    CompressedColumnIndex, ZoneBlockEntry,
};
use crate::engine::core::column::compression::compression_codec::CompressionCodec;

pub struct ColumnBlockWriterAsync {
    segment_dir: PathBuf,
    writers: Map<ColumnKey, TokioFile>,
    current_offsets: Map<ColumnKey, u64>,
    key_to_path: Option<Map<ColumnKey, PathBuf>>,
}

impl ColumnBlockWriterAsync {
    pub fn new(segment_dir: PathBuf) -> Self {
        Self {
            segment_dir,
            writers: Map::new(),
            current_offsets: Map::new(),
            key_to_path: None,
        }
    }

    pub fn with_paths(segment_dir: PathBuf, key_to_path: Map<ColumnKey, PathBuf>) -> Self {
        Self {
            segment_dir,
            writers: Map::new(),
            current_offsets: Map::new(),
            key_to_path: Some(key_to_path),
        }
    }

    fn column_path(&self, key: &ColumnKey) -> PathBuf {
        if let Some(map) = &self.key_to_path {
            if let Some(p) = map.get(key) {
                return p.clone();
            }
        }
        self.segment_dir.join(format!("{}_{}.col", key.0, key.1))
    }

    pub async fn append_zone<C: CompressionCodec>(
        &mut self,
        index: &mut CompressedColumnIndex,
        key: ColumnKey,
        zone_id: u32,
        uncompressed_block: &[u8],
        row_count: u32,
        codec: &C,
    ) -> Result<(), StoreError> {
        // Ensure writer exists
        if !self.writers.contains_key(&key) {
            let path = self.column_path(&key);
            let file = ensure_header_if_new_async(&path, FileKind::SegmentColumn.magic())
                .await
                .map_err(|e| {
                    StoreError::FlushFailed(format!(
                        "Failed to open/create .col file with header: {}",
                        e
                    ))
                })?;
            self.writers.insert(key.clone(), file);
        }
        let writer = self.writers.get_mut(&key).expect("writer just inserted");

        // Get current offset (seek to end to get file size)
        let current_pos = writer
            .seek(tokio::io::SeekFrom::End(0))
            .await
            .map_err(|e| StoreError::FlushFailed(format!("Failed to seek to end: {}", e)))?;
        let start = self
            .current_offsets
            .entry(key.clone())
            .or_insert(current_pos);
        let block_start = *start;
        let uncompressed_len = uncompressed_block.len() as u32;
        let compressed = codec.compress(uncompressed_block)?;

        // Write compressed data
        writer.write_all(&compressed).await.map_err(|e| {
            StoreError::FlushFailed(format!("Failed to write compressed data: {}", e))
        })?;
        *start += compressed.len() as u64;

        index.entries.insert(
            zone_id,
            ZoneBlockEntry {
                zone_id,
                block_start,
                comp_len: compressed.len() as u32,
                uncomp_len: uncompressed_len,
                num_rows: row_count,
            },
        );

        Ok(())
    }

    pub async fn finish(&mut self) -> Result<(), StoreError> {
        for (_key, file) in self.writers.drain() {
            file.sync_all()
                .await
                .map_err(|e| StoreError::FlushFailed(format!("Failed to sync file: {}", e)))?;
        }
        Ok(())
    }
}
