use std::collections::HashMap as Map;
use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;

use crate::engine::core::ColumnKey;
use crate::engine::errors::StoreError;
use crate::shared::storage_header::{FileKind, ensure_header_if_new};

use crate::engine::core::column::compression::codec::CompressionCodec;
use crate::engine::core::column::compression::index::{CompressedColumnIndex, ZoneBlockEntry};

pub struct ColumnBlockWriter {
    segment_dir: PathBuf,
    writers: Map<ColumnKey, std::io::BufWriter<File>>,
    current_offsets: Map<ColumnKey, u64>,
    key_to_path: Option<Map<ColumnKey, PathBuf>>,
}

impl ColumnBlockWriter {
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

    pub fn append_zone<C: CompressionCodec>(
        &mut self,
        index: &mut CompressedColumnIndex,
        key: ColumnKey,
        zone_id: u32,
        uncompressed_block: &[u8],
        in_block_offsets: Vec<u32>,
        codec: &C,
    ) -> Result<(), StoreError> {
        // Ensure writer exists without borrowing self immutably during the entry borrow
        if !self.writers.contains_key(&key) {
            let path = self.column_path(&key);
            let file = ensure_header_if_new(&path, FileKind::SegmentColumn.magic())
                .expect("Failed to open/create .col file with header");
            self.writers
                .insert(key.clone(), std::io::BufWriter::new(file));
        }
        let writer = self.writers.get_mut(&key).expect("writer just inserted");

        let start = self
            .current_offsets
            .entry(key.clone())
            .or_insert_with(|| writer.seek(SeekFrom::End(0)).expect("seek end"));
        let block_start = *start;
        let uncompressed_len = uncompressed_block.len() as u32;
        let compressed = codec.compress(uncompressed_block)?;
        writer.write_all(&compressed)?;
        *start += compressed.len() as u64;

        index.entries.insert(
            zone_id,
            ZoneBlockEntry {
                zone_id,
                block_start,
                comp_len: compressed.len() as u32,
                uncomp_len: uncompressed_len,
                num_rows: in_block_offsets.len() as u32,
                in_block_offsets,
            },
        );

        Ok(())
    }

    pub fn finish(&mut self) -> Result<(), StoreError> {
        for (_key, mut w) in self.writers.drain() {
            w.flush()?;
            w.get_ref().sync_data()?;
        }
        Ok(())
    }
}
