// Compressed-only column reader
use crate::engine::core::column::compression::{CompressedColumnIndex, CompressionCodec, Lz4Codec};
use crate::engine::core::read::cache::GlobalColumnBlockCache;
use crate::engine::core::{ColumnHandle, QueryCaches};
use crate::engine::errors::{ColumnLoadError, QueryExecutionError};
use crate::shared::storage_header::{BinaryHeader, FileKind};
use memmap2::MmapOptions;
use std::fs::File;
use std::path::Path;
use tracing::info;

#[derive(Debug)]
pub struct ColumnReader;

impl ColumnReader {
    fn parse_values(
        entry: &crate::engine::core::column::compression::ZoneBlockEntry,
        decompressed: &[u8],
    ) -> Result<Vec<String>, QueryExecutionError> {
        let mut out = Vec::with_capacity(entry.num_rows as usize);
        for &off in &entry.in_block_offsets {
            let base = off as usize;
            if base + 2 > decompressed.len() {
                return Err(QueryExecutionError::ColRead("Offset out of bounds".into()));
            }
            let len = u16::from_le_bytes(decompressed[base..base + 2].try_into().unwrap()) as usize;
            let end = base + 2 + len;
            if end > decompressed.len() {
                return Err(QueryExecutionError::ColRead(
                    "Value length out of bounds".into(),
                ));
            }
            let val = String::from_utf8(decompressed[base + 2..end].to_vec())
                .map_err(|e| QueryExecutionError::ColRead(format!("Invalid UTF-8: {}", e)))?;
            out.push(val);
        }
        Ok(out)
    }
    pub fn load_for_zone_with_cache(
        segment_dir: &Path,
        segment_id: &str,
        uid: &str,
        field: &str,
        zone_id: u32,

        caches: Option<&QueryCaches>,
    ) -> Result<Vec<String>, QueryExecutionError> {
        if let Some(caches_ref) = caches {
            if let Ok(handle) = caches_ref.get_or_load_column_handle(segment_id, uid, field) {
                info!(
                    target: "cache::column_handle::hit",
                    %segment_id,
                    %uid,
                    %field,
                    zone_id,
                    "Using cached ColumnHandle"
                );
                // Try per-query memoized decompressed block, falling back to global cache
                if let Some(entry) = handle.zfc_index.entries.get(&zone_id) {
                    let block = caches_ref
                        .get_or_load_decompressed_block(
                            &handle, segment_id, uid, field, zone_id, entry,
                        )
                        .map_err(|e| QueryExecutionError::ColRead(format!("cache load: {}", e)))?;
                    return Self::parse_values(entry, &block.bytes);
                }
                return Ok(Vec::new());
            }
        }

        // Fallback to disk load
        Self::load_for_zone(segment_dir, segment_id, uid, field, zone_id)
    }

    pub fn load_for_zone(
        segment_dir: &Path,
        segment_id: &str,
        uid: &str,
        field: &str,
        zone_id: u32,
    ) -> Result<Vec<String>, QueryExecutionError> {
        info!(
            target: "col_reader::zone_load",
            %segment_id,
            %uid,
            %field,
            zone_id,
            "Loading column values for specific zone"
        );
        // Compressed-only
        Self::load_zone_compressed(segment_dir, segment_id, uid, field, zone_id)
    }

    fn load_from_handle(
        handle: &ColumnHandle,
        zone_id: u32,
    ) -> Result<Vec<String>, QueryExecutionError> {
        let Some(entry) = handle.zfc_index.entries.get(&zone_id) else {
            return Ok(Vec::new());
        };
        // Attempt global block cache first
        let (block, _outcome) = GlobalColumnBlockCache::instance()
            .get_or_load(&handle.col_path, zone_id, || {
                // Loader: slice compressed block and decompress
                let start = entry.block_start as usize;
                let end = start + entry.comp_len as usize;
                if end > handle.col_mmap.len() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Compressed block out of bounds",
                    ));
                }
                let compressed = &handle.col_mmap[start..end];
                let codec = Lz4Codec;
                let decompressed =
                    CompressionCodec::decompress(&codec, compressed, entry.uncomp_len as usize)
                        .map_err(|e| {
                            std::io::Error::new(
                                std::io::ErrorKind::Other,
                                format!("decompress: {}", e),
                            )
                        })?;
                Ok(decompressed)
            })
            .map_err(|e| QueryExecutionError::ColRead(format!("cache load: {}", e)))?;

        Self::parse_values(entry, &block.bytes)
    }

    fn load_zone_compressed(
        segment_dir: &Path,
        _segment_id: &str,
        uid: &str,
        field: &str,
        zone_id: u32,
    ) -> Result<Vec<String>, QueryExecutionError> {
        let zfc_path = segment_dir.join(format!("{}_{}.zfc", uid, field));
        let index = CompressedColumnIndex::load_from_path(&zfc_path)
            .map_err(|e| QueryExecutionError::ColRead(format!("zfc load: {}", e)))?;
        let Some(entry) = index.entries.get(&zone_id) else {
            return Ok(Vec::new());
        };

        let col_path = segment_dir.join(format!("{}_{}.col", uid, field));
        // Validate header
        let mut f = File::open(&col_path)
            .map_err(|e| QueryExecutionError::ColLoad(ColumnLoadError::Io(e)))?;
        let header = BinaryHeader::read_from(&mut f)
            .map_err(|e| QueryExecutionError::ColRead(format!("Header read failed: {}", e)))?;
        if header.magic != FileKind::SegmentColumn.magic() {
            return Err(QueryExecutionError::ColRead(
                "invalid magic for .col".into(),
            ));
        }
        let file = File::open(&col_path)
            .map_err(|e| QueryExecutionError::ColLoad(ColumnLoadError::Io(e)))?;
        let mmap = unsafe {
            MmapOptions::new()
                .map(&file)
                .map_err(|e| QueryExecutionError::ColLoad(ColumnLoadError::Io(e)))?
        };
        let start = entry.block_start as usize;
        let end = start + entry.comp_len as usize;
        if end > mmap.len() {
            return Err(QueryExecutionError::ColRead(
                "Compressed block out of bounds".into(),
            ));
        }
        let compressed = &mmap[start..end];

        let codec = Lz4Codec;
        let decompressed =
            CompressionCodec::decompress(&codec, compressed, entry.uncomp_len as usize)
                .map_err(|e| QueryExecutionError::ColRead(format!("decompress: {}", e)))?;

        let mut out = Vec::with_capacity(entry.num_rows as usize);
        for &off in &entry.in_block_offsets {
            let base = off as usize;
            if base + 2 > decompressed.len() {
                return Err(QueryExecutionError::ColRead("Offset out of bounds".into()));
            }
            let len = u16::from_le_bytes(decompressed[base..base + 2].try_into().unwrap()) as usize;
            let end = base + 2 + len;
            if end > decompressed.len() {
                return Err(QueryExecutionError::ColRead(
                    "Value length out of bounds".into(),
                ));
            }
            let val = String::from_utf8(decompressed[base + 2..end].to_vec())
                .map_err(|e| QueryExecutionError::ColRead(format!("Invalid UTF-8: {}", e)))?;
            out.push(val);
        }
        Ok(out)
    }
}
