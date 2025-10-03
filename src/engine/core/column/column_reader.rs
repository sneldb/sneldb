// Compressed-only column reader
use crate::engine::core::QueryCaches;
use crate::engine::core::column::column_values::ColumnValues;
use crate::engine::core::column::compression::{CompressedColumnIndex, CompressionCodec, Lz4Codec};
use crate::engine::core::read::cache::DecompressedBlock;
use crate::engine::errors::{ColumnLoadError, QueryExecutionError};
use crate::shared::storage_header::{BinaryHeader, FileKind};
use memmap2::MmapOptions;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use tracing::info;

#[derive(Debug)]
pub struct ColumnReader;

impl ColumnReader {
    fn build_zero_copy_values(
        entry: &crate::engine::core::column::compression::ZoneBlockEntry,
        block: &Arc<DecompressedBlock>,
    ) -> Result<ColumnValues, QueryExecutionError> {
        let decompressed: &[u8] = block.as_bytes();
        let mut ranges: Vec<(usize, usize)> = Vec::with_capacity(entry.num_rows as usize);
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
            ranges.push((base + 2, len));
        }
        Ok(ColumnValues::new(Arc::clone(block), ranges))
    }
    pub fn load_for_zone_with_cache(
        segment_dir: &Path,
        segment_id: &str,
        uid: &str,
        field: &str,
        zone_id: u32,

        caches: Option<&QueryCaches>,
    ) -> Result<ColumnValues, QueryExecutionError> {
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
                    return Self::build_zero_copy_values(entry, &block);
                }
                return Ok(ColumnValues::new(
                    Arc::new(DecompressedBlock::from_bytes(Vec::new())),
                    Vec::new(),
                ));
            }
        }

        // Fallback to disk load (zero-copy variant)
        Self::load_zone_compressed_zero_copy(segment_dir, segment_id, uid, field, zone_id)
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

    // Note: legacy helper removed; use zero-copy path or disk path above

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

    #[inline]
    fn load_zone_compressed_zero_copy(
        segment_dir: &Path,
        _segment_id: &str,
        uid: &str,
        field: &str,
        zone_id: u32,
    ) -> Result<ColumnValues, QueryExecutionError> {
        let zfc_path = segment_dir.join(format!("{}_{}.zfc", uid, field));
        let index = CompressedColumnIndex::load_from_path(&zfc_path)
            .map_err(|e| QueryExecutionError::ColRead(format!("zfc load: {}", e)))?;
        let Some(entry) = index.entries.get(&zone_id) else {
            return Ok(ColumnValues::new(
                Arc::new(DecompressedBlock::from_bytes(Vec::new())),
                Vec::new(),
            ));
        };

        let col_path = segment_dir.join(format!("{}_{}.col", uid, field));
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
        let block = Arc::new(DecompressedBlock::from_bytes(decompressed));
        Self::build_zero_copy_values(entry, &block)
    }
}
