use crate::engine::core::QueryCaches;
use crate::engine::core::column::column_block_snapshot::ColumnBlockSnapshot;
use crate::engine::core::column::column_values::ColumnValues;
use crate::engine::core::column::compression::CompressedColumnIndex;
use crate::engine::core::column::format::PhysicalType;
use crate::engine::core::column::reader::decompress;
use crate::engine::core::column::reader::io;
use crate::engine::core::read::cache::DecompressedBlock;
use crate::engine::errors::QueryExecutionError;
use std::path::Path;
use std::sync::Arc;
use tracing::{Level, info};

/// ColumnReader is a thin facade around the reader/* pipeline.
///
/// Responsibilities:
/// - Map column file and bounds-check compressed slice (reader::io)
/// - Decompress into a pooled buffer (reader::decompress)
/// - Parse typed block view (reader::view)
/// - Delegate decode to per-physical-type decoders (reader::decoders)
///
/// Format-specific logic stays in reader/*; this type coordinates those steps
/// and provides legacy helpers for Vec<String> materialization.
#[derive(Debug)]
pub struct ColumnReader;

impl ColumnReader {
    /// Build a zero-copy ColumnValues view for a decompressed block. For legacy
    /// var-bytes (no header) constructs ranges directly; otherwise parses a
    /// ColumnBlockView and delegates to decoders.
    fn build_zero_copy_values(
        entry: &crate::engine::core::column::compression::ZoneBlockEntry,
        block: &Arc<DecompressedBlock>,
    ) -> Result<(PhysicalType, ColumnValues), QueryExecutionError> {
        let decompressed: &[u8] = &block.bytes;

        use crate::engine::core::column::format::ColumnBlockHeader;

        // Legacy var-bytes blocks had no typed header; detect by size.
        if decompressed.len() < ColumnBlockHeader::LEN {
            // Legacy var-bytes block: sequence of [u16 len][bytes]
            let mut ranges: Vec<(usize, usize)> = Vec::with_capacity(entry.num_rows as usize);
            let mut cursor: usize = 0;
            for _ in 0..(entry.num_rows as usize) {
                if cursor + 2 > decompressed.len() {
                    return Err(QueryExecutionError::ColRead(format!(
                        "legacy var-bytes header truncated: cursor+2={} > len={}",
                        cursor + 2,
                        decompressed.len()
                    )));
                }
                let len =
                    u16::from_le_bytes([decompressed[cursor], decompressed[cursor + 1]]) as usize;
                let start = cursor + 2;
                let end = start + len;
                if end > decompressed.len() {
                    return Err(QueryExecutionError::ColRead(format!(
                        "legacy var-bytes payload OOB: end={} > len={}",
                        end,
                        decompressed.len()
                    )));
                }
                ranges.push((start, len));
                cursor = end;
            }
            if ranges.len() != entry.num_rows as usize {
                return Err(QueryExecutionError::ColRead(format!(
                    "legacy var-bytes row count mismatch: parsed={} expected={}",
                    ranges.len(),
                    entry.num_rows
                )));
            }
            return Ok((
                PhysicalType::VarBytes,
                ColumnValues::new(Arc::clone(block), ranges),
            ));
        }

        // Delegate type-specific decode to decoders
        let view = crate::engine::core::column::reader::view::ColumnBlockView::parse(decompressed)?;
        let phys = view.phys;
        let values = super::reader::decoders::decoder_for(phys).build_values(
            &view,
            entry.num_rows as usize,
            Arc::clone(block),
        )?;
        Ok((phys, values))
    }

    /// Load a zone using caches where possible, returning a zero-copy view.
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
                if tracing::enabled!(Level::INFO) {
                    info!(
                        target: "cache::column_handle::hit",
                        %segment_id,
                        %uid,
                        %field,
                        zone_id,
                        "Using cached ColumnHandle"
                    );
                }
                if let Some(entry) = handle.zfc_index.entries.get(&zone_id) {
                    let block = caches_ref
                        .get_or_load_decompressed_block(
                            &handle, segment_id, uid, field, zone_id, entry,
                        )
                        .map_err(|e| QueryExecutionError::ColRead(format!("cache load: {e}")))?;
                    let (_phys, values) = Self::build_zero_copy_values(entry, &block)?;
                    return Ok(values);
                }
                // Legitimate empty: zone not present.
                return Ok(ColumnValues::empty());
            }
        }

        // Fallback to disk path (zero-copy)
        Self::load_zone_compressed_zero_copy(segment_dir, segment_id, uid, field, zone_id)
    }

    pub fn load_for_zone_snapshot(
        segment_dir: &Path,
        segment_id: &str,
        uid: &str,
        field: &str,
        zone_id: u32,
        caches: Option<&QueryCaches>,
    ) -> Result<ColumnBlockSnapshot, QueryExecutionError> {
        if let Some(caches_ref) = caches {
            if let Ok(handle) = caches_ref.get_or_load_column_handle(segment_id, uid, field) {
                if tracing::enabled!(Level::INFO) {
                    info!(
                        target: "cache::column_handle::hit",
                        %segment_id,
                        %uid,
                        %field,
                        zone_id,
                        "Using cached ColumnHandle"
                    );
                }
                if let Some(entry) = handle.zfc_index.entries.get(&zone_id) {
                    let block = caches_ref
                        .get_or_load_decompressed_block(
                            &handle, segment_id, uid, field, zone_id, entry,
                        )
                        .map_err(|e| QueryExecutionError::ColRead(format!("cache load: {e}")))?;
                    let (phys, values) = Self::build_zero_copy_values(entry, &block)?;
                    return Ok(ColumnBlockSnapshot::new(phys, values));
                }
                return Ok(ColumnBlockSnapshot::empty());
            }
        }

        Self::load_zone_compressed_snapshot(segment_dir, segment_id, uid, field, zone_id)
    }

    /// Convenience path that collects strings (legacy callers). Decodes via the
    /// same pipeline and converts to Vec<String> for backward compatibility.
    pub fn load_for_zone(
        segment_dir: &Path,
        _segment_id: &str,
        uid: &str,
        field: &str,
        zone_id: u32,
    ) -> Result<Vec<String>, QueryExecutionError> {
        if tracing::enabled!(Level::INFO) {
            info!(
                target: "col_reader::zone_load",
                %uid,
                %field,
                zone_id,
                "Loading column values for specific zone (legacy Vec<String> path)"
            );
        }
        let snapshot =
            Self::load_zone_compressed_snapshot(segment_dir, _segment_id, uid, field, zone_id)?;
        Ok(snapshot.into_strings())
    }

    /// Legacy helper: fully materialize strings from a compressed block.
    fn load_zone_compressed(
        segment_dir: &Path,
        _segment_id: &str,
        uid: &str,
        field: &str,
        zone_id: u32,
    ) -> Result<Vec<String>, QueryExecutionError> {
        let snapshot =
            Self::load_zone_compressed_snapshot(segment_dir, _segment_id, uid, field, zone_id)?;
        Ok(snapshot.into_strings())
    }

    /// Zero-copy load that returns a ColumnValues view, using the pooled buffer underneath.
    #[inline]
    fn load_zone_compressed_zero_copy(
        segment_dir: &Path,
        _segment_id: &str,
        uid: &str,
        field: &str,
        zone_id: u32,
    ) -> Result<ColumnValues, QueryExecutionError> {
        let snapshot =
            Self::load_zone_compressed_snapshot(segment_dir, _segment_id, uid, field, zone_id)?;
        Ok(snapshot.into_values())
    }

    fn load_zone_compressed_snapshot(
        segment_dir: &Path,
        _segment_id: &str,
        uid: &str,
        field: &str,
        zone_id: u32,
    ) -> Result<ColumnBlockSnapshot, QueryExecutionError> {
        let zfc_path = segment_dir.join(format!("{}_{}.zfc", uid, field));
        let index = CompressedColumnIndex::load_from_path(&zfc_path)
            .map_err(|e| QueryExecutionError::ColRead(format!("zfc load: {e}")))?;
        let Some(entry) = index.entries.get(&zone_id) else {
            return Ok(ColumnBlockSnapshot::empty());
        };

        let mmap = io::map_column_file(segment_dir, uid, field)?;
        let (start, end) = io::compressed_range(entry, mmap.len())?;
        let compressed = &mmap[start..end];

        let decompressed = decompress::decompress_block(compressed, entry.uncomp_len as usize)?;

        let block = Arc::new(DecompressedBlock::from_bytes(decompressed));
        let (phys, values) = Self::build_zero_copy_values(entry, &block)?;
        Ok(ColumnBlockSnapshot::new(phys, values))
    }
}
