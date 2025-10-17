use super::view::ColumnBlockView;
use crate::engine::core::column::column_values::ColumnValues;
use crate::engine::core::column::format::{ColumnBlockHeader, PhysicalType};
use crate::engine::core::read::cache::DecompressedBlock;
use crate::engine::errors::QueryExecutionError;
use std::sync::Arc;

pub trait ColumnDecoder {
    fn build_values(
        &self,
        view: &ColumnBlockView<'_>,
        entry_rows: usize,
        block: Arc<DecompressedBlock>,
    ) -> Result<ColumnValues, QueryExecutionError>;

    fn build_strings(
        &self,
        view: &ColumnBlockView<'_>,
        entry_rows: usize,
        block: Arc<DecompressedBlock>,
    ) -> Result<Vec<String>, QueryExecutionError> {
        let vals = self.build_values(view, entry_rows, block)?;
        let mut out = Vec::with_capacity(vals.len());
        for i in 0..vals.len() {
            if let Some(n) = vals.get_i64_at(i) {
                out.push(n.to_string());
                continue;
            }
            if let Some(n) = vals.get_u64_at(i) {
                out.push(n.to_string());
                continue;
            }
            if let Some(f) = vals.get_f64_at(i) {
                out.push(f.to_string());
                continue;
            }
            if let Some(b) = vals.get_bool_at(i) {
                out.push(if b { "true".into() } else { "false".into() });
                continue;
            }
            out.push(vals.get_str_at(i).unwrap_or("").to_string());
        }
        Ok(out)
    }
}

pub struct VarBytesDecoder;
pub struct I64Decoder;
pub struct U64Decoder;
pub struct F64Decoder;
pub struct BoolDecoder;

impl ColumnDecoder for VarBytesDecoder {
    fn build_values(
        &self,
        view: &ColumnBlockView<'_>,
        _entry_rows: usize,
        block: Arc<DecompressedBlock>,
    ) -> Result<ColumnValues, QueryExecutionError> {
        let row_count = view.header.row_count as usize;
        let need = row_count
            .checked_mul(4)
            .ok_or_else(|| QueryExecutionError::ColRead("aux len overflow".into()))?;
        if view.aux_start + need != view.aux_end {
            return Err(QueryExecutionError::ColRead(
                "invalid aux length for VarBytes".into(),
            ));
        }
        let mut ranges: Vec<(usize, usize)> = Vec::with_capacity(row_count);
        let mut cursor = 0usize;
        for i in 0..row_count {
            let base = view.aux_start + i * 4;
            let mut b = [0u8; 4];
            b.copy_from_slice(&view.bytes[base..base + 4]);
            let len = u32::from_le_bytes(b) as usize;
            let end = cursor + len;
            if end > view.payload_len() {
                return Err(QueryExecutionError::ColRead(format!(
                    "VarBytes payload OOB at idx={i}"
                )));
            }
            ranges.push((view.aux_end + cursor, len));
            cursor = end;
        }
        Ok(ColumnValues::new(block, ranges))
    }
}

impl ColumnDecoder for I64Decoder {
    fn build_values(
        &self,
        view: &ColumnBlockView<'_>,
        entry_rows: usize,
        block: Arc<DecompressedBlock>,
    ) -> Result<ColumnValues, QueryExecutionError> {
        let mut rows = view.header.row_count as usize;
        if rows == 0 {
            rows = entry_rows;
        }
        let need = rows
            .checked_mul(8)
            .ok_or_else(|| QueryExecutionError::ColRead("size overflow".into()))?;
        if view.payload_start + need > view.bytes.len() {
            return Err(QueryExecutionError::ColRead("numeric payload OOB".into()));
        }
        let nulls = if (view.header.flags & ColumnBlockHeader::FLAG_HAS_NULLS) != 0 {
            Some((view.aux_start, view.payload_start - view.aux_start))
        } else {
            None
        };
        Ok(ColumnValues::new_typed_i64(
            block,
            view.payload_start,
            rows,
            nulls,
        ))
    }
}

impl ColumnDecoder for U64Decoder {
    fn build_values(
        &self,
        view: &ColumnBlockView<'_>,
        entry_rows: usize,
        block: Arc<DecompressedBlock>,
    ) -> Result<ColumnValues, QueryExecutionError> {
        let mut rows = view.header.row_count as usize;
        if rows == 0 {
            rows = entry_rows;
        }
        let need = rows
            .checked_mul(8)
            .ok_or_else(|| QueryExecutionError::ColRead("size overflow".into()))?;
        if view.payload_start + need > view.bytes.len() {
            return Err(QueryExecutionError::ColRead("numeric payload OOB".into()));
        }
        let nulls = if (view.header.flags & ColumnBlockHeader::FLAG_HAS_NULLS) != 0 {
            Some((view.aux_start, view.payload_start - view.aux_start))
        } else {
            None
        };
        Ok(ColumnValues::new_typed_u64(
            block,
            view.payload_start,
            rows,
            nulls,
        ))
    }
}

impl ColumnDecoder for F64Decoder {
    fn build_values(
        &self,
        view: &ColumnBlockView<'_>,
        entry_rows: usize,
        block: Arc<DecompressedBlock>,
    ) -> Result<ColumnValues, QueryExecutionError> {
        let mut rows = view.header.row_count as usize;
        if rows == 0 {
            rows = entry_rows;
        }
        let need = rows
            .checked_mul(8)
            .ok_or_else(|| QueryExecutionError::ColRead("size overflow".into()))?;
        if view.payload_start + need > view.bytes.len() {
            return Err(QueryExecutionError::ColRead("numeric payload OOB".into()));
        }
        let nulls = if (view.header.flags & ColumnBlockHeader::FLAG_HAS_NULLS) != 0 {
            Some((view.aux_start, view.payload_start - view.aux_start))
        } else {
            None
        };
        Ok(ColumnValues::new_typed_f64(
            block,
            view.payload_start,
            rows,
            nulls,
        ))
    }
}

impl ColumnDecoder for BoolDecoder {
    fn build_values(
        &self,
        view: &ColumnBlockView<'_>,
        entry_rows: usize,
        block: Arc<DecompressedBlock>,
    ) -> Result<ColumnValues, QueryExecutionError> {
        let mut rows = view.header.row_count as usize;
        if rows == 0 {
            rows = entry_rows;
        }
        let bits_len = (rows + 7) / 8;
        if view.payload_start + bits_len > view.bytes.len() {
            return Err(QueryExecutionError::ColRead("bool payload OOB".into()));
        }
        let nulls = if (view.header.flags & ColumnBlockHeader::FLAG_HAS_NULLS) != 0 {
            Some((view.aux_start, view.aux_end - view.aux_start))
        } else {
            None
        };
        Ok(ColumnValues::new_typed_bool(
            block,
            view.payload_start,
            rows,
            nulls,
        ))
    }
}

static VARBYTES_DECODER: VarBytesDecoder = VarBytesDecoder;
static I64_DECODER: I64Decoder = I64Decoder;
static U64_DECODER: U64Decoder = U64Decoder;
static F64_DECODER: F64Decoder = F64Decoder;
static BOOL_DECODER: BoolDecoder = BoolDecoder;

pub fn decoder_for(phys: PhysicalType) -> &'static dyn ColumnDecoder {
    match phys {
        PhysicalType::VarBytes => &VARBYTES_DECODER,
        PhysicalType::I64 => &I64_DECODER,
        PhysicalType::U64 => &U64_DECODER,
        PhysicalType::F64 => &F64_DECODER,
        PhysicalType::Bool => &BOOL_DECODER,
        _ => &VARBYTES_DECODER,
    }
}
