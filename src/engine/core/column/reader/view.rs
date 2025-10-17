use crate::engine::core::column::format::{ColumnBlockHeader, PhysicalType};
use crate::engine::errors::QueryExecutionError;

pub struct ColumnBlockView<'a> {
    pub header: ColumnBlockHeader,
    pub aux_start: usize,
    pub aux_end: usize,
    pub payload_start: usize,
    pub bytes: &'a [u8],
    pub phys: PhysicalType,
}

#[inline]
fn align_up(off: usize, align: usize) -> usize {
    let rem = off % align;
    if rem == 0 { off } else { off + (align - rem) }
}

impl<'a> ColumnBlockView<'a> {
    pub fn parse(bytes: &'a [u8]) -> Result<Self, QueryExecutionError> {
        if bytes.len() < ColumnBlockHeader::LEN {
            return Err(QueryExecutionError::ColRead(
                "decompressed block too small for header".into(),
            ));
        }
        let header = ColumnBlockHeader::read_from(&bytes[..ColumnBlockHeader::LEN])
            .ok_or_else(|| QueryExecutionError::ColRead("invalid column block header".into()))?;
        let aux_start = ColumnBlockHeader::LEN;
        let aux_end = aux_start + (header.aux_len as usize);
        if aux_end > bytes.len() {
            return Err(QueryExecutionError::ColRead(
                "aux section out of bounds".into(),
            ));
        }
        let phys = PhysicalType::from(header.phys);
        let payload_start = match phys {
            PhysicalType::I64 | PhysicalType::U64 | PhysicalType::F64 => align_up(aux_end, 8),
            _ => aux_end,
        };
        Ok(Self {
            header,
            aux_start,
            aux_end,
            payload_start,
            bytes,
            phys,
        })
    }

    pub fn payload_len(&self) -> usize {
        self.bytes.len().saturating_sub(self.payload_start)
    }
}
