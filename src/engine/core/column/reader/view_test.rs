use crate::engine::core::column::format::{ColumnBlockHeader, PhysicalType};
use crate::engine::core::column::reader::view::ColumnBlockView;

fn make_block(
    phys: PhysicalType,
    has_nulls: bool,
    rows: u32,
    aux: Vec<u8>,
    payload: Vec<u8>,
) -> Vec<u8> {
    let hdr = ColumnBlockHeader::new(phys, has_nulls, rows, aux.len() as u32);
    let mut buf = Vec::new();
    hdr.write_to(&mut buf);
    buf.extend_from_slice(&aux);
    buf.extend_from_slice(&payload);
    buf
}

#[test]
fn parse_varbytes_ok() {
    // rows=2, aux holds [len0,len1]
    let aux = [5u32.to_le_bytes(), 2u32.to_le_bytes()].concat();
    let payload = b"helloZZ".to_vec();
    let block = make_block(PhysicalType::VarBytes, false, 2, aux, payload);
    let view = ColumnBlockView::parse(&block).expect("parse");
    assert_eq!(view.header.row_count, 2);
    assert_eq!(view.aux_end - view.aux_start, 8);
    assert_eq!(view.payload_start, ColumnBlockHeader::LEN + 8);
}

#[test]
fn parse_numeric_aligns_payload() {
    // rows=1, aux len=1 byte of nulls to force misalignment
    let aux = vec![0x00];
    let payload = vec![0u8; 8];
    let block = make_block(PhysicalType::I64, true, 1, aux, payload);
    let view = ColumnBlockView::parse(&block).expect("parse");
    assert_eq!(view.payload_start % 8, 0);
}
