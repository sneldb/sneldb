use crate::engine::core::column::format::{ColumnBlockHeader, PhysicalType};
use crate::engine::core::column::reader::decoders::decoder_for;
use crate::engine::core::read::cache::DecompressedBlock;
use std::sync::Arc;

fn make_block(
    phys: PhysicalType,
    flags: u8,
    rows: u32,
    aux: Vec<u8>,
    payload: Vec<u8>,
) -> Arc<DecompressedBlock> {
    let hdr = ColumnBlockHeader {
        phys: phys.into(),
        flags,
        reserved: 0,
        row_count: rows,
        aux_len: aux.len() as u32,
    };
    let mut buf = Vec::new();
    hdr.write_to(&mut buf);
    buf.extend_from_slice(&aux);
    // pad to 8-byte for numeric
    if matches!(
        phys,
        PhysicalType::I64 | PhysicalType::U64 | PhysicalType::F64
    ) {
        let mis = (buf.len() % 8) as usize;
        if mis != 0 {
            buf.extend(std::iter::repeat(0u8).take(8 - mis));
        }
    }
    buf.extend_from_slice(&payload);
    Arc::new(DecompressedBlock::from_bytes(buf))
}

#[test]
fn decode_varbytes() {
    let aux = [5u32.to_le_bytes(), 2u32.to_le_bytes()].concat();
    let payload = b"helloZZ".to_vec();
    let block = make_block(PhysicalType::VarBytes, 0, 2, aux, payload);
    let view =
        crate::engine::core::column::reader::view::ColumnBlockView::parse(&block.bytes).unwrap();
    let vals = decoder_for(view.phys)
        .build_values(&view, 2, Arc::clone(&block))
        .unwrap();
    assert_eq!(vals.len(), 2);
    assert_eq!(vals.get_str_at(0).unwrap(), "hello");
    assert_eq!(vals.get_str_at(1).unwrap(), "ZZ");
}

#[test]
fn decode_i64() {
    let rows = 3u32;
    let aux = vec![0u8]; // nulls present, but none set
    let mut payload = Vec::new();
    for n in [10i64, 20, 30] {
        payload.extend_from_slice(&n.to_le_bytes());
    }
    let block = make_block(
        PhysicalType::I64,
        ColumnBlockHeader::FLAG_HAS_NULLS,
        rows,
        aux,
        payload,
    );
    let view =
        crate::engine::core::column::reader::view::ColumnBlockView::parse(&block.bytes).unwrap();
    let vals = decoder_for(view.phys)
        .build_values(&view, rows as usize, Arc::clone(&block))
        .unwrap();
    assert_eq!(vals.get_i64_at(0), Some(10));
    assert_eq!(vals.get_i64_at(2), Some(30));
}
