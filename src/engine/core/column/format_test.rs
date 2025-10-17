use crate::engine::core::column::format::{ColumnBlockHeader, PhysicalType};

#[test]
fn physical_type_from_u8_and_into_u8() {
    // into<u8>
    assert_eq!(u8::from(PhysicalType::VarBytes), 0);
    assert_eq!(u8::from(PhysicalType::I64), 1);
    assert_eq!(u8::from(PhysicalType::U64), 2);
    assert_eq!(u8::from(PhysicalType::F64), 3);
    assert_eq!(u8::from(PhysicalType::Bool), 4);
    assert_eq!(u8::from(PhysicalType::I32Date), 5);

    // from<u8>
    assert_eq!(PhysicalType::from(0u8), PhysicalType::VarBytes);
    assert_eq!(PhysicalType::from(1u8), PhysicalType::I64);
    assert_eq!(PhysicalType::from(2u8), PhysicalType::U64);
    assert_eq!(PhysicalType::from(3u8), PhysicalType::F64);
    assert_eq!(PhysicalType::from(4u8), PhysicalType::Bool);
    assert_eq!(PhysicalType::from(5u8), PhysicalType::I32Date);

    // Unknown values map to VarBytes by design
    for v in [6u8, 200u8, 255u8] {
        assert_eq!(PhysicalType::from(v), PhysicalType::VarBytes);
    }
}

#[test]
fn header_len_matches_repr_c_layout() {
    // ColumnBlockHeader::LEN is expected to be 12 bytes (u8 + u8 + u16 + u32 + u32)
    assert_eq!(ColumnBlockHeader::LEN, 12);
}

#[test]
fn header_write_read_roundtrip_no_nulls() {
    let hdr = ColumnBlockHeader::new(PhysicalType::VarBytes, false, 123, 456);
    let mut buf = Vec::new();
    hdr.write_to(&mut buf);
    assert_eq!(buf.len(), ColumnBlockHeader::LEN);

    let parsed = ColumnBlockHeader::read_from(&buf).expect("header parse");
    assert_eq!(parsed.phys, u8::from(PhysicalType::VarBytes));
    assert_eq!(parsed.flags & ColumnBlockHeader::FLAG_HAS_NULLS, 0);
    assert_eq!(parsed.row_count, 123);
    assert_eq!(parsed.aux_len, 456);
}

#[test]
fn header_write_read_roundtrip_with_nulls() {
    let hdr = ColumnBlockHeader::new(PhysicalType::I64, true, 1_000, 32);
    let mut buf = Vec::new();
    hdr.write_to(&mut buf);

    let parsed = ColumnBlockHeader::read_from(&buf).expect("header parse");
    assert_eq!(parsed.phys, u8::from(PhysicalType::I64));
    assert_ne!(parsed.flags & ColumnBlockHeader::FLAG_HAS_NULLS, 0);
    assert_eq!(parsed.row_count, 1_000);
    assert_eq!(parsed.aux_len, 32);
}

#[test]
fn header_read_from_too_short() {
    // Provide fewer than LEN bytes; expect None
    let buf = vec![0u8; ColumnBlockHeader::LEN - 1];
    assert!(ColumnBlockHeader::read_from(&buf).is_none());
}
