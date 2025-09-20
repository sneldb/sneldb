use crate::engine::core::column::compression::le_slice_reader::{
    LeSliceReader, SIZE_U32, SIZE_U64,
};

#[test]
fn reads_u32_and_advances() {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(0x11223344u32).to_le_bytes());
    buf.extend_from_slice(&(0x55667788u32).to_le_bytes());
    let mut r = LeSliceReader::new(&buf);

    assert!(r.has_bytes(SIZE_U32));
    let a = r.read_u32().unwrap();
    assert_eq!(a, 0x11223344);
    assert_eq!(r.remaining(), SIZE_U32);

    let b = r.read_u32().unwrap();
    assert_eq!(b, 0x55667788);
    assert_eq!(r.remaining(), 0);
    assert!(r.read_u32().is_none());
}

#[test]
fn reads_u64_and_advances() {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(0x0102030405060708u64).to_le_bytes());
    let mut r = LeSliceReader::new(&buf);

    assert!(r.has_bytes(SIZE_U64));
    let v = r.read_u64().unwrap();
    assert_eq!(v, 0x0102030405060708u64);
    assert_eq!(r.remaining(), 0);
    assert!(r.read_u64().is_none());
}

#[test]
fn mixed_reads_respect_order_and_bounds() {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(0xAAu32).to_le_bytes());
    buf.extend_from_slice(&(0x0Bu64).to_le_bytes());
    buf.extend_from_slice(&(0xCCu32).to_le_bytes());
    let mut r = LeSliceReader::new(&buf);

    assert_eq!(r.read_u32().unwrap(), 0xAA);
    assert_eq!(r.read_u64().unwrap(), 0x0B);
    assert_eq!(r.read_u32().unwrap(), 0xCC);
    assert_eq!(r.remaining(), 0);
}

#[test]
fn returns_none_when_insufficient_bytes() {
    let buf = vec![0x01, 0x02, 0x03]; // 3 bytes only
    let mut r = LeSliceReader::new(&buf);
    assert!(!r.has_bytes(SIZE_U32));
    assert!(r.read_u32().is_none());
    assert!(r.read_u64().is_none());
}
