use crate::shared::storage_header::{BinaryHeader, FileKind, MagicFile};
use std::io::Cursor;

#[test]
fn header_roundtrip_ok() {
    let hdr = BinaryHeader::new(FileKind::ZoneMeta.magic(), 1, 0);
    let mut buf = Vec::new();
    hdr.write_to(&mut buf).unwrap();
    assert_eq!(buf.len(), BinaryHeader::TOTAL_LEN);

    let mut cur = Cursor::new(buf);
    let read = BinaryHeader::read_from(&mut cur).unwrap();
    assert_eq!(read.magic, FileKind::ZoneMeta.magic());
    assert_eq!(read.version, 1);
    assert_eq!(read.flags, 0);
    assert_eq!(read.reserved, 0);
    assert_eq!(read.header_crc32, hdr.header_crc32);
}

#[test]
fn wrong_magic_rejected_via_crc_mismatch() {
    let hdr = BinaryHeader::new(FileKind::ZoneMeta.magic(), 1, 0);
    let mut buf = Vec::new();
    hdr.write_to(&mut buf).unwrap();
    // Flip one magic byte; CRC should no longer match
    buf[0] ^= 0xFF;
    let mut cur = Cursor::new(buf);
    let read = BinaryHeader::read_from(&mut cur);
    assert!(read.is_err());
}

#[test]
fn version_mismatch_detected_by_trait() {
    struct Dummy;
    impl MagicFile for Dummy {
        const MAGIC: [u8; 8] = *b"EVDBTST\0";
        const VERSION: u16 = 2;
    }

    // Build header with version 1 but trait expects 2
    let hdr = BinaryHeader::new(*b"EVDBTST\0", 1, 0);
    let mut buf = Vec::new();
    hdr.write_to(&mut buf).unwrap();
    let mut cur = Cursor::new(buf);
    let res = Dummy::read_and_validate_header(&mut cur);
    assert!(res.is_err());
}

#[test]
fn crc_mismatch_rejected() {
    let hdr = BinaryHeader::new(FileKind::ZoneMeta.magic(), 1, 0);
    let mut buf = Vec::new();
    hdr.write_to(&mut buf).unwrap();

    // Corrupt the stored CRC32
    let crc_pos = BinaryHeader::TOTAL_LEN - 4;
    buf[crc_pos] ^= 0xAA;
    let mut cur = Cursor::new(buf);
    let read = BinaryHeader::read_from(&mut cur);
    assert!(read.is_err());
}

#[test]
fn truncated_header_rejected() {
    let hdr = BinaryHeader::new(FileKind::ZoneMeta.magic(), 1, 0);
    let mut buf = Vec::new();
    hdr.write_to(&mut buf).unwrap();
    buf.truncate(BinaryHeader::TOTAL_LEN - 1);
    let mut cur = Cursor::new(buf);
    let read = BinaryHeader::read_from(&mut cur);
    assert!(read.is_err());
}
