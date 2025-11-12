use crate::shared::storage_header::{
    BinaryHeader, FileKind, MagicFile, ensure_header_if_new, open_and_header_offset,
};
use std::fs::File;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use tempfile::tempdir;

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

#[test]
fn header_constants_correct() {
    assert_eq!(BinaryHeader::LEN_WITHOUT_CRC, 16);
    assert_eq!(BinaryHeader::TOTAL_LEN, 20);
}

#[test]
fn header_new_computes_crc32() {
    let hdr = BinaryHeader::new(*b"TESTMAG\0", 42, 7);
    assert_eq!(hdr.magic, *b"TESTMAG\0");
    assert_eq!(hdr.version, 42);
    assert_eq!(hdr.flags, 7);
    assert_eq!(hdr.reserved, 0);
    assert_ne!(hdr.header_crc32, 0); // CRC should be computed
}

#[test]
fn header_new_reserved_always_zero() {
    let hdr = BinaryHeader::new(*b"TESTMAG\0", 1, 0);
    assert_eq!(hdr.reserved, 0);
}

#[test]
fn header_with_different_versions() {
    for version in [0, 1, 42, 255, 65535] {
        let hdr = BinaryHeader::new(*b"TESTVER\0", version, 0);
        assert_eq!(hdr.version, version);
        let mut buf = Vec::new();
        hdr.write_to(&mut buf).unwrap();
        let mut cur = Cursor::new(buf);
        let read = BinaryHeader::read_from(&mut cur).unwrap();
        assert_eq!(read.version, version);
    }
}

#[test]
fn header_with_different_flags() {
    for flags in [0, 1, 42, 255, 65535] {
        let hdr = BinaryHeader::new(*b"TESTFLG\0", 1, flags);
        assert_eq!(hdr.flags, flags);
        let mut buf = Vec::new();
        hdr.write_to(&mut buf).unwrap();
        let mut cur = Cursor::new(buf);
        let read = BinaryHeader::read_from(&mut cur).unwrap();
        assert_eq!(read.flags, flags);
    }
}

#[test]
fn header_with_different_magic_values() {
    let magics = [
        *b"ABCDEFG\0",
        *b"1234567\0",
        *b"\0\0\0\0\0\0\0\0",
        *b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
    ];
    for magic in magics {
        let hdr = BinaryHeader::new(magic, 1, 0);
        assert_eq!(hdr.magic, magic);
        let mut buf = Vec::new();
        hdr.write_to(&mut buf).unwrap();
        let mut cur = Cursor::new(buf);
        let read = BinaryHeader::read_from(&mut cur).unwrap();
        assert_eq!(read.magic, magic);
    }
}

#[test]
fn header_crc32_changes_with_any_field_change() {
    let base = BinaryHeader::new(*b"TESTCRC\0", 1, 0);
    let mut buf = Vec::new();
    base.write_to(&mut buf).unwrap();
    let base_crc = base.header_crc32;

    // Change magic
    let hdr_magic = BinaryHeader::new(*b"TESTCRD\0", 1, 0);
    assert_ne!(hdr_magic.header_crc32, base_crc);

    // Change version
    let hdr_vers = BinaryHeader::new(*b"TESTCRC\0", 2, 0);
    assert_ne!(hdr_vers.header_crc32, base_crc);

    // Change flags
    let hdr_flags = BinaryHeader::new(*b"TESTCRC\0", 1, 1);
    assert_ne!(hdr_flags.header_crc32, base_crc);
}

#[test]
fn header_corruption_detection() {
    let hdr = BinaryHeader::new(*b"TESTCOR\0", 1, 0);
    let mut buf = Vec::new();
    hdr.write_to(&mut buf).unwrap();

    // Corrupt version byte
    buf[8] ^= 0xFF;
    let mut cur = Cursor::new(buf.clone());
    assert!(BinaryHeader::read_from(&mut cur).is_err());

    // Corrupt flags byte
    buf = Vec::new();
    hdr.write_to(&mut buf).unwrap();
    buf[10] ^= 0xFF;
    let mut cur = Cursor::new(buf.clone());
    assert!(BinaryHeader::read_from(&mut cur).is_err());

    // Corrupt reserved bytes
    buf = Vec::new();
    hdr.write_to(&mut buf).unwrap();
    buf[12] ^= 0xFF;
    let mut cur = Cursor::new(buf);
    assert!(BinaryHeader::read_from(&mut cur).is_err());
}

#[test]
fn header_read_empty_buffer_fails() {
    let mut cur = Cursor::new(Vec::<u8>::new());
    let result = BinaryHeader::read_from(&mut cur);
    assert!(result.is_err());
}

#[test]
fn header_read_incomplete_buffer_fails() {
    for len in 0..BinaryHeader::TOTAL_LEN {
        let buf = vec![0u8; len];
        let mut cur = Cursor::new(buf);
        let result = BinaryHeader::read_from(&mut cur);
        assert!(result.is_err(), "Should fail with buffer length {}", len);
    }
}

#[test]
fn header_write_to_file_and_read_back() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("header.bin");

    let original = BinaryHeader::new(FileKind::SegmentColumn.magic(), 42, 123);
    {
        let mut file = File::create(&path).unwrap();
        original.write_to(&mut file).unwrap();
    }

    let mut file = File::open(&path).unwrap();
    let read = BinaryHeader::read_from(&mut file).unwrap();

    assert_eq!(read.magic, original.magic);
    assert_eq!(read.version, original.version);
    assert_eq!(read.flags, original.flags);
    assert_eq!(read.reserved, original.reserved);
    assert_eq!(read.header_crc32, original.header_crc32);
}

#[test]
fn filekind_all_magic_values() {
    // Test all FileKind variants have unique, correct magic values
    let expected_magics = [
        (FileKind::SegmentColumn, *b"EVDBCOL\0"),
        (FileKind::ZoneOffsets, *b"EVDBZOF\0"),
        (FileKind::ZoneCompressedOffsets, *b"EVDBZCF\0"),
        (FileKind::ZoneMeta, *b"EVDBZON\0"),
        (FileKind::ZoneIndex, *b"EVDBUID\0"),
        (FileKind::XorFilter, *b"EVDBXRF\0"),
        (FileKind::ZoneXorFilter, *b"EVDBZXF\0"),
        (FileKind::ShardSegmentIndex, *b"EVDBSIX\0"),
        (FileKind::SchemaStore, *b"EVDBSCH\0"),
        (FileKind::EnumBitmap, *b"EVDBEBM\0"),
        (FileKind::EventSnapshot, *b"EVDBSNP\0"),
        (FileKind::EventSnapshotMeta, *b"EVDBSMT\0"),
        (FileKind::ZoneSurfFilter, *b"EVDBZSF\0"),
        (FileKind::ZoneRlte, *b"EVDBZRT\0"),
        (FileKind::CalendarDir, *b"EVDBCAL\0"),
        (FileKind::TemporalIndex, *b"EVDBTFI\0"),
        (FileKind::IndexCatalog, *b"EVDBICX\0"),
        (FileKind::MaterializationCatalog, *b"EVDBMCL\0"),
        (FileKind::MaterializationCatalogEntry, *b"EVDBMCE\0"),
        (FileKind::MaterializedManifest, *b"EVDBMMF\0"),
        (FileKind::MaterializedFrame, *b"EVDBMFR\0"),
    ];

    for (kind, expected) in expected_magics {
        assert_eq!(kind.magic(), expected);
    }
}

#[test]
fn materialization_catalog_entry_magic_value() {
    // Specific test for the new MaterializationCatalogEntry file kind
    assert_eq!(FileKind::MaterializationCatalogEntry.magic(), *b"EVDBMCE\0");

    // Verify it's different from other materialization-related file kinds
    assert_ne!(
        FileKind::MaterializationCatalogEntry.magic(),
        FileKind::MaterializationCatalog.magic()
    );
    assert_ne!(
        FileKind::MaterializationCatalogEntry.magic(),
        FileKind::MaterializedManifest.magic()
    );
    assert_ne!(
        FileKind::MaterializationCatalogEntry.magic(),
        FileKind::MaterializedFrame.magic()
    );
}

#[test]
fn magicfile_trait_write_header() {
    struct TestFile;
    impl MagicFile for TestFile {
        const MAGIC: [u8; 8] = *b"TESTFIL\0";
        const VERSION: u16 = 99;
    }

    let mut buf = Vec::new();
    TestFile::write_header(&mut buf).unwrap();

    assert_eq!(buf.len(), BinaryHeader::TOTAL_LEN);

    let mut cur = Cursor::new(buf);
    let header = BinaryHeader::read_from(&mut cur).unwrap();
    assert_eq!(header.magic, *b"TESTFIL\0");
    assert_eq!(header.version, 99);
    assert_eq!(header.flags, 0);
}

#[test]
fn magicfile_trait_read_and_validate_correct() {
    struct TestFile;
    impl MagicFile for TestFile {
        const MAGIC: [u8; 8] = *b"TESTFIL\0";
        const VERSION: u16 = 42;
    }

    let hdr = BinaryHeader::new(*b"TESTFIL\0", 42, 0);
    let mut buf = Vec::new();
    hdr.write_to(&mut buf).unwrap();

    let mut cur = Cursor::new(buf);
    let read = TestFile::read_and_validate_header(&mut cur).unwrap();
    assert_eq!(read.magic, *b"TESTFIL\0");
    assert_eq!(read.version, 42);
}

#[test]
fn magicfile_trait_rejects_wrong_magic() {
    struct TestFile;
    impl MagicFile for TestFile {
        const MAGIC: [u8; 8] = *b"TESTFIL\0";
        const VERSION: u16 = 1;
    }

    let hdr = BinaryHeader::new(*b"WRONGMG\0", 1, 0);
    let mut buf = Vec::new();
    hdr.write_to(&mut buf).unwrap();

    let mut cur = Cursor::new(buf);
    let result = TestFile::read_and_validate_header(&mut cur);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("invalid magic"));
}

#[test]
fn magicfile_trait_rejects_wrong_version() {
    struct TestFile;
    impl MagicFile for TestFile {
        const MAGIC: [u8; 8] = *b"TESTFIL\0";
        const VERSION: u16 = 2;
    }

    let hdr = BinaryHeader::new(*b"TESTFIL\0", 1, 0);
    let mut buf = Vec::new();
    hdr.write_to(&mut buf).unwrap();

    let mut cur = Cursor::new(buf);
    let result = TestFile::read_and_validate_header(&mut cur);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("unsupported version")
    );
}

#[test]
fn magicfile_trait_default_version() {
    struct TestFile;
    impl MagicFile for TestFile {
        const MAGIC: [u8; 8] = *b"TESTDFT\0";
        // Uses default VERSION = 1
    }

    let hdr = BinaryHeader::new(*b"TESTDFT\0", 1, 0);
    let mut buf = Vec::new();
    hdr.write_to(&mut buf).unwrap();

    let mut cur = Cursor::new(buf);
    let read = TestFile::read_and_validate_header(&mut cur).unwrap();
    assert_eq!(read.version, 1);
}

#[test]
fn ensure_header_if_new_creates_for_empty_file() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("new.bin");

    let f = ensure_header_if_new(&path, FileKind::SchemaStore.magic()).unwrap();
    drop(f);

    let len = std::fs::metadata(&path).unwrap().len() as usize;
    assert_eq!(len, BinaryHeader::TOTAL_LEN);

    // Verify header was written correctly
    let mut file = File::open(&path).unwrap();
    let header = BinaryHeader::read_from(&mut file).unwrap();
    assert_eq!(header.magic, FileKind::SchemaStore.magic());
    assert_eq!(header.version, 1);
}

#[test]
fn ensure_header_if_new_validates_existing_header() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("existing.bin");

    // Create file with header
    {
        let mut file = File::create(&path).unwrap();
        let hdr = BinaryHeader::new(FileKind::EventSnapshot.magic(), 1, 0);
        hdr.write_to(&mut file).unwrap();
        file.write_all(&[0x42, 0x43, 0x44]).unwrap();
    }

    // Reopen with same magic should succeed
    let mut f = ensure_header_if_new(&path, FileKind::EventSnapshot.magic()).unwrap();
    let pos = f.seek(SeekFrom::Current(0)).unwrap();
    assert_eq!(pos, BinaryHeader::TOTAL_LEN as u64 + 3); // Header + 3 bytes
}

#[test]
fn ensure_header_if_new_rejects_wrong_magic() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("wrong.bin");

    // Create file with one magic
    {
        let mut file = File::create(&path).unwrap();
        let hdr = BinaryHeader::new(FileKind::ZoneMeta.magic(), 1, 0);
        hdr.write_to(&mut file).unwrap();
    }

    // Try to open with different magic
    let err = ensure_header_if_new(&path, FileKind::XorFilter.magic()).unwrap_err();
    assert!(err.to_string().contains("unexpected magic"));
}

#[test]
fn ensure_header_if_new_seeks_to_end() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("seek.bin");

    // Create and append data
    {
        let mut f = ensure_header_if_new(&path, FileKind::EnumBitmap.magic()).unwrap();
        f.write_all(b"data").unwrap();
    }

    // Reopen and verify cursor is at end
    let mut f = ensure_header_if_new(&path, FileKind::EnumBitmap.magic()).unwrap();
    let pos = f.seek(SeekFrom::Current(0)).unwrap();
    assert_eq!(pos, BinaryHeader::TOTAL_LEN as u64 + 4);
}

#[test]
fn ensure_header_if_new_multiple_file_kinds() {
    let dir = tempdir().unwrap();
    let kinds = [
        FileKind::SegmentColumn,
        FileKind::ZoneOffsets,
        FileKind::ZoneMeta,
        FileKind::XorFilter,
        FileKind::MaterializationCatalogEntry,
    ];

    for (idx, kind) in kinds.iter().enumerate() {
        let path = dir.path().join(format!("file{}.bin", idx));
        let f = ensure_header_if_new(&path, kind.magic()).unwrap();
        drop(f);

        let mut file = File::open(&path).unwrap();
        let header = BinaryHeader::read_from(&mut file).unwrap();
        assert_eq!(header.magic, kind.magic());
    }
}

#[test]
fn open_and_header_offset_file_too_small() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("small.bin");

    // Create file smaller than header
    {
        let mut file = File::create(&path).unwrap();
        file.write_all(&[0x01, 0x02, 0x03]).unwrap();
    }

    let result = open_and_header_offset(&path, FileKind::ZoneIndex.magic());
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("too small"));
}

#[test]
fn open_and_header_offset_exactly_header_size() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("exact.bin");

    {
        let mut file = File::create(&path).unwrap();
        let hdr = BinaryHeader::new(FileKind::ShardSegmentIndex.magic(), 1, 0);
        hdr.write_to(&mut file).unwrap();
    }

    let (_file, offset) =
        open_and_header_offset(&path, FileKind::ShardSegmentIndex.magic()).unwrap();
    assert_eq!(offset, BinaryHeader::TOTAL_LEN);
}

#[test]
fn open_and_header_offset_with_payload() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("payload.bin");

    let payload_data = b"Hello, World!";
    {
        let mut file = File::create(&path).unwrap();
        let hdr = BinaryHeader::new(FileKind::TemporalIndex.magic(), 1, 0);
        hdr.write_to(&mut file).unwrap();
        file.write_all(payload_data).unwrap();
    }

    let (mut file, offset) =
        open_and_header_offset(&path, FileKind::TemporalIndex.magic()).unwrap();
    assert_eq!(offset, BinaryHeader::TOTAL_LEN);

    // Verify we can read payload from offset
    file.seek(SeekFrom::Start(offset as u64)).unwrap();
    let mut buf = vec![0u8; payload_data.len()];
    file.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, payload_data);
}

#[test]
fn open_and_header_offset_corrupted_header() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("corrupt.bin");

    {
        let mut file = File::create(&path).unwrap();
        let hdr = BinaryHeader::new(FileKind::MaterializedManifest.magic(), 1, 0);
        hdr.write_to(&mut file).unwrap();
        // Corrupt CRC
        file.seek(SeekFrom::Start(BinaryHeader::TOTAL_LEN as u64 - 1))
            .unwrap();
        file.write_all(&[0xFF]).unwrap();
    }

    let result = open_and_header_offset(&path, FileKind::MaterializedManifest.magic());
    assert!(result.is_err()); // Should fail due to CRC mismatch
}

#[test]
fn open_and_header_offset_all_file_kinds() {
    let dir = tempdir().unwrap();
    let kinds = [
        FileKind::SegmentColumn,
        FileKind::ZoneCompressedOffsets,
        FileKind::ZoneXorFilter,
        FileKind::MaterializedFrame,
        FileKind::MaterializationCatalogEntry,
    ];

    for (idx, kind) in kinds.iter().enumerate() {
        let path = dir.path().join(format!("open{}.bin", idx));
        {
            let mut file = File::create(&path).unwrap();
            let hdr = BinaryHeader::new(kind.magic(), 1, 0);
            hdr.write_to(&mut file).unwrap();
        }

        let (_file, offset) = open_and_header_offset(&path, kind.magic()).unwrap();
        assert_eq!(offset, BinaryHeader::TOTAL_LEN);
    }
}

#[test]
fn header_clone_and_equality() {
    let hdr1 = BinaryHeader::new(*b"TESTEQ\0\0", 1, 42);
    let hdr2 = hdr1.clone();
    assert_eq!(hdr1, hdr2);

    let hdr3 = BinaryHeader::new(*b"TESTEQ2\0", 1, 43);
    assert_ne!(hdr1, hdr3);
}

#[test]
fn header_debug_format() {
    let hdr = BinaryHeader::new(*b"TESTDBG\0", 1, 0);
    let debug_str = format!("{:?}", hdr);
    // Just verify it doesn't panic and produces some output
    assert!(!debug_str.is_empty());
}

#[test]
fn open_and_header_offset_validates_magic() {
    use std::fs::OpenOptions;
    use std::io::Write;

    let dir = tempdir().unwrap();
    let path = dir.path().join("idx.bin");

    // Write header for ZoneIndex and one byte body
    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        let hdr = BinaryHeader::new(FileKind::ZoneIndex.magic(), 1, 0);
        hdr.write_to(&mut f).unwrap();
        f.write_all(&[0x01]).unwrap();
        f.flush().unwrap();
    }

    // Correct magic -> Ok and offset equals header length
    let (_fh, off) = open_and_header_offset(&path, FileKind::ZoneIndex.magic()).unwrap();
    assert_eq!(off, BinaryHeader::TOTAL_LEN);

    // Wrong magic -> Err
    let err = open_and_header_offset(&path, FileKind::XorFilter.magic()).unwrap_err();
    let msg = format!("{}", err);
    assert!(msg.contains("invalid magic"));
}

#[test]
fn materialization_catalog_entry_header_roundtrip() {
    // Test the new MaterializationCatalogEntry file kind specifically
    let hdr = BinaryHeader::new(FileKind::MaterializationCatalogEntry.magic(), 1, 0);
    let mut buf = Vec::new();
    hdr.write_to(&mut buf).unwrap();
    assert_eq!(buf.len(), BinaryHeader::TOTAL_LEN);

    let mut cur = Cursor::new(buf);
    let read = BinaryHeader::read_from(&mut cur).unwrap();
    assert_eq!(read.magic, FileKind::MaterializationCatalogEntry.magic());
    assert_eq!(read.version, 1);
    assert_eq!(read.flags, 0);
    assert_eq!(read.header_crc32, hdr.header_crc32);
}
