use crc32fast::Hasher as Crc32Hasher;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BinaryHeader {
    pub magic: [u8; 8],
    pub version: u16,
    pub flags: u16,
    pub reserved: u32,
    pub header_crc32: u32,
}

impl BinaryHeader {
    pub const LEN_WITHOUT_CRC: usize = 8 + 2 + 2 + 4;
    pub const TOTAL_LEN: usize = Self::LEN_WITHOUT_CRC + 4;

    pub fn new(magic: [u8; 8], version: u16, flags: u16) -> Self {
        let mut header = Self {
            magic,
            version,
            flags,
            reserved: 0,
            header_crc32: 0,
        };
        header.header_crc32 = header.compute_crc32();
        header
    }

    fn compute_crc32(&self) -> u32 {
        let mut hasher = Crc32Hasher::new();
        hasher.update(&self.magic);
        hasher.update(&self.version.to_le_bytes());
        hasher.update(&self.flags.to_le_bytes());
        hasher.update(&self.reserved.to_le_bytes());
        hasher.finalize()
    }

    pub fn write_to<W: Write>(&self, mut w: W) -> std::io::Result<()> {
        w.write_all(&self.magic)?;
        w.write_all(&self.version.to_le_bytes())?;
        w.write_all(&self.flags.to_le_bytes())?;
        w.write_all(&self.reserved.to_le_bytes())?;
        w.write_all(&self.header_crc32.to_le_bytes())?;
        Ok(())
    }

    pub fn read_from<R: Read>(mut r: R) -> std::io::Result<Self> {
        let mut magic = [0u8; 8];
        r.read_exact(&mut magic)?;

        let mut v = [0u8; 2];
        r.read_exact(&mut v)?;
        let version = u16::from_le_bytes(v);

        let mut f = [0u8; 2];
        r.read_exact(&mut f)?;
        let flags = u16::from_le_bytes(f);

        let mut res = [0u8; 4];
        r.read_exact(&mut res)?;
        let reserved = u32::from_le_bytes(res);

        let mut c = [0u8; 4];
        r.read_exact(&mut c)?;
        let header_crc32 = u32::from_le_bytes(c);

        let hdr = Self {
            magic,
            version,
            flags,
            reserved,
            header_crc32,
        };
        let expected = hdr.compute_crc32();
        if expected != header_crc32 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "header CRC mismatch",
            ));
        }
        Ok(hdr)
    }
}

pub trait MagicFile {
    const MAGIC: [u8; 8];
    const VERSION: u16 = 1;

    fn write_header<W: Write>(writer: &mut W) -> std::io::Result<()> {
        let header = BinaryHeader::new(Self::MAGIC, Self::VERSION, 0);
        header.write_to(writer)
    }

    fn read_and_validate_header<R: Read>(reader: &mut R) -> std::io::Result<BinaryHeader> {
        let header = BinaryHeader::read_from(reader)?;
        if header.magic != Self::MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid magic",
            ));
        }
        if header.version != Self::VERSION {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unsupported version",
            ));
        }
        Ok(header)
    }
}

pub enum FileKind {
    SegmentColumn,
    ZoneOffsets,
    ZoneMeta,
    ZoneIndex,
    XorFilter,
    ShardSegmentIndex,
    SchemaStore,
    EnumBitmap,
    EventSnapshot,
    EventSnapshotMeta,
    ZoneSurfFilter,
}

impl FileKind {
    pub const fn magic(&self) -> [u8; 8] {
        match self {
            FileKind::SegmentColumn => *b"EVDBCOL\0",
            FileKind::ZoneOffsets => *b"EVDBZOF\0",
            FileKind::ZoneMeta => *b"EVDBZON\0",
            FileKind::ZoneIndex => *b"EVDBUID\0",
            FileKind::XorFilter => *b"EVDBXRF\0",
            FileKind::ShardSegmentIndex => *b"EVDBSIX\0",
            FileKind::SchemaStore => *b"EVDBSCH\0",
            FileKind::EnumBitmap => *b"EVDBEBM\0",
            FileKind::EventSnapshot => *b"EVDBSNP\0",
            FileKind::EventSnapshotMeta => *b"EVDBSMT\0",
            FileKind::ZoneSurfFilter => *b"EVDBZSF\0",
        }
    }
}

pub fn ensure_header_if_new(path: &Path, expected_magic: [u8; 8]) -> std::io::Result<File> {
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(path)?;

    let len = file.metadata()?.len();
    if len == 0 {
        let header = BinaryHeader::new(expected_magic, 1, 0);
        header.write_to(&mut file)?;
    } else {
        // Require valid header for any non-empty file
        file.seek(SeekFrom::Start(0))?;
        let header = BinaryHeader::read_from(&mut file)?;
        if header.magic != expected_magic {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unexpected magic in existing file",
            ));
        }
    }

    file.seek(SeekFrom::End(0))?;
    Ok(file)
}

pub fn open_and_header_offset(
    path: &Path,
    expected_magic: [u8; 8],
) -> std::io::Result<(File, usize)> {
    let mut file = File::open(path)?;
    let len = file.metadata()?.len();
    if len < BinaryHeader::TOTAL_LEN as u64 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "file too small for header",
        ));
    }
    file.seek(SeekFrom::Start(0))?;
    let header = BinaryHeader::read_from(&mut file)?;
    if header.magic != expected_magic {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid magic",
        ));
    }
    Ok((file, BinaryHeader::TOTAL_LEN))
}
