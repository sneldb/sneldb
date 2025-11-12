use crate::engine::core::WalEntry;
use crate::shared::time;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use tracing::{debug, error, info};

/// Archive format version for future compatibility
pub const ARCHIVE_VERSION: u8 = 1;

/// Header metadata for archived WAL files
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalArchiveHeader {
    /// Format version (currently 1)
    pub version: u8,
    /// Shard ID this archive belongs to
    pub shard_id: usize,
    /// Original WAL log ID
    pub log_id: u64,
    /// Timestamp of first entry
    pub start_timestamp: u64,
    /// Timestamp of last entry
    pub end_timestamp: u64,
    /// Total number of entries in archive
    pub entry_count: u64,
    /// Compression algorithm used ("zstd", "lz4", etc.)
    pub compression: String,
    /// Compression level used
    pub compression_level: i32,
    /// Timestamp when archive was created
    pub created_at: u64,
}

impl WalArchiveHeader {
    /// Create a new archive header
    pub fn new(
        shard_id: usize,
        log_id: u64,
        entry_count: u64,
        start_timestamp: u64,
        end_timestamp: u64,
        compression: String,
        compression_level: i32,
    ) -> Self {
        Self {
            version: ARCHIVE_VERSION,
            shard_id,
            log_id,
            start_timestamp,
            end_timestamp,
            entry_count,
            compression,
            compression_level,
            created_at: time::now(),
        }
    }
}

/// Body of the archive containing all WAL entries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalArchiveBody {
    pub entries: Vec<WalEntry>,
}

impl WalArchiveBody {
    pub fn new(entries: Vec<WalEntry>) -> Self {
        Self { entries }
    }
}

/// Complete WAL archive structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalArchive {
    pub header: WalArchiveHeader,
    pub body: WalArchiveBody,
}

impl WalArchive {
    /// Create a new archive from a WAL log file
    pub fn from_wal_file(
        wal_path: &Path,
        shard_id: usize,
        log_id: u64,
        compression: String,
        compression_level: i32,
    ) -> std::io::Result<Self> {
        info!(
            target: "wal_archive::from_wal_file",
            shard_id, log_id, ?wal_path,
            "Reading WAL file for archiving"
        );

        let file = File::open(wal_path)?;
        let reader = BufReader::new(file);
        let mut entries = Vec::new();
        let mut start_timestamp = u64::MAX;
        let mut end_timestamp = 0u64;

        for (line_num, line) in reader.lines().enumerate() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }

            match serde_json::from_str::<WalEntry>(&line) {
                Ok(entry) => {
                    start_timestamp = start_timestamp.min(entry.timestamp);
                    end_timestamp = end_timestamp.max(entry.timestamp);
                    entries.push(entry);
                }
                Err(e) => {
                    error!(
                        target: "wal_archive::from_wal_file",
                        shard_id, log_id, line_num, error = %e,
                        "Failed to parse WAL entry, skipping"
                    );
                }
            }
        }

        let entry_count = entries.len() as u64;

        // Handle empty files
        if entry_count == 0 {
            start_timestamp = 0;
        }

        let header = WalArchiveHeader::new(
            shard_id,
            log_id,
            entry_count,
            start_timestamp,
            end_timestamp,
            compression,
            compression_level,
        );

        let body = WalArchiveBody::new(entries);

        debug!(
            target: "wal_archive::from_wal_file",
            shard_id, log_id, entry_count,
            "Loaded WAL entries for archiving"
        );

        Ok(Self { header, body })
    }

    /// Serialize and compress the archive to bytes
    pub fn to_compressed_bytes(&self) -> std::io::Result<Vec<u8>> {
        debug!(
            target: "wal_archive::to_compressed_bytes",
            shard_id = self.header.shard_id,
            log_id = self.header.log_id,
            entry_count = self.header.entry_count,
            "Serializing archive with MessagePack"
        );

        // Serialize to MessagePack
        let serialized = rmp_serde::to_vec(self).map_err(|e| {
            error!(
                target: "wal_archive::to_compressed_bytes",
                error = %e,
                "MessagePack serialization failed"
            );
            std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
        })?;

        debug!(
            target: "wal_archive::to_compressed_bytes",
            serialized_size = serialized.len(),
            "MessagePack serialization complete"
        );

        // Compress with zstd
        let compression_level = self.header.compression_level;
        let compressed = zstd::encode_all(&serialized[..], compression_level).map_err(|e| {
            error!(
                target: "wal_archive::to_compressed_bytes",
                error = %e,
                "Zstd compression failed"
            );
            e
        })?;

        info!(
            target: "wal_archive::to_compressed_bytes",
            shard_id = self.header.shard_id,
            log_id = self.header.log_id,
            original_size = serialized.len(),
            compressed_size = compressed.len(),
            ratio = format!("{:.2}%", (compressed.len() as f64 / serialized.len() as f64) * 100.0),
            "Archive compressed successfully"
        );

        Ok(compressed)
    }

    /// Decompress and deserialize archive from bytes
    pub fn from_compressed_bytes(compressed: &[u8]) -> std::io::Result<Self> {
        debug!(
            target: "wal_archive::from_compressed_bytes",
            compressed_size = compressed.len(),
            "Decompressing archive"
        );

        // Decompress with zstd
        let decompressed = zstd::decode_all(compressed)?;

        debug!(
            target: "wal_archive::from_compressed_bytes",
            decompressed_size = decompressed.len(),
            "Decompression complete, deserializing MessagePack"
        );

        // Deserialize from MessagePack
        let archive: Self = rmp_serde::from_slice(&decompressed).map_err(|e| {
            error!(
                target: "wal_archive::from_compressed_bytes",
                error = %e,
                "MessagePack deserialization failed"
            );
            std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
        })?;

        info!(
            target: "wal_archive::from_compressed_bytes",
            shard_id = archive.header.shard_id,
            log_id = archive.header.log_id,
            entry_count = archive.header.entry_count,
            "Archive deserialized successfully"
        );

        Ok(archive)
    }

    /// Generate the archive filename based on header metadata
    pub fn generate_filename(&self) -> String {
        format!(
            "wal-{:05}-{}-{}.wal.zst",
            self.header.log_id, self.header.start_timestamp, self.header.end_timestamp
        )
    }

    /// Write the archive to a file
    pub fn write_to_file(&self, archive_dir: &Path) -> std::io::Result<PathBuf> {
        std::fs::create_dir_all(archive_dir)?;

        let filename = self.generate_filename();
        let archive_path = archive_dir.join(&filename);

        info!(
            target: "wal_archive::write_to_file",
            shard_id = self.header.shard_id,
            log_id = self.header.log_id,
            ?archive_path,
            "Writing archive to file"
        );

        let compressed = self.to_compressed_bytes()?;
        let mut file = File::create(&archive_path)?;
        file.write_all(&compressed)?;
        file.sync_all()?;

        info!(
            target: "wal_archive::write_to_file",
            shard_id = self.header.shard_id,
            log_id = self.header.log_id,
            ?archive_path,
            file_size = compressed.len(),
            "Archive written successfully"
        );

        Ok(archive_path)
    }

    /// Read an archive from a file
    pub fn read_from_file(archive_path: &Path) -> std::io::Result<Self> {
        info!(
            target: "wal_archive::read_from_file",
            ?archive_path,
            "Reading archive from file"
        );

        let mut file = File::open(archive_path)?;
        let mut compressed = Vec::new();
        file.read_to_end(&mut compressed)?;

        Self::from_compressed_bytes(&compressed)
    }
}
