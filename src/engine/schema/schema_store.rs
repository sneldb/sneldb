use crate::engine::schema::errors::SchemaError;
use crate::engine::schema::registry::SchemaRecord;
use crate::shared::storage_header::{BinaryHeader, FileKind};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use tracing::warn;

#[derive(Debug, Clone, PartialEq)]
pub struct SchemaStore {
    file_path: PathBuf,
}

impl SchemaStore {
    pub fn new(path: PathBuf) -> Result<Self, SchemaError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| SchemaError::IoWriteFailed(format!("Creating dir: {}", e)))?;
        }
        Ok(Self { file_path: path })
    }

    pub fn append(&self, record: &SchemaRecord) -> Result<(), SchemaError> {
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.file_path)
            .map_err(|e| SchemaError::IoWriteFailed(e.to_string()))?;

        // If file is empty, write header first
        if file
            .metadata()
            .map_err(|e| SchemaError::IoWriteFailed(e.to_string()))?
            .len()
            == 0
        {
            let header = BinaryHeader::new(FileKind::SchemaStore.magic(), 1, 0);
            header
                .write_to(&mut file)
                .map_err(|e| SchemaError::IoWriteFailed(e.to_string()))?;
        }

        let encoded = bincode::serialize(record)
            .map_err(|e| SchemaError::SerializationFailed(e.to_string()))?;

        file.write_all(&(encoded.len() as u32).to_le_bytes())
            .map_err(|e| SchemaError::IoWriteFailed(e.to_string()))?;

        file.write_all(&encoded)
            .map_err(|e| SchemaError::IoWriteFailed(e.to_string()))?;

        Ok(())
    }

    pub fn load(&self) -> Result<Vec<SchemaRecord>, SchemaError> {
        let mut file = match File::open(&self.file_path) {
            Ok(f) => f,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
            Err(e) => return Err(SchemaError::IoReadFailed(e.to_string())),
        };

        let mut records = vec![];

        // If the file is empty, return no records
        if file
            .metadata()
            .map_err(|e| SchemaError::IoReadFailed(e.to_string()))?
            .len()
            == 0
        {
            return Ok(records);
        }

        // Require header if non-empty; if header is corrupt/missing, treat as empty to keep registry resilient.
        match BinaryHeader::read_from(&mut file) {
            Ok(header) => {
                if header.magic != FileKind::SchemaStore.magic() {
                    return Err(SchemaError::IoReadFailed(
                        "invalid magic for schemas.bin".into(),
                    ));
                }
            }
            Err(_) => {
                // Corrupt or legacy file: ignore contents for resilience
                return Ok(records);
            }
        }

        loop {
            let mut len_buf = [0u8; 4];
            if let Err(e) = file.read_exact(&mut len_buf) {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    // Truncated length prefix — end of valid data
                    break;
                } else {
                    // Unexpected read failure
                    return Err(SchemaError::IoReadFailed(e.to_string()));
                }
            }

            let len = u32::from_le_bytes(len_buf);
            if len > 10 * 1024 {
                warn!(
                    "Warning: Skipping schema with length too large: {} bytes",
                    len
                );
                break;
            }

            let mut buf = vec![0u8; len as usize];
            if let Err(e) = file.read_exact(&mut buf) {
                warn!(
                    "Warning: Skipping corrupted schema: failed to read full record: {}",
                    e
                );
                break;
            }

            match bincode::deserialize::<SchemaRecord>(&buf) {
                Ok(record) => records.push(record),
                Err(e) => {
                    warn!(
                        "Warning: Skipping corrupted schema: failed to deserialize: {}",
                        e
                    );
                    break;
                }
            }
        }

        Ok(records)
    }
}
