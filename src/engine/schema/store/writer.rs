use crate::engine::schema::errors::SchemaError;
use crate::engine::schema::registry::SchemaRecord;
use crc32fast::Hasher as Crc32Hasher;
use std::fs::File;
use std::io::Write;

/// Computes CRC32 for the given data.
pub fn compute_crc32(data: &[u8]) -> u32 {
    let mut hasher = Crc32Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

/// Writes a record to the file.
pub fn write_record(file: &mut File, record: &SchemaRecord) -> Result<(), SchemaError> {
    let encoded =
        bincode::serialize(record).map_err(|e| SchemaError::SerializationFailed(e.to_string()))?;

    file.write_all(&(encoded.len() as u32).to_le_bytes())
        .map_err(|e| SchemaError::IoWriteFailed(e.to_string()))?;

    let crc = compute_crc32(&encoded);
    file.write_all(&crc.to_le_bytes())
        .map_err(|e| SchemaError::IoWriteFailed(e.to_string()))?;

    file.write_all(&encoded)
        .map_err(|e| SchemaError::IoWriteFailed(e.to_string()))?;

    Ok(())
}
