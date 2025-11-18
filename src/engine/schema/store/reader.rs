use crate::engine::schema::errors::SchemaError;
use crate::engine::schema::registry::SchemaRecord;
use crate::engine::schema::store::types::{
    MAX_RECORD_LEN_BYTES, RecordReadResult, SchemaStoreDiagnostics,
};
use crate::engine::schema::store::writer::compute_crc32;
use crate::shared::storage_header::BinaryHeader;
use std::fs::File;
use std::io::Read;
use tracing::warn;

/// Records a skipped record in diagnostics and logs a warning.
pub fn record_skipped_record(
    diagnostics: &mut Option<&mut SchemaStoreDiagnostics>,
    offset: u64,
    reason: &str,
) {
    if let Some(diag) = diagnostics.as_mut() {
        diag.skipped_records += 1;
        diag.issues
            .push(format!("skipping schema at offset {}: {}", offset, reason));
    }
    warn!("Warning: Skipping schema at offset {}: {}", offset, reason);
}

/// Reads a u32 from the file, updating offset.
/// Returns Ok(Some(value)) on success, Ok(None) on EOF, Err on other errors.
pub fn read_u32(file: &mut File, offset: &mut u64) -> Result<Option<u32>, SchemaError> {
    let mut buf = [0u8; 4];
    match file.read_exact(&mut buf) {
        Ok(()) => {
            *offset += 4;
            Ok(Some(u32::from_le_bytes(buf)))
        }
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(SchemaError::IoReadFailed(e.to_string())),
    }
}

/// Reads a single record from the file.
pub fn read_single_record(
    file: &mut File,
    offset: &mut u64,
    diagnostics: &mut Option<&mut SchemaStoreDiagnostics>,
) -> Result<RecordReadResult, SchemaError> {
    // Read record length
    let len = match read_u32(file, offset) {
        Ok(Some(len)) => len,
        Ok(None) => return Ok(RecordReadResult::Eof),
        Err(e) => return Err(e),
    };

    if len > MAX_RECORD_LEN_BYTES {
        record_skipped_record(
            diagnostics,
            *offset - 4,
            &format!("length too large: {} bytes", len),
        );
        return Ok(RecordReadResult::Eof); // Stop reading on invalid length
    }

    // Read CRC
    let expected_crc = match read_u32(file, offset) {
        Ok(Some(crc)) => crc,
        Ok(None) => {
            record_skipped_record(diagnostics, *offset - 4, "failed to read CRC");
            return Ok(RecordReadResult::Eof);
        }
        Err(e) => {
            record_skipped_record(
                diagnostics,
                *offset - 4,
                &format!("failed to read CRC: {}", e),
            );
            return Err(e);
        }
    };

    // Read record data
    let record_start = *offset;
    let mut buf = vec![0u8; len as usize];
    if let Err(e) = file.read_exact(&mut buf) {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            record_skipped_record(
                diagnostics,
                record_start - 8,
                "failed to read full record: unexpected EOF",
            );
            return Ok(RecordReadResult::Eof);
        }
        record_skipped_record(
            diagnostics,
            record_start - 8,
            &format!("failed to read full record: {}", e),
        );
        return Err(SchemaError::IoReadFailed(e.to_string()));
    }
    *offset += len as u64;

    // Verify CRC
    let actual_crc = compute_crc32(&buf);
    if actual_crc != expected_crc {
        record_skipped_record(
            diagnostics,
            record_start - 8,
            &format!(
                "CRC mismatch (expected {}, got {})",
                expected_crc, actual_crc
            ),
        );
        return Ok(RecordReadResult::Corrupted);
    }

    // Deserialize record
    match bincode::deserialize::<SchemaRecord>(&buf) {
        Ok(record) => Ok(RecordReadResult::Valid(record)),
        Err(e) => {
            record_skipped_record(
                diagnostics,
                record_start - 8,
                &format!("failed to deserialize: {}", e),
            );
            Ok(RecordReadResult::Corrupted)
        }
    }
}

/// Reads all records from the file.
pub fn read_records(
    file: &mut File,
    records: &mut Vec<SchemaRecord>,
    mut diagnostics: Option<&mut SchemaStoreDiagnostics>,
) -> Result<(), SchemaError> {
    let mut offset = BinaryHeader::TOTAL_LEN as u64;
    loop {
        match read_single_record(file, &mut offset, &mut diagnostics)? {
            RecordReadResult::Valid(record) => records.push(record),
            RecordReadResult::Corrupted => continue, // Skip corrupted record, try next
            RecordReadResult::Eof => break,          // End of file, stop reading
        }
    }
    Ok(())
}
