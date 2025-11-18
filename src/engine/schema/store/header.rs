use crate::engine::schema::errors::SchemaError;
use crate::engine::schema::store::types::{SCHEMA_STORE_VERSION, SchemaStoreDiagnostics};
use crate::shared::storage_header::{BinaryHeader, FileKind};
use std::fs::File;

/// Checks if file is empty.
pub fn is_file_empty(file: &File) -> Result<bool, SchemaError> {
    file.metadata()
        .map_err(|e| SchemaError::IoReadFailed(e.to_string()))
        .map(|m| m.len() == 0)
}

/// Validates and reads the header from the file.
/// For diagnostics mode, returns Ok(None) on errors and records them in diagnostics.
/// For load mode, returns errors directly.
pub fn read_and_validate_header(
    file: &mut File,
    mut diagnostics: Option<&mut SchemaStoreDiagnostics>,
) -> Result<Option<BinaryHeader>, SchemaError> {
    let header = match BinaryHeader::read_from(file) {
        Ok(header) => header,
        Err(e) => {
            if let Some(diag) = diagnostics.as_mut() {
                diag.issues
                    .push(format!("failed to read schema store header: {}", e));
                return Ok(None);
            }
            return Err(SchemaError::IoReadFailed(format!(
                "failed to read schema store header: {}",
                e
            )));
        }
    };

    if header.magic != FileKind::SchemaStore.magic() {
        let msg = "invalid magic for schemas.bin";
        if let Some(diag) = diagnostics.as_mut() {
            diag.issues.push(msg.into());
            return Ok(None);
        }
        return Err(SchemaError::IoReadFailed(msg.into()));
    }

    if let Some(diag) = diagnostics.as_mut() {
        diag.version = Some(header.version);
    }

    if header.version != SCHEMA_STORE_VERSION {
        let msg = format!("unsupported schema store version: {}", header.version);
        if let Some(diag) = diagnostics.as_mut() {
            diag.issues.push(msg.clone());
            return Ok(None);
        }
        return Err(SchemaError::IoReadFailed(msg));
    }

    Ok(Some(header))
}

/// Writes the header if the file is empty.
pub fn ensure_header(file: &mut File) -> Result<(), SchemaError> {
    if is_file_empty(file)? {
        let header = BinaryHeader::new(FileKind::SchemaStore.magic(), SCHEMA_STORE_VERSION, 0);
        header
            .write_to(file)
            .map_err(|e| SchemaError::IoWriteFailed(e.to_string()))?;
    }
    Ok(())
}
