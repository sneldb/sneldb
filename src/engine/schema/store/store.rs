use crate::engine::schema::errors::SchemaError;
use crate::engine::schema::registry::SchemaRecord;
use crate::engine::schema::store::guard::FileLockGuard;
use crate::engine::schema::store::header::{
    ensure_header, is_file_empty, read_and_validate_header,
};
use crate::engine::schema::store::reader::read_records;
use crate::engine::schema::store::types::{SchemaStoreDiagnostics, SchemaStoreOptions};
use crate::engine::schema::store::writer::write_record;
use std::fs::{File, OpenOptions};
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq)]
pub struct SchemaStore {
    file_path: PathBuf,
    options: SchemaStoreOptions,
}

impl SchemaStore {
    pub fn new(path: PathBuf) -> Result<Self, SchemaError> {
        Self::new_with_options(path, SchemaStoreOptions::default())
    }

    pub fn new_with_options(
        path: PathBuf,
        options: SchemaStoreOptions,
    ) -> Result<Self, SchemaError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| SchemaError::IoWriteFailed(format!("Creating dir: {}", e)))?;
        }
        Ok(Self {
            file_path: path,
            options,
        })
    }

    /// Opens file for reading, returns None if file doesn't exist.
    fn open_file_for_read(&self) -> Result<Option<File>, SchemaError> {
        match File::open(&self.file_path) {
            Ok(f) => Ok(Some(f)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(SchemaError::IoReadFailed(e.to_string())),
        }
    }

    pub fn append(&self, record: &SchemaRecord) -> Result<(), SchemaError> {
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.file_path)
            .map_err(|e| SchemaError::IoWriteFailed(e.to_string()))?;

        let guard = FileLockGuard::new_exclusive(file)?;
        let file_mut = guard.get_mut();
        ensure_header(file_mut)?;
        write_record(file_mut, record)?;

        if self.options.fsync {
            file_mut
                .sync_all()
                .map_err(|e| SchemaError::IoWriteFailed(e.to_string()))?;
        }

        Ok(())
    }

    pub fn load(&self) -> Result<Vec<SchemaRecord>, SchemaError> {
        let Some(file) = self.open_file_for_read()? else {
            return Ok(vec![]);
        };

        let guard = FileLockGuard::new_shared(file)?;
        let file_mut = guard.get_mut();

        if is_file_empty(file_mut)? {
            return Ok(vec![]);
        }

        read_and_validate_header(file_mut, None)?;
        let mut records = vec![];
        read_records(file_mut, &mut records, None)?;
        Ok(records)
    }

    pub fn diagnose(&self) -> Result<SchemaStoreDiagnostics, SchemaError> {
        let Some(file) = self.open_file_for_read()? else {
            return Ok(SchemaStoreDiagnostics::default());
        };

        let guard = FileLockGuard::new_shared(file)?;
        let file_mut = guard.get_mut();
        let mut diagnostics = SchemaStoreDiagnostics::default();

        if is_file_empty(file_mut)? {
            return Ok(diagnostics);
        }

        if read_and_validate_header(file_mut, Some(&mut diagnostics))?.is_none() {
            return Ok(diagnostics);
        }

        let mut records = Vec::new();
        read_records(file_mut, &mut records, Some(&mut diagnostics))?;
        diagnostics.valid_records = records.len();
        Ok(diagnostics)
    }

    pub fn repair_to(&self, out_path: PathBuf) -> Result<SchemaStoreDiagnostics, SchemaError> {
        if out_path == self.file_path {
            return Err(SchemaError::IoWriteFailed(
                "repair output path must differ from source".into(),
            ));
        }

        let Some(file) = self.open_file_for_read()? else {
            return Ok(SchemaStoreDiagnostics::default());
        };

        let guard = FileLockGuard::new_shared(file)?;
        let file_mut = guard.get_mut();
        let mut diagnostics = SchemaStoreDiagnostics::default();

        if is_file_empty(file_mut)? {
            return Ok(diagnostics);
        }

        if read_and_validate_header(file_mut, Some(&mut diagnostics))?.is_none() {
            return Ok(diagnostics);
        }

        let mut records = Vec::new();
        read_records(file_mut, &mut records, Some(&mut diagnostics))?;
        diagnostics.valid_records = records.len();

        let _ = std::fs::remove_file(&out_path);
        let writer =
            SchemaStore::new_with_options(out_path.clone(), SchemaStoreOptions { fsync: true })?;
        for record in records {
            writer.append(&record)?;
        }

        Ok(diagnostics)
    }
}
