use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::engine::materialize::MaterializationError;
use crate::engine::materialize::catalog::RetentionPolicy;
use crate::shared::storage_header::{BinaryHeader, FileKind};

use super::frame::StoredFrameMeta;

pub const MANIFEST_VERSION: u16 = 1;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ManifestState {
    frames: Vec<StoredFrameMeta>,
    next_frame_index: u64,
    retention: Option<RetentionPolicy>,
}

impl ManifestState {
    pub fn frames(&self) -> &[StoredFrameMeta] {
        &self.frames
    }

    pub fn push_frame(&mut self, meta: StoredFrameMeta) {
        self.frames.push(meta);
    }

    pub fn next_frame_index(&self) -> u64 {
        self.next_frame_index
    }

    pub fn bump_frame_index(&mut self) {
        self.next_frame_index = self.next_frame_index.saturating_add(1);
    }

    pub fn retention_policy(&self) -> Option<&RetentionPolicy> {
        self.retention.as_ref()
    }

    pub fn set_retention_policy(&mut self, policy: RetentionPolicy) {
        self.retention = Some(policy);
    }

    pub fn clear_retention_policy(&mut self) {
        self.retention = None;
    }

    pub fn remove_indices(&mut self, indices: &[usize]) {
        for &idx in indices.iter().rev() {
            if idx < self.frames.len() {
                self.frames.remove(idx);
            }
        }
        if self.next_frame_index < self.frames.len() as u64 {
            self.next_frame_index = self.frames.len() as u64;
        }
    }

    pub fn ensure_next_index(&mut self) {
        if self.next_frame_index < self.frames.len() as u64 {
            self.next_frame_index = self.frames.len() as u64;
        }
    }
}

#[derive(Debug)]
pub struct ManifestStore {
    path: PathBuf,
}

impl ManifestStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<(Self, ManifestState), MaterializationError> {
        let path = path.into();
        let state = if path.exists() {
            load_manifest(&path)?
        } else {
            ManifestState::default()
        };

        Ok((Self { path }, state))
    }

    pub fn persist(&self, state: &ManifestState) -> Result<(), MaterializationError> {
        persist_manifest(&self.path, state)
    }
}

fn load_manifest(path: &Path) -> Result<ManifestState, MaterializationError> {
    let mut file = File::open(path)?;
    let header = BinaryHeader::read_from(&mut file)?;
    if header.magic != FileKind::MaterializedManifest.magic() {
        return Err(MaterializationError::Header(
            "Invalid manifest magic".into(),
        ));
    }
    if header.version != MANIFEST_VERSION {
        return Err(MaterializationError::Header(format!(
            "Unsupported manifest version {}",
            header.version
        )));
    }

    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;
    if buf.is_empty() {
        return Ok(ManifestState::default());
    }

    let mut state: ManifestState = bincode::deserialize(&buf)?;
    state.ensure_next_index();
    Ok(state)
}

fn persist_manifest(path: &Path, state: &ManifestState) -> Result<(), MaterializationError> {
    let mut tmp = path.to_path_buf();
    tmp.set_extension("tmp");

    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp)?;

    BinaryHeader::new(FileKind::MaterializedManifest.magic(), MANIFEST_VERSION, 0)
        .write_to(&mut file)
        .map_err(|e| MaterializationError::Header(e.to_string()))?;

    let mut frames = state.frames.clone();
    frames.sort_by(|a, b| a.file_name.cmp(&b.file_name));

    let serialized = bincode::serialize(&ManifestState {
        frames,
        next_frame_index: state.next_frame_index,
        retention: state.retention.clone(),
    })?;

    file.write_all(&serialized)?;
    file.flush()?;
    file.sync_all()?;

    fs::rename(&tmp, path)?;
    Ok(())
}
