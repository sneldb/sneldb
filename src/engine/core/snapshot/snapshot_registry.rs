use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use tracing::{info, warn};

use crate::engine::core::Event;
use crate::engine::core::snapshot::snapshot_meta::SnapshotMeta;
use crate::engine::core::snapshot::snapshot_meta_reader::SnapshotMetaReader;
use crate::engine::core::snapshot::snapshot_meta_writer::SnapshotMetaWriter;
use crate::engine::core::snapshot::snapshot_writer::SnapshotWriter;
use crate::engine::errors::StoreError;

/// Snapshot key is uid + context_id only.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SnapshotKey {
    pub uid: String,
    pub context_id: String,
}

impl SnapshotKey {
    pub fn new(uid: impl Into<String>, context_id: impl Into<String>) -> Self {
        Self {
            uid: uid.into(),
            context_id: context_id.into(),
        }
    }

    pub fn context_id(&self) -> &str {
        &self.context_id
    }
}

/// Maintains a mapping from `(uid, context_id)` to a single snapshot covering [from_ts, to_ts].
/// Persisted via a single `.smt` file in `base_dir` and separate `.snp` files per key.
#[derive(Debug)]
pub struct SnapshotRegistry {
    base_dir: PathBuf,
    // Mapping: (uid, context_id) -> SnapshotMeta
    by_key: HashMap<(String, String), SnapshotMeta>,
}

impl SnapshotRegistry {
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
            by_key: HashMap::new(),
        }
    }

    fn meta_path(&self) -> PathBuf {
        self.base_dir.join("registry.smt")
    }

    fn data_filename(uid: &str, context_id: &str) -> String {
        // simple and safe file name scheme based on uid + context_id
        format!("{}__{}.snp", sanitize(uid), sanitize(context_id))
    }

    pub fn ensure_dirs(&self) -> Result<(), StoreError> {
        if !self.base_dir.exists() {
            fs::create_dir_all(&self.base_dir)?;
        }
        Ok(())
    }

    /// Load snapshot metas from the registry `.smt` file in `base_dir`.
    pub fn load(&mut self) -> Result<(), StoreError> {
        self.ensure_dirs()?;
        let meta_path = self.meta_path();
        if !meta_path.exists() {
            info!(target = "snapshot_registry::load", path = %meta_path.display(), "No registry file yet");
            self.by_key.clear();
            return Ok(());
        }

        let metas = SnapshotMetaReader::new(&meta_path).read_all()?;
        self.by_key.clear();
        for m in metas.into_iter() {
            let key = (m.uid.clone(), m.context_id.clone());
            self.by_key.insert(key, m);
        }

        info!(
            target = "snapshot_registry::load",
            count = self.by_key.len(),
            "Loaded snapshot registry entries"
        );
        Ok(())
    }

    /// Save current metas to the `.smt` file atomically.
    pub fn save(&self) -> Result<(), StoreError> {
        self.ensure_dirs()?;
        let metas: Vec<SnapshotMeta> = self.by_key.values().cloned().collect();
        SnapshotMetaWriter::new(&self.meta_path()).write_all(&metas)
    }

    /// Returns the snapshot meta for a key if exists.
    pub fn get_meta(&self, key: &SnapshotKey) -> Option<&SnapshotMeta> {
        self.by_key.get(&(key.uid.clone(), key.context_id.clone()))
    }

    /// Returns the snapshot data path for a key if exists.
    pub fn get_data_path(&self, key: &SnapshotKey) -> Option<PathBuf> {
        if self
            .by_key
            .contains_key(&(key.uid.clone(), key.context_id.clone()))
        {
            Some(
                self.base_dir
                    .join(Self::data_filename(&key.uid, &key.context_id)),
            )
        } else {
            None
        }
    }

    /// Insert or replace a snapshot for the given key. This will overwrite previous meta and point to the new `.snp` path.
    pub fn upsert(
        &mut self,
        key: SnapshotKey,
        from_ts: u64,
        to_ts: u64,
    ) -> Result<PathBuf, StoreError> {
        self.ensure_dirs()?;

        let id = key.uid.clone();
        let context_id = key.context_id.clone();

        let data_path = self.base_dir.join(Self::data_filename(&id, &context_id));
        let meta = SnapshotMeta::new(id.clone(), context_id.clone(), from_ts, to_ts);
        self.by_key.insert((id, context_id), meta);
        Ok(data_path)
    }

    /// Atomically refresh the snapshot for the given uid+context key with the provided events and time range.
    /// Writes to a temporary file and then renames over the target `.snp`.
    pub fn refresh_snapshot(
        &mut self,
        key: SnapshotKey,
        from_ts: u64,
        to_ts: u64,
        events: &[Event],
    ) -> Result<(), StoreError> {
        self.ensure_dirs()?;

        // Determine final path and temp path
        let id = key.uid.clone();
        let ctx = key.context_id.clone();
        let final_path = self.base_dir.join(Self::data_filename(&id, &ctx));
        let tmp_path = final_path.with_extension("snp.tmp");

        // Write to temp
        SnapshotWriter::new(&tmp_path).write_all(events)?;

        // Rename atomically
        fs::rename(&tmp_path, &final_path)?;

        // Update meta and save registry
        self.upsert(key, from_ts, to_ts)?;
        self.save()?;
        Ok(())
    }

    /// Decide how to handle REPLAY SINCE `t` based on the snapshot.
    /// Returns a strategy:
    /// - UseSnapshot(from_ts, to_ts, path) if t >= from_ts
    /// - IgnoreSnapshot if t < from_ts
    pub fn decide_replay_strategy(&self, key: &SnapshotKey, since_ts: u64) -> ReplayStrategy {
        if let Some(meta) = self.get_meta(key) {
            if since_ts >= meta.from_ts {
                if let Some(path) = self.get_data_path(key) {
                    return ReplayStrategy::UseSnapshot {
                        start_ts: meta.from_ts,
                        end_ts: meta.to_ts,
                        path,
                    };
                }
                warn!(
                    target = "snapshot_registry::strategy",
                    "Meta present without data file; ignoring snapshot"
                );
            }
        }
        ReplayStrategy::IgnoreSnapshot
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayStrategy {
    UseSnapshot {
        start_ts: u64,
        end_ts: u64,
        path: PathBuf,
    },
    IgnoreSnapshot,
}

fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => c,
            _ => '_',
        })
        .collect()
}
