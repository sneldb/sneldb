use crate::engine::core::ZonePlan;
use crate::shared::storage_header::{BinaryHeader, FileKind};
use serde_json::Value;
use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use tracing::{debug, info, warn};
use xorf::{BinaryFuse8, Filter};

/// A wrapper around the xorf BinaryFuse8 implementation
#[derive(Clone, Debug)]
pub struct FieldXorFilter {
    inner: BinaryFuse8,
}

impl FieldXorFilter {
    pub fn new(values: &[String]) -> Result<Self, String> {
        debug!(
            target: "sneldb::xorfilter",
            "Creating XOR filter from {} values",
            values.len()
        );

        if values.is_empty() {
            return Err("Cannot create XOR filter from empty values".to_string());
        }

        // Hash all values (values should already be unique from collect_field_values,
        // but we deduplicate hashes to handle hash collisions)
        let mut unique_hashes: HashSet<u64> = HashSet::with_capacity(values.len());
        for value in values {
            let hash = crate::shared::hash::stable_hash64(value);
            unique_hashes.insert(hash);
        }

        // Convert HashSet to Vec for iterator (order doesn't matter for filter construction)
        let hashes_vec: Vec<u64> = unique_hashes.into_iter().collect();

        let filter = BinaryFuse8::try_from_iterator(hashes_vec.into_iter())
            .map_err(|e| format!("Failed to construct binary fuse filter: {:?}", e))?;

        Ok(Self { inner: filter })
    }

    pub fn value_to_string(value: &Value) -> Option<String> {
        match value {
            Value::String(s) => Some(s.clone()),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Some(i.to_string())
                } else if let Some(f) = n.as_f64() {
                    Some(f.to_string())
                } else {
                    Some(n.to_string())
                }
            }
            Value::Bool(b) => Some(b.to_string()),
            _ => {
                info!(
                    target: "sneldb::xorfilter",
                    "Unsupported value type for XOR filter: {:?}", value
                );
                None
            }
        }
    }

    pub fn contains(&self, value: &str) -> bool {
        let h = crate::shared::hash::stable_hash64(&value);
        self.inner.contains(&h)
    }

    pub fn contains_value(&self, value: &Value) -> bool {
        match Self::value_to_string(value) {
            Some(value_str) => {
                let result = self.contains(&value_str);
                info!(
                    target: "sneldb::xorfilter",
                    "Checking value '{}' -> result: {}",
                    value_str, result
                );
                result
            }
            None => {
                info!(
                    target: "sneldb::xorfilter",
                    "Skipping unsupported value: {:?}", value
                );
                false
            }
        }
    }

    pub fn save(&self, path: &Path) -> std::io::Result<()> {
        debug!(target: "sneldb::xorfilter", "Saving XOR filter to {:?}", path);
        let file = OpenOptions::new().create(true).write(true).open(path)?;
        let mut writer = BufWriter::new(file);

        let data = bincode::serialize(&self.inner)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let header = BinaryHeader::new(FileKind::XorFilter.magic(), 1, 0);
        header.write_to(&mut writer)?;
        writer.write_all(&data)?;
        writer.flush()?;

        info!(
            target: "sneldb::xorfilter",
            "Saved XOR filter to {:?} ({} bytes)",
            path,
            data.len()
        );

        Ok(())
    }

    pub fn load(path: &Path) -> std::io::Result<Self> {
        debug!(target: "sneldb::xorfilter", "Loading XOR filter from {:?}", path);
        let data = std::fs::read(path)?;
        let mut slice = &data[..];
        let header = BinaryHeader::read_from(&mut slice)?;
        if header.magic != FileKind::XorFilter.magic() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid xorfilter magic",
            ));
        }
        let filter = bincode::deserialize(&data[BinaryHeader::TOTAL_LEN..])
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        info!(
            target: "sneldb::xorfilter",
            "Loaded XOR filter from {:?} ({} bytes)",
            path,
            data.len()
        );

        Ok(Self { inner: filter })
    }

    fn get_filter_path(uid: &str, field: &str, segment_dir: &Path) -> PathBuf {
        segment_dir.join(format!("{}_{}.xf", uid, field))
    }

    /// Build XOR filters only for fields present in `allowed_fields`.
    /// Errors during filter creation are logged and skipped (best-effort).
    pub fn build_all_filtered(
        zone_plans: &[ZonePlan],
        segment_dir: &Path,
        allowed_fields: &HashSet<String>,
    ) -> std::io::Result<()> {
        debug!(
            target: "sneldb::xorfilter",
            allowed = allowed_fields.len(),
            "Building filtered XOR filters"
        );
        let field_values = ZonePlan::collect_field_values(zone_plans);
        for ((uid, field), values) in field_values {
            if !allowed_fields.contains(&field) {
                continue;
            }
            let values_vec: Vec<String> = values.into_iter().collect();
            match Self::new(&values_vec) {
                Ok(filter) => {
                    let path = Self::get_filter_path(&uid, &field, segment_dir);
                    if let Err(e) = filter.save(&path) {
                        warn!(
                            target: "sneldb::xorfilter",
                            uid = %uid,
                            field = %field,
                            error = %e,
                            "Failed to save XOR filter"
                        );
                    }
                }
                Err(e) => {
                    // Log errors as WARN - BinaryFuse8 construction can fail for various reasons
                    warn!(
                        target: "sneldb::xorfilter",
                        uid = %uid,
                        field = %field,
                        error = %e,
                        values_count = values_vec.len(),
                        "Skipping XOR filter creation due to error"
                    );
                    // Continue with other fields - this is best-effort
                }
            }
        }
        Ok(())
    }
}
