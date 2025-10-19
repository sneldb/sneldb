use crate::engine::core::ZonePlan;
use crate::shared::storage_header::{BinaryHeader, FileKind};
use serde_json::Value;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use tracing::{debug, info};
use xorf::{BinaryFuse8, Filter};

/// A wrapper around the xorf BinaryFuse8 implementation
#[derive(Clone, Debug)]
pub struct FieldXorFilter {
    inner: BinaryFuse8,
}

impl FieldXorFilter {
    pub fn new(values: &[String]) -> Self {
        debug!(
            target: "sneldb::xorfilter",
            "Creating XOR filter from {} values",
            values.len()
        );
        let hashes: Vec<u64> = values
            .iter()
            .map(|s| crate::shared::hash::stable_hash64(s))
            .collect();

        let filter = BinaryFuse8::try_from_iterator(hashes.iter().cloned())
            .expect("Failed to create XOR filter");

        Self { inner: filter }
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

    pub fn build_all(zone_plans: &[ZonePlan], segment_dir: &Path) -> std::io::Result<()> {
        debug!(
            target: "sneldb::xorfilter",
            "Building XOR filters for {} zone plans",
            zone_plans.len()
        );

        let field_values = ZonePlan::collect_field_values(zone_plans);
        info!(
            target: "sneldb::xorfilter",
            "Collected {} field groups across all zones",
            field_values.len()
        );

        for ((uid, field), values) in field_values {
            let values_vec: Vec<String> = values.into_iter().collect();
            let filter = Self::new(&values_vec);
            let path = Self::get_filter_path(&uid, &field, segment_dir);

            info!(
                target: "sneldb::xorfilter",
                "Building XOR filter for {}.{} ({} unique values)",
                uid,
                field,
                values_vec.len()
            );

            filter.save(&path)?;
            debug!(
                target: "sneldb::xorfilter",
                "Filter saved for {}.{} -> {:?}",
                uid, field, path
            );
        }

        info!(
            target: "sneldb::xorfilter",
            "Successfully built and saved all XOR filters"
        );
        Ok(())
    }
}
