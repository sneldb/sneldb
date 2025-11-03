use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::command::types::MaterializedQuerySpec;
use crate::engine::materialize::MaterializationError;
use crate::engine::materialize::high_water::HighWaterMark;
use crate::engine::materialize::spec::MaterializedQuerySpecExt;
use crate::shared::time::now;

use super::policy::RetentionPolicy;
use super::schema::SchemaSnapshot;
use super::serde_ext::materialized_query_spec;
use super::telemetry::MaterializationTelemetry;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializationEntry {
    pub name: String,
    pub spec_hash: u64,
    #[serde(with = "materialized_query_spec")]
    pub spec: MaterializedQuerySpec,
    pub storage_path: std::path::PathBuf,
    #[serde(default)]
    pub schema: Vec<SchemaSnapshot>,
    #[serde(default)]
    pub high_water_mark: Option<HighWaterMark>,
    #[serde(default)]
    pub row_count: u64,
    #[serde(default)]
    pub delta_rows_appended: u64,
    #[serde(default)]
    pub byte_size: u64,
    #[serde(default)]
    pub delta_bytes_appended: u64,
    #[serde(default)]
    pub created_at: u64,
    #[serde(default)]
    pub updated_at: u64,
    #[serde(default)]
    pub retention: Option<RetentionPolicy>,
}

impl MaterializationEntry {
    pub fn new(spec: MaterializedQuerySpec, root_dir: &Path) -> Result<Self, MaterializationError> {
        let spec_hash = spec.plan_hash()?;
        let storage_path = root_dir.join(spec.alias());
        Ok(Self {
            name: spec.alias().to_string(),
            spec_hash,
            storage_path,
            spec,
            schema: Vec::new(),
            high_water_mark: None,
            row_count: 0,
            delta_rows_appended: 0,
            byte_size: 0,
            delta_bytes_appended: 0,
            created_at: now(),
            updated_at: now(),
            retention: None,
        })
    }

    pub fn retention_policy(&self) -> Option<&RetentionPolicy> {
        self.retention.as_ref()
    }

    pub fn set_retention(&mut self, policy: RetentionPolicy) {
        self.retention = Some(policy);
    }

    pub fn telemetry_summary(&self) -> MaterializationTelemetry {
        MaterializationTelemetry {
            row_count: self.row_count,
            delta_rows_appended: self.delta_rows_appended,
            byte_size: self.byte_size,
            delta_bytes_appended: self.delta_bytes_appended,
            high_water_mark: self.high_water_mark,
            updated_at: self.updated_at,
        }
    }

    pub fn touch(&mut self) {
        self.updated_at = now();
    }
}
