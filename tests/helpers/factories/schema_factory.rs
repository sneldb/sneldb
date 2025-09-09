use crate::engine::schema::FieldType;
use crate::engine::schema::errors::SchemaError;
use crate::engine::schema::registry::{MiniSchema, SchemaRegistry};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::sync::RwLock;

/// Creates a new test schema registry with isolated temp storage.
/// TempDir is automatically cleaned up when dropped.
pub struct SchemaRegistryFactory {
    registry: Arc<RwLock<SchemaRegistry>>,
    _tempdir: TempDir, // keep it alive
}

impl SchemaRegistryFactory {
    pub fn new() -> Self {
        let tempdir = tempfile::tempdir().expect("Failed to create temp dir");
        let path = tempdir.path().join("schemas.bin");
        let registry =
            SchemaRegistry::new_with_path(path).expect("Failed to initialize SchemaRegistry");

        Self {
            registry: Arc::new(RwLock::new(registry)),
            _tempdir: tempdir, // store so it doesn't get deleted immediately
        }
    }

    pub fn registry(&self) -> Arc<RwLock<SchemaRegistry>> {
        self.registry.clone()
    }

    pub async fn define_with_fields(
        &self,
        event_type: &str,
        fields: &[(&str, &str)],
    ) -> Result<(), SchemaError> {
        let mut map = HashMap::new();
        for (k, v) in fields {
            let ft = FieldType::from_spec_with_nullable(v).unwrap_or(FieldType::String);
            map.insert(k.to_string(), ft);
        }
        let mini = MiniSchema { fields: map };
        self.registry.write().await.define(event_type, mini)
    }

    pub async fn define_with_field_types(
        &self,
        event_type: &str,
        fields: &[(&str, FieldType)],
    ) -> Result<(), SchemaError> {
        let mut map = HashMap::new();
        for (k, v) in fields {
            map.insert((*k).to_string(), v.clone());
        }
        let mini = MiniSchema { fields: map };
        self.registry.write().await.define(event_type, mini)
    }
}
