use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::engine::core::read::flow::BatchSchema;
use crate::engine::core::read::result::ColumnSpec;
use crate::engine::materialize::MaterializationError;
use crate::engine::materialize::catalog::SchemaSnapshot;

pub struct SchemaConverter;

impl SchemaConverter {
    pub fn to_batch_schema(schema: &[SchemaSnapshot]) -> Result<BatchSchema, MaterializationError> {
        let columns = schema
            .iter()
            .map(|s| ColumnSpec {
                name: s.name.clone(),
                logical_type: s.logical_type.clone(),
            })
            .collect();
        BatchSchema::new(columns).map_err(|e| MaterializationError::Batch(e.to_string()))
    }

    pub fn from_batch_schema(schema: &BatchSchema) -> Vec<SchemaSnapshot> {
        schema
            .columns()
            .iter()
            .map(|c| SchemaSnapshot::new(&c.name, &c.logical_type))
            .collect()
    }

    pub fn hash(schema: &[SchemaSnapshot]) -> u64 {
        let mut hasher = DefaultHasher::new();
        for snapshot in schema {
            snapshot.name.hash(&mut hasher);
            snapshot.logical_type.hash(&mut hasher);
        }
        hasher.finish()
    }
}

// Convenience functions for backward compatibility
pub fn schema_to_batch_schema(
    schema: &[SchemaSnapshot],
) -> Result<BatchSchema, MaterializationError> {
    SchemaConverter::to_batch_schema(schema)
}

pub fn batch_schema_to_snapshots(schema: &BatchSchema) -> Vec<SchemaSnapshot> {
    SchemaConverter::from_batch_schema(schema)
}

pub fn schema_hash(schema: &[SchemaSnapshot]) -> u64 {
    SchemaConverter::hash(schema)
}
