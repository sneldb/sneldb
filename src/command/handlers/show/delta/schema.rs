use std::sync::Arc;

use crate::command::handlers::show::errors::{ShowError, ShowResult};
use crate::engine::core::read::flow::BatchSchema;
use crate::engine::materialize::{MaterializationEntry, schema_to_batch_schema};

pub struct SchemaBuilder;

impl SchemaBuilder {
    pub fn build(entry: &MaterializationEntry) -> ShowResult<Arc<BatchSchema>> {
        schema_to_batch_schema(&entry.schema)
            .map(Arc::new)
            .map_err(|err| ShowError::new(format!("Failed to build batch schema: {err}")))
    }
}
