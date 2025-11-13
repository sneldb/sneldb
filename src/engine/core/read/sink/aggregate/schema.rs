use std::collections::HashMap;

/// Manages schema caching and column index mapping for efficient lookups
pub(crate) struct SchemaCache {
    /// Cached column indices to avoid HashMap lookups
    column_indices: Option<HashMap<String, usize>>,
    /// Cache column names to avoid cloning for every batch
    cached_column_names: Option<Vec<String>>,
}

impl SchemaCache {
    pub(crate) fn new() -> Self {
        Self {
            column_indices: None,
            cached_column_names: None,
        }
    }

    /// Initialize column indices from a batch schema to avoid HashMap lookups
    /// Caches column names if schema is compatible to avoid cloning
    pub(crate) fn initialize_column_indices(&mut self, column_names: &[String]) {
        // Check if schema is compatible (same column names)
        let schema_changed = if let Some(ref cached) = self.cached_column_names {
            cached.len() != column_names.len()
                || cached.iter().zip(column_names.iter()).any(|(a, b)| a != b)
        } else {
            true
        };

        if schema_changed {
            // Schema changed: update cache
            self.cached_column_names = Some(column_names.to_vec());

            // Update column indices
            let mut indices = HashMap::with_capacity(column_names.len());
            for (idx, name) in column_names.iter().enumerate() {
                indices.insert(name.clone(), idx);
            }
            self.column_indices = Some(indices);
        }
    }

    pub(crate) fn column_indices(&self) -> Option<&HashMap<String, usize>> {
        self.column_indices.as_ref()
    }
}

