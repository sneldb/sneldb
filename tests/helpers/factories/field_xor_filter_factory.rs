use crate::engine::core::FieldXorFilter;
use serde_json::Value;
use serde_json::json;

/// Factory for building `FieldXorFilter` instances in tests.
pub struct FieldXorFilterFactory {
    values: Vec<Value>,
}

impl FieldXorFilterFactory {
    pub fn new() -> Self {
        Self {
            values: vec![
                json!("apple"),
                json!("banana"),
                json!("cherry"),
                json!(42),
                json!(true),
            ],
        }
    }

    pub fn with(mut self, value: Value) -> Self {
        self.values.push(value);
        self
    }

    pub fn with_all(mut self, new_values: Vec<Value>) -> Self {
        self.values.extend(new_values);
        self
    }

    pub fn build(self) -> FieldXorFilter {
        let strings: Vec<String> = self
            .values
            .into_iter()
            .filter_map(|v| FieldXorFilter::value_to_string(&v))
            .collect();

        FieldXorFilter::new(&strings)
    }
}
