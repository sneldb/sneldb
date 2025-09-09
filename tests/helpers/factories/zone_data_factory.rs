use crate::engine::core::ZoneData;

pub struct ZoneDataFactory {
    offsets: Vec<u64>,
    values: Vec<String>,
}

impl ZoneDataFactory {
    pub fn new() -> Self {
        Self {
            offsets: vec![],
            values: vec![],
        }
    }

    pub fn with_offset(mut self, offset: u64) -> Self {
        self.offsets.push(offset);
        self
    }

    pub fn with_value<S: Into<String>>(mut self, value: S) -> Self {
        self.values.push(value.into());
        self
    }

    pub fn with_offsets(mut self, offsets: &[u64]) -> Self {
        self.offsets.extend_from_slice(offsets);
        self
    }

    pub fn with_values<S: AsRef<str>>(mut self, values: &[S]) -> Self {
        self.values
            .extend(values.iter().map(|v| v.as_ref().to_string()));
        self
    }

    pub fn create(self) -> ZoneData {
        ZoneData {
            offsets: self.offsets,
            values: self.values,
        }
    }
}
