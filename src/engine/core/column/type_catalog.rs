use std::collections::HashMap;

use crate::engine::core::ColumnKey;
use crate::engine::core::column::format::PhysicalType;

#[derive(Clone, Debug, Default)]
pub struct ColumnTypeCatalog {
    map: HashMap<ColumnKey, PhysicalType>,
}

impl ColumnTypeCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            map: HashMap::with_capacity(capacity),
        }
    }

    pub fn record(&mut self, key: ColumnKey, phys: PhysicalType) {
        self.map.insert(key, phys);
    }

    pub fn record_ref(&mut self, key: &ColumnKey, phys: PhysicalType) {
        self.map.insert(key.clone(), phys);
    }

    pub fn record_if_absent(&mut self, key: &ColumnKey, phys: PhysicalType) {
        self.map.entry(key.clone()).or_insert(phys);
    }

    pub fn get(&self, key: &ColumnKey) -> Option<PhysicalType> {
        self.map.get(key).copied()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&ColumnKey, &PhysicalType)> {
        self.map.iter()
    }
}
