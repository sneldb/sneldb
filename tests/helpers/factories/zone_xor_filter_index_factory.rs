use std::collections::HashMap;

use xorf::BinaryFuse8;

use crate::engine::core::zone::zone_xor_index::ZoneXorFilterIndex;

pub struct ZoneXorFilterIndexFactory {
    uid: String,
    field: String,
    /// zone_id -> values
    zones: HashMap<u32, Vec<String>>,
}

impl ZoneXorFilterIndexFactory {
    pub fn new(uid: impl Into<String>, field: impl Into<String>) -> Self {
        Self {
            uid: uid.into(),
            field: field.into(),
            zones: HashMap::new(),
        }
    }

    pub fn with_zone_values(mut self, zone_id: u32, values: Vec<String>) -> Self {
        self.zones.insert(zone_id, values);
        self
    }

    pub fn typical(mut self) -> Self {
        self.zones.insert(0, vec!["apple".into(), "banana".into()]);
        self.zones.insert(1, vec!["carrot".into(), "banana".into()]);
        self
    }

    pub fn build(self) -> ZoneXorFilterIndex {
        let mut idx = ZoneXorFilterIndex::new(self.uid, self.field);
        for (zone_id, values) in self.zones.into_iter() {
            let mut v = values;
            v.sort();
            v.dedup();
            let hashes = v.into_iter().map(|s| hash64(&s)).collect::<Vec<u64>>();
            let filter = BinaryFuse8::try_from_iterator(hashes.into_iter())
                .expect("failed to build BinaryFuse8");
            idx.put_zone_filter(zone_id, filter);
        }
        idx
    }
}

fn hash64(s: &str) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}
