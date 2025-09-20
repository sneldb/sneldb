use std::collections::HashMap;
use tracing::debug;

/// Key for column offsets: (event_uid, field)
pub type ColumnKey = (String, String);
/// Offsets for a column: vector of u64 file positions
pub type Offsets = Vec<u64>;
/// Zone ID for a column: u32
pub type ZoneId = u32;
/// Zone values for a column: Vec<String>
pub type ZoneValues = Vec<String>;

#[derive(Debug, Default, Clone)]
pub struct ZoneData {
    pub offsets: Vec<u64>,
    pub values: Vec<String>,
}

/// Offsets for each `ColumnKey` (event_uid, field) pair across zones.
#[derive(Debug)]
pub struct ColumnOffsets {
    offsets: HashMap<ColumnKey, HashMap<ZoneId, ZoneData>>,
}

impl ColumnOffsets {
    pub fn new() -> Self {
        Self {
            offsets: HashMap::new(),
        }
    }

    // .zf is removed; this type stays only for building secondary indexes from values

    pub fn insert_offset(&mut self, key: ColumnKey, zone_id: ZoneId, offset: u64, value: String) {
        debug!(
            target: "col_offsets::insert",
            ?key,
            zone_id,
            offset,
            value = value.as_str(),
            "Inserting offset entry"
        );

        self.offsets
            .entry(key.clone())
            .or_default()
            .entry(zone_id)
            .or_default()
            .offsets
            .push(offset);

        self.offsets
            .get_mut(&key)
            .unwrap()
            .get_mut(&zone_id)
            .unwrap()
            .values
            .push(value);
    }

    pub fn get(&self, key: &ColumnKey) -> Option<&HashMap<ZoneId, ZoneData>> {
        self.offsets.get(key)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&ColumnKey, &HashMap<ZoneId, ZoneData>)> {
        self.offsets.iter()
    }

    // .zf write removed

    pub fn as_map(&self) -> &HashMap<ColumnKey, HashMap<ZoneId, ZoneData>> {
        &self.offsets
    }

    pub fn as_flat_map(&self) -> HashMap<ColumnKey, Offsets> {
        let mut flat = HashMap::new();
        for (key, zone_map) in &self.offsets {
            let entry = flat.entry(key.clone()).or_insert_with(Vec::new);
            for zone_data in zone_map.values() {
                entry.extend(zone_data.offsets.iter().cloned());
            }
        }
        flat
    }
}
