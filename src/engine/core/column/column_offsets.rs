use crate::engine::errors::StoreError;
use crate::engine::schema::SchemaRegistry;
use crate::shared::storage_header::{BinaryHeader, FileKind, open_and_header_offset};
use memmap2::MmapOptions;
use std::collections::HashMap;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

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

    pub fn load(
        segment_dir: &Path,
        segment_id: &str,
        uid: &str,
        field: &str,
    ) -> Result<HashMap<ZoneId, Vec<u64>>, StoreError> {
        let zf_path = segment_dir.join(format!("{}_{}.zf", uid, field));
        info!(
            target: "col_offsets::load",
            segment = %segment_id,
            uid = %uid,
            field = %field,
            path = %zf_path.display(),
            "Loading zone offset file"
        );

        let (file, header_offset) =
            open_and_header_offset(&zf_path, FileKind::ZoneOffsets.magic())?;
        let mmap = unsafe { MmapOptions::new().map(&file)? };

        let mut zone_offsets: HashMap<ZoneId, Vec<u64>> = HashMap::new();
        let mut pos = header_offset;

        while pos + 8 <= mmap.len() {
            let zone_id = u32::from_le_bytes(mmap[pos..pos + 4].try_into().unwrap());
            pos += 4;

            let num_offsets = u32::from_le_bytes(mmap[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            let mut offsets = Vec::with_capacity(num_offsets);
            for _ in 0..num_offsets {
                if pos + 8 > mmap.len() {
                    return Err(StoreError::FlushFailed(
                        "Unexpected end of file".to_string(),
                    ));
                }
                let offset = u64::from_le_bytes(mmap[pos..pos + 8].try_into().unwrap());
                offsets.push(offset);
                pos += 8;
            }

            debug!(
                target: "col_offsets::load",
                zone_id = zone_id,
                count = offsets.len(),
                "Loaded zone offsets"
            );
            zone_offsets.insert(zone_id, offsets);
        }

        info!(
            target: "col_offsets::load",
            zones = zone_offsets.len(),
            "Finished loading .zf file"
        );
        Ok(zone_offsets)
    }

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

    pub async fn write(
        &self,
        segment_dir: &Path,
        registry: &Arc<RwLock<SchemaRegistry>>,
    ) -> Result<(), StoreError> {
        for ((event_type, field), zones) in self.iter() {
            let uid = registry.read().await.get_uid(event_type).unwrap();
            let filename = format!("{}_{}.zf", uid, field);
            let path = segment_dir.join(&filename);

            info!(
                target: "col_offsets::write",
                %event_type,
                %field,
                uid = %uid,
                path = %path.display(),
                "Writing .zf file"
            );

            let mut writer = BufWriter::new(std::fs::File::create(&path)?);
            // Fresh file; write header
            BinaryHeader::new(FileKind::ZoneOffsets.magic(), 1, 0).write_to(&mut writer)?;

            let mut zone_entries: Vec<_> = zones.iter().collect();
            zone_entries.sort_by_key(|(zone_id, _)| *zone_id);

            for (zone_id, zone_data) in zone_entries {
                writer.write_all(&zone_id.to_le_bytes())?;
                writer.write_all(&(zone_data.offsets.len() as u32).to_le_bytes())?;
                for offset in &zone_data.offsets {
                    writer.write_all(&offset.to_le_bytes())?;
                }

                debug!(
                    target: "col_offsets::write",
                    zone_id = zone_id,
                    count = zone_data.offsets.len(),
                    "Wrote offsets for zone"
                );
            }
        }

        Ok(())
    }

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
