use crate::engine::core::ZonePlan;
use crate::engine::schema::FieldType;
use crate::engine::schema::registry::SchemaRegistry;
use crate::shared::storage_header::{BinaryHeader, FileKind};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Default, Clone)]
pub struct EnumBitmapIndex {
    pub variants: Vec<String>,
    pub zone_bitmaps: HashMap<u32, Vec<Vec<u8>>>, // zone_id -> [variant_id -> packed bits]
    pub rows_per_zone: u16,
}

impl EnumBitmapIndex {
    pub fn save(&self, path: &Path) -> std::io::Result<()> {
        let mut f = OpenOptions::new().create(true).write(true).open(path)?;

        BinaryHeader::new(FileKind::EnumBitmap.magic(), 1, 0).write_to(&mut f)?;

        // variants
        f.write_all(&(self.variants.len() as u16).to_le_bytes())?;
        for v in &self.variants {
            let bytes = v.as_bytes();
            f.write_all(&(bytes.len() as u16).to_le_bytes())?;
            f.write_all(bytes)?;
        }

        // rows_per_zone
        f.write_all(&self.rows_per_zone.to_le_bytes())?;

        // zones
        for (zone_id, bitsets) in &self.zone_bitmaps {
            f.write_all(&zone_id.to_le_bytes())?;
            // each variant bitmap payload length u32 then bytes
            f.write_all(&(bitsets.len() as u16).to_le_bytes())?;
            for bits in bitsets {
                f.write_all(&(bits.len() as u32).to_le_bytes())?;
                f.write_all(bits)?;
            }
        }
        Ok(())
    }

    pub fn load(path: &Path) -> std::io::Result<Self> {
        let data = std::fs::read(path)?;
        let mut cursor = &data[..];
        let header = BinaryHeader::read_from(&mut cursor)?;
        if header.magic != FileKind::EnumBitmap.magic() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid magic",
            ));
        }

        let mut vbuf = [0u8; 2];
        cursor.read_exact(&mut vbuf)?;
        let var_count = u16::from_le_bytes(vbuf) as usize;
        let mut variants = Vec::with_capacity(var_count);
        for _ in 0..var_count {
            let mut l = [0u8; 2];
            cursor.read_exact(&mut l)?;
            let len = u16::from_le_bytes(l) as usize;
            let mut buf = vec![0u8; len];
            cursor.read_exact(&mut buf)?;
            variants.push(
                String::from_utf8(buf)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?,
            );
        }

        let mut r = [0u8; 2];
        cursor.read_exact(&mut r)?;
        let rows_per_zone = u16::from_le_bytes(r);

        let mut zone_bitmaps: HashMap<u32, Vec<Vec<u8>>> = HashMap::new();
        while !cursor.is_empty() {
            if cursor.len() < 4 {
                break;
            }
            let mut zid_buf = [0u8; 4];
            cursor.read_exact(&mut zid_buf)?;
            let zone_id = u32::from_le_bytes(zid_buf);

            let mut cbuf = [0u8; 2];
            cursor.read_exact(&mut cbuf)?;
            let count = u16::from_le_bytes(cbuf) as usize;
            let mut bitsets = Vec::with_capacity(count);
            for _ in 0..count {
                let mut lbuf = [0u8; 4];
                cursor.read_exact(&mut lbuf)?;
                let blen = u32::from_le_bytes(lbuf) as usize;
                let mut b = vec![0u8; blen];
                cursor.read_exact(&mut b)?;
                bitsets.push(b);
            }
            zone_bitmaps.insert(zone_id, bitsets);
        }

        Ok(Self {
            variants,
            zone_bitmaps,
            rows_per_zone,
        })
    }

    pub fn has_any(&self, zone_id: u32, variant_id: usize) -> bool {
        self.zone_bitmaps
            .get(&zone_id)
            .and_then(|v| v.get(variant_id))
            .map(|bytes| bytes.iter().any(|b| *b != 0))
            .unwrap_or(false)
    }
}

pub struct EnumBitmapBuilder<'a> {
    _uid: &'a str,
    _field: &'a str,
    variants: Vec<String>,
    rows_per_zone: u16,
    zone_maps: HashMap<u32, Vec<Vec<u8>>>,
}

impl<'a> EnumBitmapBuilder<'a> {
    pub fn new(uid: &'a str, field: &'a str, variants: Vec<String>, rows_per_zone: u16) -> Self {
        Self {
            _uid: uid,
            _field: field,
            variants,
            rows_per_zone,
            zone_maps: HashMap::new(),
        }
    }

    fn alloc_bitmap(&self) -> Vec<u8> {
        let bits = self.rows_per_zone as usize;
        let bytes = (bits + 7) / 8;
        vec![0u8; bytes]
    }

    fn set_bit(bytes: &mut [u8], idx: usize) {
        let byte = idx / 8;
        let bit = idx % 8;
        bytes[byte] |= 1u8 << bit;
    }

    pub fn add_zone_values(&mut self, zone_id: u32, values: &[String]) {
        let mut bitsets: Vec<Vec<u8>> = vec![self.alloc_bitmap(); self.variants.len()];
        for (i, val) in values.iter().enumerate() {
            if let Some(vid) = self.variants.iter().position(|v| v == val) {
                Self::set_bit(&mut bitsets[vid], i);
            }
        }
        self.zone_maps.insert(zone_id, bitsets);
    }

    pub fn build(self) -> EnumBitmapIndex {
        EnumBitmapIndex {
            variants: self.variants,
            zone_bitmaps: self.zone_maps,
            rows_per_zone: self.rows_per_zone,
        }
    }

    pub async fn build_all(
        zone_plans: &[ZonePlan],
        segment_dir: &Path,
        registry: &Arc<RwLock<SchemaRegistry>>,
    ) -> std::io::Result<()> {
        if zone_plans.is_empty() {
            return Ok(());
        }
        let uid = &zone_plans[0].uid;
        let event_type = &zone_plans[0].event_type;

        // Determine enum fields from schema
        let schema = registry.read().await.get(event_type).cloned();
        let Some(schema) = schema else {
            return Ok(());
        };
        let enum_fields: Vec<_> = schema
            .fields
            .iter()
            .filter_map(|(k, v)| match v {
                FieldType::Enum(et) => Some((k.clone(), et.variants.clone())),
                _ => None,
            })
            .collect();
        if enum_fields.is_empty() {
            return Ok(());
        }

        let rows_per_zone = (zone_plans[0].end_index - zone_plans[0].start_index + 1) as u16;

        for (field, variants) in enum_fields {
            let mut builder = EnumBitmapBuilder::new(uid, &field, variants.clone(), rows_per_zone);

            for zone in zone_plans {
                // Collect values for this field in zone order
                let mut values: Vec<String> = Vec::with_capacity(rows_per_zone as usize);
                for e in &zone.events {
                    values.push(e.get_field_value(&field));
                }
                builder.add_zone_values(zone.id, &values);
            }

            let index = builder.build();
            let path = segment_dir.join(format!("{}_{}.ebm", uid, field));
            index.save(&path)?;
            tracing::debug!(target = "sneldb::flush", uid=%uid, field=%field, path=%path.display(), "Saved EBM file");
        }

        Ok(())
    }
}
