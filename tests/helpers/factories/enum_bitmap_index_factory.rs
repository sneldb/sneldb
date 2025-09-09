use crate::engine::core::zone::enum_bitmap_index::EnumBitmapIndex;
use std::collections::HashMap;

pub struct EnumBitmapIndexFactory {
    variants: Vec<String>,
    rows_per_zone: u16,
    zone_bits: HashMap<u32, Vec<Vec<u8>>>,
}

impl EnumBitmapIndexFactory {
    pub fn new() -> Self {
        Self {
            variants: vec!["pro".into(), "basic".into()],
            rows_per_zone: 8,
            zone_bits: HashMap::new(),
        }
    }

    pub fn with_variants(mut self, variants: &[&str]) -> Self {
        self.variants = variants.iter().map(|s| (*s).to_string()).collect();
        self
    }

    pub fn with_rows_per_zone(mut self, rows: u16) -> Self {
        self.rows_per_zone = rows;
        self
    }

    pub fn with_zone_variant_bits(
        mut self,
        zone_id: u32,
        variant_id: usize,
        positions: &[usize],
    ) -> Self {
        let bytes_len = ((self.rows_per_zone as usize) + 7) / 8;
        let entry = self
            .zone_bits
            .entry(zone_id)
            .or_insert_with(|| vec![vec![0u8; bytes_len]; self.variants.len()]);
        for &pos in positions {
            let byte = pos / 8;
            let bit = pos % 8;
            entry[variant_id][byte] |= 1u8 << bit;
        }
        self
    }

    pub fn build(self) -> EnumBitmapIndex {
        EnumBitmapIndex {
            variants: self.variants,
            zone_bitmaps: self.zone_bits,
            rows_per_zone: self.rows_per_zone,
        }
    }
}
