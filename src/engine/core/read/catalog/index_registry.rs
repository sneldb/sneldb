use super::{IndexKind, SegmentIndexCatalog};
use crate::engine::core::read::cache::GlobalIndexCatalogCache;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Default, Clone)]
pub struct IndexRegistry {
    // segment_id -> catalog (for a given uid)
    catalogs_by_segment: HashMap<String, SegmentIndexCatalog>,
}

impl IndexRegistry {
    pub fn new() -> Self {
        Self {
            catalogs_by_segment: HashMap::new(),
        }
    }

    pub fn insert_catalog(&mut self, catalog: SegmentIndexCatalog) {
        self.catalogs_by_segment
            .insert(catalog.segment_id.clone(), catalog);
    }

    pub fn load_for_segments(&mut self, base_dir: &PathBuf, segment_ids: &[String], uid: &str) {
        for seg in segment_ids {
            if self.catalogs_by_segment.contains_key(seg) {
                continue;
            }
            if let Ok((arc, _)) =
                GlobalIndexCatalogCache::instance().get_or_load(base_dir, seg, uid)
            {
                self.catalogs_by_segment.insert(seg.clone(), (*arc).clone());
            }
        }
    }

    pub fn available_for(&self, segment_id: &str, field: &str) -> IndexKind {
        self.catalogs_by_segment
            .get(segment_id)
            .map(|c| {
                c.field_kinds
                    .get(field)
                    .copied()
                    .unwrap_or_else(|| IndexKind::from_bits_truncate(0))
            })
            .unwrap_or_else(|| IndexKind::from_bits_truncate(0))
    }

    pub fn available_global(&self, segment_id: &str) -> IndexKind {
        self.catalogs_by_segment
            .get(segment_id)
            .map(|c| c.global_kinds)
            .unwrap_or_else(|| IndexKind::from_bits_truncate(0))
    }

    pub fn has_catalog(&self, segment_id: &str) -> bool {
        self.catalogs_by_segment.contains_key(segment_id)
    }
}
