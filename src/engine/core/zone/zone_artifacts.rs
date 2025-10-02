use std::path::PathBuf;
use std::sync::Arc;
use tracing::info;

use crate::engine::core::filter::zone_surf_filter::ZoneSurfFilter;
use crate::engine::core::zone::enum_bitmap_index::EnumBitmapIndex;
use crate::engine::core::zone::zone_xor_index::ZoneXorFilterIndex;
use crate::engine::core::{FieldXorFilter, QueryCaches, ZoneIndex};

pub struct ZoneArtifacts<'a> {
    pub base_dir: &'a PathBuf,
    pub caches: Option<&'a QueryCaches>,
}

impl<'a> ZoneArtifacts<'a> {
    pub fn new(base_dir: &'a PathBuf, caches: Option<&'a QueryCaches>) -> Self {
        Self { base_dir, caches }
    }

    #[inline]
    fn index_path(&self, segment_id: &str, uid: &str) -> PathBuf {
        self.base_dir.join(segment_id).join(format!("{}.idx", uid))
    }

    #[inline]
    fn zone_surf_path(&self, segment_id: &str, uid: &str, column: &str) -> PathBuf {
        self.base_dir
            .join(segment_id)
            .join(format!("{}_{}.zsrf", uid, column))
    }

    #[inline]
    fn ebm_path(&self, segment_id: &str, uid: &str, column: &str) -> PathBuf {
        self.base_dir
            .join(segment_id)
            .join(format!("{}_{}.ebm", uid, column))
    }

    #[inline]
    fn zxf_path(&self, segment_id: &str, uid: &str, column: &str) -> PathBuf {
        self.base_dir
            .join(segment_id)
            .join(format!("{}_{}.zxf", uid, column))
    }

    #[inline]
    fn xf_path(&self, segment_id: &str, uid: &str, column: &str) -> PathBuf {
        self.base_dir
            .join(segment_id)
            .join(format!("{}_{}.xf", uid, column))
    }

    pub fn load_zone_index(&self, segment_id: &str, uid: &str) -> Result<Arc<ZoneIndex>, String> {
        if let Some(caches) = self.caches {
            if let Ok(index) = caches.get_or_load_zone_index(segment_id, uid) {
                info!(target: "cache::zone_index::hit", %segment_id, %uid, "Using cached ZoneIndex");
                return Ok(index);
            }
        }
        let path = self.index_path(segment_id, uid);
        ZoneIndex::load_from_path(&path)
            .map(Arc::new)
            .map_err(|e| format!("{:?}", e))
    }

    pub fn load_zone_surf(
        &self,
        segment_id: &str,
        uid: &str,
        column: &str,
    ) -> Result<ZoneSurfFilter, String> {
        if let Some(caches) = self.caches {
            if let Ok(filter) = caches.get_or_load_zone_surf(segment_id, uid, column) {
                if tracing::enabled!(tracing::Level::INFO) {
                    tracing::info!(target: "sneldb::surf", %segment_id, %uid, field = %column, "Loaded ZoneSuRF via cache");
                }
                return Ok((*filter).clone());
            }
        }
        // Fallback to direct file load
        let path = self.zone_surf_path(segment_id, uid, column);
        if tracing::enabled!(tracing::Level::INFO) {
            tracing::info!(target: "sneldb::surf", %segment_id, %uid, field = %column, path = %path.display(), "Loading ZoneSuRF directly from file");
        }
        ZoneSurfFilter::load(&path).map_err(|e| format!("{:?}", e))
    }

    pub fn load_ebm(
        &self,
        segment_id: &str,
        uid: &str,
        column: &str,
    ) -> Result<EnumBitmapIndex, String> {
        let path = self.ebm_path(segment_id, uid, column);
        EnumBitmapIndex::load(&path).map_err(|_| "ebm load error".to_string())
    }

    pub fn load_zxf(
        &self,
        segment_id: &str,
        uid: &str,
        column: &str,
    ) -> Result<ZoneXorFilterIndex, String> {
        let path = self.zxf_path(segment_id, uid, column);
        ZoneXorFilterIndex::load(&path).map_err(|e| format!("{:?}", e))
    }

    pub fn load_xf(
        &self,
        segment_id: &str,
        uid: &str,
        column: &str,
    ) -> Result<FieldXorFilter, String> {
        let path = self.xf_path(segment_id, uid, column);
        FieldXorFilter::load(&path).map_err(|e| format!("{:?}", e))
    }
}
