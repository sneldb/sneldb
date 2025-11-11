use std::path::PathBuf;
use std::sync::Arc;
use tracing::info;

use crate::engine::core::filter::zone_surf_filter::ZoneSurfFilter;
use crate::engine::core::time::{CalendarDir, TemporalCalendarIndex, ZoneTemporalIndex};
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
                if tracing::enabled!(tracing::Level::INFO) {
                    info!(target: "cache::zone_index::hit", %segment_id, %uid, "Using cached ZoneIndex");
                }
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
    ) -> Result<Arc<ZoneSurfFilter>, String> {
        if let Some(caches) = self.caches {
            match caches.get_or_load_zone_surf(segment_id, uid, column) {
                Ok(filter) => {
                    if tracing::enabled!(tracing::Level::INFO) {
                        tracing::info!(target: "sneldb::surf", %segment_id, %uid, field = %column, "Loaded ZoneSuRF via cache");
                    }
                    return Ok(filter);
                }
                Err(e) => {
                    if tracing::enabled!(tracing::Level::WARN) {
                        tracing::warn!(
                            target: "sneldb::surf",
                            %segment_id,
                            %uid,
                            field = %column,
                            error = %e,
                            "Failed to load ZoneSuRF via cache, falling back to direct file load"
                        );
                    }
                }
            }
        }
        // Fallback to direct file load
        let path = self.zone_surf_path(segment_id, uid, column);
        if tracing::enabled!(tracing::Level::INFO) {
            tracing::info!(target: "sneldb::surf", %segment_id, %uid, field = %column, path = %path.display(), "Loading ZoneSuRF directly from file");
        }
        let result = ZoneSurfFilter::load(&path);
        match &result {
            Ok(_) => {}
            Err(e) => {
                if tracing::enabled!(tracing::Level::WARN) {
                    tracing::warn!(
                        target: "sneldb::surf",
                        %segment_id,
                        %uid,
                        field = %column,
                        path = %path.display(),
                        error = %e,
                        "Failed to load ZoneSuRF from file"
                    );
                }
            }
        }
        result
            .map(Arc::new)
            .map_err(|e| format!("Failed to load ZoneSuRF from {}: {:?}", path.display(), e))
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
        if let Some(caches) = self.caches {
            match caches.get_or_load_zone_xor_filter(segment_id, uid, column) {
                Ok(index) => {
                    if tracing::enabled!(tracing::Level::INFO) {
                        tracing::info!(target: "sneldb::zxf", %segment_id, %uid, field = %column, "Loaded ZoneXorFilter via cache");
                    }
                    // Return owned value (clone from Arc)
                    return Ok((*index).clone());
                }
                Err(e) => {
                    if tracing::enabled!(tracing::Level::WARN) {
                        tracing::warn!(
                            target: "sneldb::zxf",
                            %segment_id,
                            %uid,
                            field = %column,
                            error = %e,
                            "Failed to load ZoneXorFilter via cache, falling back to direct file load"
                        );
                    }
                }
            }
        }
        // Fallback to direct file load
        let path = self.zxf_path(segment_id, uid, column);
        if tracing::enabled!(tracing::Level::INFO) {
            tracing::info!(target: "sneldb::zxf", %segment_id, %uid, field = %column, path = %path.display(), "Loading ZoneXorFilter directly from file");
        }
        // Load blocking I/O in a way that works with both single-threaded and multi-threaded runtimes
        // Since this is called from a synchronous context, we use std::thread::spawn when in tokio runtime
        if tokio::runtime::Handle::try_current().is_ok() {
            // We're in a tokio runtime - use std::thread::spawn to avoid blocking the runtime
            // This works for both single-threaded and multi-threaded runtimes
            let path = path.clone();
            std::thread::spawn(move || {
                ZoneXorFilterIndex::load(&path).map_err(|e| format!("{:?}", e))
            })
            .join()
            .map_err(|_| "Thread join failed".to_string())?
        } else {
            ZoneXorFilterIndex::load(&path).map_err(|e| format!("{:?}", e))
        }
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

    pub fn load_calendar(&self, segment_id: &str, uid: &str) -> Result<CalendarDir, String> {
        let dir = self.base_dir.join(segment_id);
        CalendarDir::load(uid, &dir).map_err(|e| format!("{:?}", e))
    }

    pub fn load_temporal_index(
        &self,
        segment_id: &str,
        uid: &str,
        zone_id: u32,
    ) -> Result<ZoneTemporalIndex, String> {
        let dir = self.base_dir.join(segment_id);
        ZoneTemporalIndex::load(uid, zone_id, &dir).map_err(|e| format!("{:?}", e))
    }

    pub fn load_field_calendar(
        &self,
        segment_id: &str,
        uid: &str,
        field: &str,
    ) -> Result<TemporalCalendarIndex, String> {
        let dir = self.base_dir.join(segment_id);
        TemporalCalendarIndex::load(uid, field, &dir).map_err(|e| format!("{:?}", e))
    }

    pub fn load_field_temporal_index(
        &self,
        segment_id: &str,
        uid: &str,
        field: &str,
        zone_id: u32,
    ) -> Result<ZoneTemporalIndex, String> {
        let dir = self.base_dir.join(segment_id);
        ZoneTemporalIndex::load_for_field(uid, field, zone_id, &dir).map_err(|e| format!("{:?}", e))
    }
}
