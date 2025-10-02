use std::path::Path;
use std::sync::Arc;

use crate::engine::core::filter::zone_surf_filter::ZoneSurfFilter;
use crate::engine::core::zone::zone_index::ZoneIndex;

use super::column_handle::ColumnHandle;
use super::global_zone_surf_cache::{CacheOutcome, GlobalZoneSurfCache};
use super::query_caches::QueryCaches;
use super::zone_surf_cache_key::ZoneSurfCacheKey;

pub trait ZoneIndexProvider {
    fn get_or_load_zone_index(
        &self,
        segment_id: &str,
        uid: &str,
    ) -> Result<Arc<ZoneIndex>, std::io::Error>;
}

pub trait ColumnProvider {
    fn get_or_load_column_handle(
        &self,
        segment_id: &str,
        uid: &str,
        field: &str,
    ) -> Result<Arc<ColumnHandle>, std::io::Error>;
}

/// Trait for loading ZoneSurfFilter instances
/// This allows for testability and different loading strategies
pub trait ZoneSurfProvider {
    fn load_zone_surf(
        &self,
        segment_id: &str,
        uid: &str,
        field: &str,
        segment_dir: &Path,
    ) -> Result<(Arc<ZoneSurfFilter>, CacheOutcome), String>;
}

impl ZoneIndexProvider for QueryCaches {
    fn get_or_load_zone_index(
        &self,
        segment_id: &str,
        uid: &str,
    ) -> Result<Arc<ZoneIndex>, std::io::Error> {
        self.get_or_load_zone_index(segment_id, uid)
    }
}

impl ColumnProvider for QueryCaches {
    fn get_or_load_column_handle(
        &self,
        segment_id: &str,
        uid: &str,
        field: &str,
    ) -> Result<Arc<ColumnHandle>, std::io::Error> {
        self.get_or_load_column_handle(segment_id, uid, field)
    }
}

impl ZoneSurfProvider for QueryCaches {
    fn load_zone_surf(
        &self,
        _segment_id: &str,
        uid: &str,
        field: &str,
        segment_dir: &Path,
    ) -> Result<(Arc<ZoneSurfFilter>, CacheOutcome), String> {
        // Build cache key with fully-qualified segment_dir to prevent cross-shard collisions
        let key = ZoneSurfCacheKey::new(segment_dir.to_string_lossy(), uid, field);
        let path = segment_dir.join(format!("{}_{}.zsrf", uid, field));

        GlobalZoneSurfCache::instance()
            .load_from_file(key, &path)
            .map_err(|e| format!("Failed to load zone surf filter: {:?}", e))
    }
}

/// Default implementation using the global cache
pub struct CachedZoneSurfProvider;

impl ZoneSurfProvider for CachedZoneSurfProvider {
    fn load_zone_surf(
        &self,
        _segment_id: &str,
        uid: &str,
        field: &str,
        segment_dir: &Path,
    ) -> Result<(Arc<ZoneSurfFilter>, CacheOutcome), String> {
        let key = ZoneSurfCacheKey::new(segment_dir.to_string_lossy(), uid, field);
        let path = segment_dir.join(format!("{}_{}.zsrf", uid, field));

        GlobalZoneSurfCache::instance()
            .load_from_file(key, &path)
            .map_err(|e| format!("Failed to load zone surf filter: {:?}", e))
    }
}

/// Direct file loading implementation (for testing or when caching is disabled)
pub struct DirectZoneSurfProvider;

impl ZoneSurfProvider for DirectZoneSurfProvider {
    fn load_zone_surf(
        &self,
        _segment_id: &str,
        uid: &str,
        field: &str,
        segment_dir: &Path,
    ) -> Result<(Arc<ZoneSurfFilter>, CacheOutcome), String> {
        let path = segment_dir.join(format!("{}_{}.zsrf", uid, field));
        ZoneSurfFilter::load(&path)
            .map(|filter| (Arc::new(filter), CacheOutcome::Miss))
            .map_err(|e| format!("Failed to load zone surf filter: {:?}", e))
    }
}
