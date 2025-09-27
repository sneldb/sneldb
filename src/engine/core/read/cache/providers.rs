use std::sync::Arc;

use crate::engine::core::zone::zone_index::ZoneIndex;

use super::column_handle::ColumnHandle;
use super::query_caches::QueryCaches;

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


