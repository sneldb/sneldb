use crate::engine::core::{ColumnKey, ColumnOffsets};
use crate::test_helpers::factories::SchemaRegistryFactory;
use tempfile::tempdir;

#[tokio::test]
async fn test_column_offsets_in_memory_for_zone_index() {
    let mut offsets = ColumnOffsets::new();
    let key: ColumnKey = ("event".into(), "field".into());
    offsets.insert_offset(key.clone(), 0, 0, "v1".into());
    offsets.insert_offset(key.clone(), 0, 0, "v2".into());
    let map = offsets.as_map();
    assert!(map.contains_key(&key));
}
