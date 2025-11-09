use crate::engine::core::filter::filter_group::FilterGroup;
use crate::engine::core::read::catalog::{IndexKind, IndexRegistry, SegmentIndexCatalog};
use crate::engine::core::read::index_planner::IndexPlanner;
use crate::engine::core::read::index_strategy::IndexStrategy;
use crate::engine::schema::registry::{MiniSchema, SchemaRecord, SchemaRegistry};
use crate::engine::schema::types::{EnumType, FieldType};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

fn make_registry_with_schema(
    path: PathBuf,
    event_type: &str,
) -> (Arc<RwLock<SchemaRegistry>>, String) {
    let mut reg = SchemaRegistry::new_with_path(path).expect("registry");
    let mut fields: HashMap<String, FieldType> = HashMap::new();
    fields.insert("id".to_string(), FieldType::I64);
    fields.insert("timestamp".to_string(), FieldType::Timestamp);
    fields.insert("created_at".to_string(), FieldType::Timestamp);
    fields.insert(
        "kind".to_string(),
        FieldType::Enum(EnumType {
            variants: vec!["A".to_string(), "B".to_string()],
        }),
    );
    let schema = MiniSchema { fields };
    reg.define(event_type, schema).expect("define");
    let uid = reg.get_uid(event_type).expect("uid");
    (Arc::new(RwLock::new(reg)), uid)
}

fn make_catalog(
    uid: &str,
    segment_id: &str,
    set: impl FnOnce(&mut SegmentIndexCatalog),
) -> SegmentIndexCatalog {
    let mut cat = SegmentIndexCatalog::new(uid.to_string(), segment_id.to_string());
    set(&mut cat);
    cat
}

#[tokio::test]
async fn planner_fullscan_when_no_catalog_for_segment() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("schemas.bin");
    let (registry, uid) = make_registry_with_schema(path, "ev");

    let index_registry = IndexRegistry::new();
    let planner = IndexPlanner::new(&registry, &index_registry, Some(uid));

    let fp = FilterGroup::Filter {
        column: "id".to_string(),
        operation: None,
        value: None,
        priority: 0,
        uid: None,
        index_strategy: None,
    };
    let strat = planner.choose(&fp, "S1").await;
    assert!(matches!(strat, IndexStrategy::FullScan));
}

#[tokio::test]
async fn planner_temporal_eq_and_range_for_timestamp() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("schemas.bin");
    let (registry, uid) = make_registry_with_schema(path, "ev");

    let mut idx = IndexRegistry::new();
    idx.insert_catalog(make_catalog(&uid, "S1", |_| {}));
    let planner = IndexPlanner::new(&registry, &idx, Some(uid.clone()));

    let fp_eq = FilterGroup::Filter {
        column: "timestamp".to_string(),
        operation: Some(crate::command::types::CompareOp::Eq),
        value: None,
        priority: 0,
        uid: None,
        index_strategy: None,
    };
    let s_eq = planner.choose(&fp_eq, "S1").await;
    assert!(matches!(s_eq, IndexStrategy::TemporalEq { .. }));

    let fp_ge = FilterGroup::Filter {
        column: "timestamp".to_string(),
        operation: Some(crate::command::types::CompareOp::Gte),
        value: None,
        priority: 0,
        uid: None,
        index_strategy: None,
    };
    let s_rg = planner.choose(&fp_ge, "S1").await;
    assert!(matches!(s_rg, IndexStrategy::TemporalRange { .. }));
}

#[tokio::test]
async fn planner_temporal_for_non_timestamp_field_uses_schema() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("schemas.bin");
    let (registry, uid) = make_registry_with_schema(path, "ev");

    let mut idx = IndexRegistry::new();
    idx.insert_catalog(make_catalog(&uid, "S1", |_| {}));
    let planner = IndexPlanner::new(&registry, &idx, Some(uid.clone()));

    let fp = FilterGroup::Filter {
        column: "created_at".to_string(),
        operation: Some(crate::command::types::CompareOp::Lt),
        value: None,
        priority: 0,
        uid: None,
        index_strategy: None,
    };
    let s = planner.choose(&fp, "S1").await;
    assert!(matches!(s, IndexStrategy::TemporalRange { .. }));
}

#[tokio::test]
async fn planner_enum_bitmap_when_available() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("schemas.bin");
    let (registry, uid) = make_registry_with_schema(path, "ev");

    let mut idx = IndexRegistry::new();
    idx.insert_catalog(make_catalog(&uid, "S1", |c| {
        c.set_field_kind("kind", IndexKind::ENUM_BITMAP);
    }));
    let planner = IndexPlanner::new(&registry, &idx, Some(uid.clone()));

    let fp = FilterGroup::Filter {
        column: "kind".to_string(),
        operation: Some(crate::command::types::CompareOp::Eq),
        value: None,
        priority: 0,
        uid: None,
        index_strategy: None,
    };
    let s = planner.choose(&fp, "S1").await;
    assert!(matches!(s, IndexStrategy::EnumBitmap { .. }));
}

#[tokio::test]
async fn planner_range_prefers_surf_when_available() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("schemas.bin");
    let (registry, uid) = make_registry_with_schema(path, "ev");

    let mut idx = IndexRegistry::new();
    idx.insert_catalog(make_catalog(&uid, "S1", |c| {
        c.set_field_kind("id", IndexKind::ZONE_SURF);
    }));
    let planner = IndexPlanner::new(&registry, &idx, Some(uid.clone()));

    let fp = FilterGroup::Filter {
        column: "id".to_string(),
        operation: Some(crate::command::types::CompareOp::Gt),
        value: None,
        priority: 0,
        uid: None,
        index_strategy: None,
    };
    let s = planner.choose(&fp, "S1").await;
    assert!(matches!(s, IndexStrategy::ZoneSuRF { .. }));
}

#[tokio::test]
async fn planner_equality_prefers_zxf_then_xf_then_fullscan() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("schemas.bin");
    let (registry, uid) = make_registry_with_schema(path, "ev");

    // With ZXF
    let mut idx = IndexRegistry::new();
    idx.insert_catalog(make_catalog(&uid, "S1", |c| {
        c.set_field_kind(
            "id",
            IndexKind::ZONE_XOR_INDEX | IndexKind::XOR_FIELD_FILTER,
        );
    }));
    let planner = IndexPlanner::new(&registry, &idx, Some(uid.clone()));
    let fp = FilterGroup::Filter {
        column: "id".to_string(),
        operation: Some(crate::command::types::CompareOp::Eq),
        value: None,
        priority: 0,
        uid: None,
        index_strategy: None,
    };
    let s = planner.choose(&fp, "S1").await;
    assert!(matches!(s, IndexStrategy::ZoneXorIndex { .. }));

    // With only XF
    let mut idx2 = IndexRegistry::new();
    idx2.insert_catalog(make_catalog(&uid, "S2", |c| {
        c.set_field_kind("id", IndexKind::XOR_FIELD_FILTER);
    }));
    let planner2 = IndexPlanner::new(&registry, &idx2, Some(uid.clone()));
    let s2 = planner2.choose(&fp, "S2").await;
    assert!(matches!(s2, IndexStrategy::XorPresence { .. }));

    // With none
    let mut idx3 = IndexRegistry::new();
    idx3.insert_catalog(make_catalog(&uid, "S3", |_c| {}));
    let planner3 = IndexPlanner::new(&registry, &idx3, Some(uid.clone()));
    let s3 = planner3.choose(&fp, "S3").await;
    assert!(matches!(s3, IndexStrategy::FullScan));
}
