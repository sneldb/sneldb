use crate::command::types::{Command, Expr};
use crate::engine::core::read::catalog::{IndexKind, SegmentIndexCatalog};
use crate::engine::schema::registry::{MiniSchema, SchemaRegistry};
use crate::engine::schema::types::FieldType;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

async fn registry_with_schema_at(path: PathBuf) -> Arc<RwLock<SchemaRegistry>> {
    let mut reg = SchemaRegistry::new_with_path(path).expect("registry");
    let mut fields: HashMap<String, FieldType> = HashMap::new();
    fields.insert("id".to_string(), FieldType::I64);
    fields.insert("timestamp".to_string(), FieldType::Timestamp);
    let schema = MiniSchema { fields };
    reg.define("ev", schema).expect("define");
    Arc::new(RwLock::new(reg))
}

#[tokio::test]
async fn query_plan_new_assigns_strategies_using_segment_with_catalog() {
    let tmp = tempfile::tempdir().unwrap();
    let schema_path = tmp.path().join("schemas.bin");
    let registry = registry_with_schema_at(schema_path).await;

    let base_dir = tempfile::tempdir().unwrap();
    let seg_ids = Arc::new(std::sync::RwLock::new(vec![
        "S1".to_string(),
        "S2".to_string(),
    ]));

    let uid = registry.read().await.get_uid("ev").unwrap();
    let mut cat = SegmentIndexCatalog::new(uid.clone(), "S2".to_string());
    cat.add_global_kind(IndexKind::ZONE_INDEX);
    let icx = base_dir.path().join("S2").join(format!("{}.icx", uid));
    std::fs::create_dir_all(icx.parent().unwrap()).unwrap();
    cat.save(&icx).unwrap();

    let cmd = crate::test_helpers::factories::command_factory::CommandFactory::query()
        .with_event_type("ev")
        .with_where_clause(Expr::Compare {
            field: "id".to_string(),
            op: crate::command::types::CompareOp::Eq,
            value: serde_json::json!(1),
        })
        .create();

    let plan = crate::engine::core::read::query_plan::QueryPlan::new(
        cmd,
        &registry,
        base_dir.path(),
        &seg_ids,
    )
    .await
    .unwrap();

    assert!(!plan.filter_plans.is_empty());
    assert!(
        plan.filter_plans
            .iter()
            .all(|fp| fp.index_strategy.is_some())
    );
}

#[tokio::test]
async fn query_plan_assigns_fullscan_when_no_catalogs() {
    let tmp = tempfile::tempdir().unwrap();
    let schema_path = tmp.path().join("schemas.bin");
    let registry = registry_with_schema_at(schema_path).await;

    let base_dir = tempfile::tempdir().unwrap();
    let seg_ids = Arc::new(std::sync::RwLock::new(vec!["Sx".to_string()]));

    let cmd = crate::test_helpers::factories::command_factory::CommandFactory::query()
        .with_event_type("ev")
        .create();

    let plan = crate::engine::core::read::query_plan::QueryPlan::new(
        cmd,
        &registry,
        base_dir.path(),
        &seg_ids,
    )
    .await
    .unwrap();

    use crate::engine::core::read::index_strategy::IndexStrategy;
    assert!(
        plan.filter_plans
            .iter()
            .all(|fp| matches!(fp.index_strategy, Some(IndexStrategy::FullScan)))
    );
}

#[tokio::test]
async fn query_plan_with_no_segments_skips_strategy_assignment() {
    let tmp = tempfile::tempdir().unwrap();
    let schema_path = tmp.path().join("schemas.bin");
    let registry = registry_with_schema_at(schema_path).await;

    let base_dir = tempfile::tempdir().unwrap();
    let seg_ids = Arc::new(std::sync::RwLock::new(Vec::<String>::new()));

    let cmd = crate::test_helpers::factories::command_factory::CommandFactory::query()
        .with_event_type("ev")
        .create();

    let plan = crate::engine::core::read::query_plan::QueryPlan::new(
        cmd,
        &registry,
        base_dir.path(),
        &seg_ids,
    )
    .await
    .unwrap();

    assert!(
        plan.filter_plans
            .iter()
            .all(|fp| fp.index_strategy.is_none())
    );
}

#[tokio::test]
async fn query_plan_aggregates_remove_implicit_since_filter() {
    let tmp = tempfile::tempdir().unwrap();
    let schema_path = tmp.path().join("schemas.bin");
    let registry = registry_with_schema_at(schema_path).await;

    let base_dir = tempfile::tempdir().unwrap();
    let seg_ids = Arc::new(std::sync::RwLock::new(vec!["S1".to_string()]));

    // Query with since and aggregation → implicit time filter should be removed
    let cmd = crate::test_helpers::factories::command_factory::CommandFactory::query()
        .with_event_type("ev")
        .with_since("2020-01-01T00:00:00Z")
        .add_count()
        .create();

    let plan = crate::engine::core::read::query_plan::QueryPlan::new(
        cmd,
        &registry,
        base_dir.path(),
        &seg_ids,
    )
    .await
    .unwrap();

    // No Gte filter for the time field should remain
    let has_implicit_since = plan.filter_plans.iter().any(|fp| {
        fp.column == "timestamp"
            && matches!(fp.operation, Some(crate::command::types::CompareOp::Gte))
            && fp
                .value
                .as_ref()
                .and_then(|v| v.as_str().map(|s| s.to_string()))
                == Some("2020-01-01T00:00:00Z".to_string())
    });
    assert!(!has_implicit_since);
}
