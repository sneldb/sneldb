use crate::command::types::{CompareOp, Expr};
use crate::engine::core::ExecutionStep;
use crate::engine::core::read::index_strategy::IndexStrategy;
use crate::engine::core::zone::zone_index::ZoneIndex;
use crate::test_helpers::factories::{
    CommandFactory, FieldXorFilterFactory, FilterGroupFactory, QueryPlanFactory,
    SchemaRegistryFactory,
};
use serde_json::json;
use std::fs;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
// Resolves zones for a simple equality filter over a single segment
async fn execution_step_resolves_candidate_zones_from_filter_and_plan() {
    use crate::logging::init_for_tests;
    init_for_tests();

    // Setup: Create a temporary segment directory for the test
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let segment_path = temp_dir.path().join("seg1");
    fs::create_dir_all(&segment_path).expect("Failed to create segment directory");

    // Step 1: Prepare a dummy schema registry
    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();
    let event_type = "ex_step_event";
    registry_factory
        .define_with_fields(event_type, &[("plan", "string")])
        .await
        .expect("Schema definition failed");

    // Step 2: Create a Command::Query using CommandFactory
    let where_clause = Expr::Compare {
        field: "plan".to_string(),
        op: CompareOp::Eq,
        value: json!("premium"),
    };
    let uid = registry
        .read()
        .await
        .get_uid(event_type)
        .expect("UID not found for 'ex_step_event'");

    let command = CommandFactory::query()
        .with_event_type(event_type)
        .with_context_id("ctx1")
        .with_limit(5)
        .with_where_clause(where_clause.clone())
        .create();

    // Step 3: Build the QueryPlan
    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(&temp_dir)
        .with_segment_ids(vec!["seg1".to_string()])
        .create()
        .await;

    // Step 4: Create FilterGroup
    let mut filter = FilterGroupFactory::new()
        .with_column("plan")
        .with_operation(CompareOp::Eq)
        .with_value(json!("premium"))
        .with_uid(&uid)
        .with_priority(2)
        .create();
    if let Some(strategy) = filter.index_strategy_mut() {
        *strategy = Some(IndexStrategy::XorPresence {
            field: "plan".to_string(),
        });
    }

    let xor_filter = FieldXorFilterFactory::new().with(json!("premium")).build();

    let xf_path = segment_path.join(format!("{}_plan.xf", uid));
    xor_filter
        .save(&xf_path)
        .expect("Failed to save xor filter");

    // Step 5: Create ExecutionStep and resolve candidate zones
    let mut step = ExecutionStep::new(filter, &plan);
    step.get_candidate_zones_with_segments(None, &["seg1".to_string()]);

    // Step 6: Validate
    assert!(
        !step.candidate_zones.is_empty(),
        "Expected candidate zones, but found none"
    );

    assert_eq!(
        step.candidate_zones[0].segment_id, "seg1",
        "Expected segment_id to be 'seg1'"
    );
}

#[tokio::test]
// Uses provided segment subset to limit where zones are resolved
async fn execution_step_respects_provided_segment_subset() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let temp_dir = tempdir().expect("Failed to create temp dir");
    let seg1 = temp_dir.path().join("seg1");
    let seg2 = temp_dir.path().join("seg2");
    fs::create_dir_all(&seg1).unwrap();
    fs::create_dir_all(&seg2).unwrap();

    // Schema and registry
    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();
    let event_type = "ex_subset_event";
    registry_factory
        .define_with_fields(event_type, &[("plan", "string")])
        .await
        .unwrap();
    let uid = registry.read().await.get_uid(event_type).unwrap();

    // Build plan
    let command = CommandFactory::query()
        .with_event_type(event_type)
        .with_context_id("ctx1")
        .create();
    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(&temp_dir)
        .with_segment_ids(vec!["seg1".into(), "seg2".into()])
        .create()
        .await;

    // Create XOR filter only in seg1
    let xor_filter = FieldXorFilterFactory::new().with(json!("yes")).build();
    xor_filter
        .save(&seg1.join(format!("{}_{}.xf", uid, "plan")))
        .expect("save xf");

    // Filter plan (plan == "yes")
    let mut filter = FilterGroupFactory::new()
        .with_column("plan")
        .with_operation(CompareOp::Eq)
        .with_value(json!("yes"))
        .with_uid(&uid)
        .create();
    if let Some(strategy) = filter.index_strategy_mut() {
        *strategy = Some(IndexStrategy::XorPresence {
            field: "plan".to_string(),
        });
    }

    let mut step = ExecutionStep::new(filter, &plan);

    // Only seg1 yields zones
    step.get_candidate_zones_with_segments(None, &["seg1".into()]);
    assert!(!step.candidate_zones.is_empty());
    assert!(step.candidate_zones.iter().all(|z| z.segment_id == "seg1"));

    // Only seg2 yields none
    let f2 = FilterGroupFactory::new()
        .with_column("plan")
        .with_operation(CompareOp::Eq)
        .with_value(json!("yes"))
        .with_uid(&uid)
        .create();
    let mut step2 = ExecutionStep::new(f2, &plan);
    step2.get_candidate_zones_with_segments(None, &["seg2".into()]);
    assert!(step2.candidate_zones.is_empty());
}

#[tokio::test]
// Empty segment list yields no candidate zones
async fn execution_step_empty_segments_list_yields_no_zones() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let temp_dir = tempdir().unwrap();
    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();
    let event_type = "ex_empty";
    registry_factory
        .define_with_fields(event_type, &[("plan", "string")])
        .await
        .unwrap();
    let uid = registry.read().await.get_uid(event_type).unwrap();

    let command = CommandFactory::query().with_event_type(event_type).create();
    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(&temp_dir)
        .with_segment_ids(vec![])
        .create()
        .await;

    let mut filter = FilterGroupFactory::new()
        .with_column("plan")
        .with_operation(CompareOp::Eq)
        .with_value(json!("x"))
        .with_uid(&uid)
        .create();
    if let Some(strategy) = filter.index_strategy_mut() {
        *strategy = Some(IndexStrategy::XorPresence {
            field: "plan".to_string(),
        });
    }

    let mut step = ExecutionStep::new(filter, &plan);
    step.get_candidate_zones_with_segments(None, &[]);
    assert!(step.candidate_zones.is_empty());
}

#[tokio::test]
// Aggregates zones across multiple segments when both match
async fn execution_step_multiple_segments_aggregate_zones() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let temp_dir = tempdir().unwrap();
    let seg1 = temp_dir.path().join("seg1");
    let seg2 = temp_dir.path().join("seg2");
    fs::create_dir_all(&seg1).unwrap();
    fs::create_dir_all(&seg2).unwrap();

    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();
    let event_type = "ex_multi";
    registry_factory
        .define_with_fields(event_type, &[("plan", "string")])
        .await
        .unwrap();
    let uid = registry.read().await.get_uid(event_type).unwrap();

    // Put filter in both segments
    let xor = FieldXorFilterFactory::new().with(json!("y")).build();
    xor.save(&seg1.join(format!("{}_{}.xf", uid, "plan")))
        .unwrap();
    xor.save(&seg2.join(format!("{}_{}.xf", uid, "plan")))
        .unwrap();

    let command = CommandFactory::query().with_event_type(event_type).create();
    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(&temp_dir)
        .with_segment_ids(vec!["seg1".into(), "seg2".into()])
        .create()
        .await;

    let mut filter = FilterGroupFactory::new()
        .with_column("plan")
        .with_operation(CompareOp::Eq)
        .with_value(json!("y"))
        .with_uid(&uid)
        .create();
    if let Some(strategy) = filter.index_strategy_mut() {
        *strategy = Some(IndexStrategy::XorPresence {
            field: "plan".to_string(),
        });
    }
    let mut step = ExecutionStep::new(filter, &plan);
    step.get_candidate_zones_with_segments(None, &["seg1".into(), "seg2".into()]);
    assert!(step.candidate_zones.len() >= 2);
}

#[tokio::test]
// Subset test for context_id: seg1 has ctxA; seg2 has ctxB (expect hits only on seg1)
async fn execution_step_context_id_respects_subset() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let temp_dir = tempdir().unwrap();
    let seg1 = temp_dir.path().join("seg1");
    let seg2 = temp_dir.path().join("seg2");
    fs::create_dir_all(&seg1).unwrap();
    fs::create_dir_all(&seg2).unwrap();

    // Schema
    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();
    let event_type = "ex_ctx";
    registry_factory
        .define_with_fields(event_type, &[("context_id", "string")])
        .await
        .unwrap();
    let uid = registry.read().await.get_uid(event_type).unwrap();

    // Write ZoneIndex only in seg1 for ctxA, and a non-matching index in seg2
    {
        use crate::test_helpers::factories::zone_index_factory::ZoneIndexFactory;
        let mut fac = ZoneIndexFactory::new();
        fac = fac.with_entry(event_type, "ctxA", 0);
        let idx: ZoneIndex = fac.create();
        idx.write_to_path(seg1.join(format!("{}.idx", uid)))
            .unwrap();
    }
    {
        use crate::test_helpers::factories::zone_index_factory::ZoneIndexFactory;
        let mut fac = ZoneIndexFactory::new();
        fac = fac.with_entry(event_type, "ctxB", 0);
        let idx: ZoneIndex = fac.create();
        idx.write_to_path(seg2.join(format!("{}.idx", uid)))
            .unwrap();
    }

    let command = CommandFactory::query()
        .with_event_type(event_type)
        .with_context_id("ctxA")
        .create();
    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(&temp_dir)
        .with_segment_ids(vec!["seg1".into(), "seg2".into()])
        .create()
        .await;

    let filter = FilterGroupFactory::new()
        .with_column("context_id")
        .with_operation(CompareOp::Eq)
        .with_value(json!("ctxA"))
        .with_uid(&uid)
        .create();

    let mut step = ExecutionStep::new(filter, &plan);
    // Expect hits when restricted to seg1, and all zones to belong to seg1
    step.get_candidate_zones_with_segments(None, &["seg1".into()]);
    assert!(!step.candidate_zones.is_empty());
    assert!(step.candidate_zones.iter().all(|z| z.segment_id == "seg1"));

    let mut step_none = ExecutionStep::new(
        FilterGroupFactory::new()
            .with_column("context_id")
            .with_operation(CompareOp::Eq)
            .with_value(json!("ctxA"))
            .with_uid(&uid)
            .create(),
        &plan,
    );
    // Expect no hits when restricted to seg2, because ctxA is absent there
    step_none.get_candidate_zones_with_segments(None, &["seg2".into()]);
    assert!(step_none.candidate_zones.is_empty());
}
