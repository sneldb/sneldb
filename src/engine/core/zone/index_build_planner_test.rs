use crate::engine::core::read::catalog::{IndexKind, SegmentIndexCatalog};
use crate::engine::core::zone::{
    index_build_planner::IndexBuildPlanner, index_build_policy::IndexBuildPolicy,
};
use crate::engine::schema::registry::MiniSchema;
use crate::engine::schema::types::{EnumType, FieldType};
use std::collections::HashMap;

fn schema(fields: Vec<(&str, FieldType)>) -> MiniSchema {
    let mut s = MiniSchema {
        fields: HashMap::new(),
    };
    for (name, ty) in fields {
        s.fields.insert(name.to_string(), ty);
    }
    s
}

#[test]
fn planner_includes_core_fields_with_expected_kinds() {
    let sch = schema(vec![]);
    let policy = IndexBuildPolicy::default();
    let planner = IndexBuildPlanner::new("uidA", "00001", &sch, policy);
    let plan = planner.plan();

    // Core fields always present
    assert!(plan.per_field.contains_key("event_type"));
    assert!(plan.per_field.contains_key("context_id"));
    assert!(plan.per_field.contains_key("timestamp"));

    // Check categories mapping
    let et = plan.per_field.get("event_type").copied().unwrap();
    assert_eq!(et, IndexKind::XOR_FIELD_FILTER | IndexKind::ZONE_XOR_INDEX);

    let ctx = plan.per_field.get("context_id").copied().unwrap();
    assert_eq!(ctx, IndexKind::XOR_FIELD_FILTER | IndexKind::ZONE_XOR_INDEX);

    let ts = plan.per_field.get("timestamp").copied().unwrap();
    let expected_ts = IndexKind::TS_CALENDAR
        | IndexKind::TS_ZTI
        | IndexKind::FIELD_CALENDAR
        | IndexKind::FIELD_ZTI;
    assert_eq!(ts, expected_ts);

    // Global kinds: ZONE_INDEX always, RLTE conditional via policy for primitives only
    assert!(plan.global.contains(IndexKind::ZONE_INDEX));
}

#[test]
fn planner_adds_schema_fields_by_category() {
    let sch = schema(vec![
        ("id", FieldType::I64),
        ("price", FieldType::F64),
        (
            "kind",
            FieldType::Enum(EnumType {
                variants: vec!["A".to_string(), "B".to_string()],
            }),
        ),
        ("created_at", FieldType::Timestamp),
    ]);
    let policy = IndexBuildPolicy {
        enable_rlte_for_primitives: true,
    };
    let planner = IndexBuildPlanner::new("u", "00002", &sch, policy);
    let plan = planner.plan();

    // Enum field => ENUM_BITMAP
    let ek = plan.per_field.get("kind").copied().unwrap();
    assert_eq!(ek, IndexKind::ENUM_BITMAP);

    // Primitive numeric => SURF | ZXF | XF (+ RLTE because policy enabled)
    let prim_expected = IndexKind::ZONE_SURF
        | IndexKind::ZONE_XOR_INDEX
        | IndexKind::XOR_FIELD_FILTER
        | IndexKind::RLTE;
    assert_eq!(plan.per_field.get("id").copied().unwrap(), prim_expected);
    assert_eq!(plan.per_field.get("price").copied().unwrap(), prim_expected);

    // Temporal (non-core) should get FIELD_* bits (plus TS_* because categorize returns Temporal)
    let created = plan.per_field.get("created_at").copied().unwrap();
    let expected_ts = IndexKind::TS_CALENDAR
        | IndexKind::TS_ZTI
        | IndexKind::FIELD_CALENDAR
        | IndexKind::FIELD_ZTI;
    assert_eq!(created, expected_ts);

    // Global should include ZONE_INDEX and RLTE due to policy
    assert!(plan.global.contains(IndexKind::ZONE_INDEX));
    assert!(plan.global.contains(IndexKind::RLTE));
}

#[test]
fn planner_respects_policy_disabling_rlte_for_primitives() {
    let sch = schema(vec![("val", FieldType::I64)]);
    let policy = IndexBuildPolicy {
        enable_rlte_for_primitives: false,
    };
    let planner = IndexBuildPlanner::new("u", "00003", &sch, policy);
    let plan = planner.plan();

    let prim = plan.per_field.get("val").copied().unwrap();
    let expected = IndexKind::ZONE_SURF | IndexKind::ZONE_XOR_INDEX | IndexKind::XOR_FIELD_FILTER;
    assert_eq!(prim, expected);
    assert!(plan.global.contains(IndexKind::ZONE_INDEX));
    assert!(!plan.global.contains(IndexKind::RLTE));
}

#[test]
fn to_catalog_reflects_plan_exactly() {
    let sch = schema(vec![("n", FieldType::I64)]);
    let policy = IndexBuildPolicy::default();
    let planner = IndexBuildPlanner::new("uidZ", "00009", &sch, policy);
    let plan = planner.plan();
    let catalog: SegmentIndexCatalog = planner.to_catalog(&plan);

    // Verify identifiers
    assert_eq!(catalog.uid, "uidZ");
    assert_eq!(catalog.segment_id, "00009");

    // Verify field kinds match the plan's per_field entries
    for (field, kinds) in plan.per_field.iter() {
        assert_eq!(
            catalog.field_kinds.get(field).copied().unwrap(),
            *kinds,
            "field {} mismatch",
            field
        );
    }
    // Verify global kinds match
    assert_eq!(catalog.global_kinds, plan.global);
}
