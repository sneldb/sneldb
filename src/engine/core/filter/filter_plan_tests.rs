use crate::command::types::{CompareOp, Expr};
use crate::engine::core::FilterPlan;
use crate::test_helpers::factories::{CommandFactory, SchemaRegistryFactory};
use serde_json::json;
use std::collections::HashSet;

#[tokio::test]
async fn builds_filters_with_context_and_timestamp_and_where_clause() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields(
            "login",
            &[
                ("context_id", "string"),
                ("timestamp", "u64"),
                ("ip", "string"),
                ("device", "string"),
            ],
        )
        .await
        .unwrap();

    let cmd = CommandFactory::query()
        .with_event_type("login")
        .with_context_id("ctx123")
        .with_since("123456")
        .with_where_clause(Expr::Compare {
            field: "ip".into(),
            op: CompareOp::Eq,
            value: json!("127.0.0.1"),
        })
        .create();

    let plans = FilterPlan::build_all(&cmd, &registry.registry()).await;

    let columns: HashSet<_> = plans.iter().map(|f| f.column.as_str()).collect();

    // Expect filters on context_id, event_type, timestamp, ip, and default one for "device"
    assert_eq!(
        columns,
        HashSet::from(["context_id", "event_type", "timestamp", "ip", "device"])
    );

    use crate::engine::types::ScalarValue;
    let ip_plan = plans.iter().find(|f| f.column == "ip").unwrap();
    assert_eq!(ip_plan.operation, Some(CompareOp::Eq));
    assert_eq!(ip_plan.value, Some(ScalarValue::from(json!("127.0.0.1"))));
    assert_eq!(ip_plan.priority, 2);
}

#[tokio::test]
async fn builds_filters_with_custom_time_field_using_clause() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields(
            "evt",
            &[
                ("context_id", "string"),
                ("created_at", "datetime"),
                ("x", "string"),
            ],
        )
        .await
        .unwrap();

    let cmd = CommandFactory::query()
        .with_event_type("evt")
        .with_context_id("c1")
        .with_since("2025-01-01T00:00:00Z")
        .with_time_field("created_at")
        .create();

    let plans = FilterPlan::build_all(&cmd, &registry.registry()).await;

    let mut map: std::collections::HashMap<&str, &FilterPlan> = std::collections::HashMap::new();
    for p in &plans {
        map.insert(&p.column, p);
    }

    // context_id and event_type should be present
    assert!(map.contains_key("context_id"));
    assert!(map.contains_key("event_type"));
    // time filter should target created_at (selected time field), not core timestamp
    assert!(map.contains_key("created_at"));
    assert!(!map.contains_key("timestamp"));
    assert_eq!(map["created_at"].operation, Some(CompareOp::Gte));
}

#[tokio::test]
async fn preserves_multiple_where_filters_for_same_column_in_or() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields(
            "subscription",
            &[
                ("plan", "string"),
                ("context_id", "string"),
                ("timestamp", "u64"),
            ],
        )
        .await
        .unwrap();

    let cmd = CommandFactory::query()
        .with_event_type("subscription")
        .with_where_clause(Expr::Or(
            Box::new(Expr::Compare {
                field: "plan".into(),
                op: CompareOp::Eq,
                value: json!("pro"),
            }),
            Box::new(Expr::Compare {
                field: "plan".into(),
                op: CompareOp::Eq,
                value: json!("premium"),
            }),
        ))
        .create();

    let plans = FilterPlan::build_all(&cmd, &registry.registry()).await;

    // Expect two separate FilterPlans for column "plan"
    let plan_filters: Vec<_> = plans.iter().filter(|p| p.column == "plan").collect();
    assert_eq!(plan_filters.len(), 2);

    // Values should be preserved as provided in the OR expression
    let values: HashSet<_> = plan_filters
        .iter()
        .map(|p| p.value.as_ref().unwrap().as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        values,
        HashSet::from(["pro".to_string(), "premium".to_string()])
    );

    // Operations should be Eq for both
    assert!(
        plan_filters
            .iter()
            .all(|p| p.operation == Some(CompareOp::Eq))
    );
}

#[tokio::test]
async fn when_since_missing_time_field_gets_fallback_plan() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields(
            "evt2",
            &[
                ("context_id", "string"),
                ("created_at", "datetime"),
                ("y", "string"),
            ],
        )
        .await
        .unwrap();

    // No SINCE, no WHERE => created_at should appear only as a fallback filter (operation None)
    let cmd = CommandFactory::query()
        .with_event_type("evt2")
        .with_context_id("c2")
        .create();

    let plans = FilterPlan::build_all(&cmd, &registry.registry()).await;

    let created = plans.iter().find(|p| p.column == "created_at").unwrap();
    assert_eq!(created.operation, None);
    assert_eq!(created.priority, 3);
}

#[tokio::test]
async fn using_time_field_with_where_on_same_column_keeps_both_filters() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields(
            "evt3",
            &[
                ("context_id", "string"),
                ("created_at", "datetime"),
                ("a", "string"),
            ],
        )
        .await
        .unwrap();

    let cmd = CommandFactory::query()
        .with_event_type("evt3")
        .with_context_id("c3")
        .with_since("123456")
        .with_time_field("created_at")
        .with_where_clause(Expr::Compare {
            field: "created_at".into(),
            op: CompareOp::Gt,
            value: json!(200000),
        })
        .create();

    let plans = FilterPlan::build_all(&cmd, &registry.registry()).await;

    let created_filters: Vec<_> = plans.iter().filter(|p| p.column == "created_at").collect();
    // Expect two filters on created_at: one from SINCE (Gte), one from WHERE (Gt)
    assert_eq!(created_filters.len(), 2);
    assert!(
        created_filters
            .iter()
            .any(|p| p.operation == Some(CompareOp::Gte))
    );
    assert!(
        created_filters
            .iter()
            .any(|p| p.operation == Some(CompareOp::Gt))
    );
}

#[tokio::test]
async fn using_timestamp_explicit_targets_core_timestamp() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields(
            "evt4",
            &[
                ("context_id", "string"),
                ("created_at", "datetime"),
                ("a", "string"),
            ],
        )
        .await
        .unwrap();

    let cmd = CommandFactory::query()
        .with_event_type("evt4")
        .with_context_id("c4")
        .with_since("123")
        .with_time_field("timestamp")
        .create();

    let plans = FilterPlan::build_all(&cmd, &registry.registry()).await;
    let map: std::collections::HashMap<_, _> =
        plans.iter().map(|p| (p.column.as_str(), p)).collect();
    assert!(map.contains_key("timestamp"));
    assert_eq!(map["timestamp"].operation, Some(CompareOp::Gte));
    // Should not carry SINCE condition on created_at
    if let Some(created) = map.get("created_at") {
        assert_eq!(created.operation, None);
        assert_eq!(created.priority, 3);
    }
}

#[tokio::test]
async fn default_without_time_field_uses_timestamp() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields("evt5", &[("context_id", "string"), ("x", "string")])
        .await
        .unwrap();

    let cmd = CommandFactory::query()
        .with_event_type("evt5")
        .with_context_id("c5")
        .with_since("9999")
        .create();

    let plans = FilterPlan::build_all(&cmd, &registry.registry()).await;
    let map: std::collections::HashMap<_, _> =
        plans.iter().map(|p| (p.column.as_str(), p)).collect();
    assert!(map.contains_key("timestamp"));
    assert_eq!(map["timestamp"].operation, Some(CompareOp::Gte));
}

#[tokio::test]
async fn temporal_where_literal_string_normalized_to_epoch() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields(
            "evt_norm_ts",
            &[
                ("context_id", "string"),
                ("created_at", "datetime"),
                ("x", "string"),
            ],
        )
        .await
        .unwrap();

    let cmd = CommandFactory::query()
        .with_event_type("evt_norm_ts")
        .with_where_clause(Expr::Compare {
            field: "created_at".into(),
            op: CompareOp::Gte,
            value: json!("2025-01-01T00:00:00Z"),
        })
        .create();

    let plans = FilterPlan::build_all(&cmd, &registry.registry()).await;
    let p = plans.iter().find(|p| p.column == "created_at").unwrap();
    assert_eq!(p.operation, Some(CompareOp::Gte));
    let v = p.value.as_ref().unwrap();
    assert!(v.as_i64().is_some() || v.as_u64().is_some() || v.as_f64().is_some(), "expected normalized epoch seconds number");
    assert_eq!(v.as_i64().unwrap(), 1_735_689_600); // 2025-01-01T00:00:00Z
}

#[tokio::test]
async fn date_where_literal_string_normalized_to_epoch_midnight() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields(
            "evt_norm_date",
            &[
                ("context_id", "string"),
                ("due_date", "date"),
                ("y", "string"),
            ],
        )
        .await
        .unwrap();

    let cmd = CommandFactory::query()
        .with_event_type("evt_norm_date")
        .with_where_clause(Expr::Compare {
            field: "due_date".into(),
            op: CompareOp::Eq,
            value: json!("2025-01-02"),
        })
        .create();

    let plans = FilterPlan::build_all(&cmd, &registry.registry()).await;
    let p = plans.iter().find(|p| p.column == "due_date").unwrap();
    assert_eq!(p.operation, Some(CompareOp::Eq));
    let v = p.value.as_ref().unwrap();
    assert!(v.as_i64().is_some() || v.as_u64().is_some() || v.as_f64().is_some(), "expected normalized epoch seconds number");
    assert_eq!(v.as_i64().unwrap(), 1_735_776_000); // 2025-01-02T00:00:00Z
}

#[tokio::test]
async fn non_temporal_where_literal_kept_as_string() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields(
            "evt_non_temp",
            &[("context_id", "string"), ("name", "string"), ("x", "i64")],
        )
        .await
        .unwrap();

    let cmd = CommandFactory::query()
        .with_event_type("evt_non_temp")
        .with_where_clause(Expr::Compare {
            field: "name".into(),
            op: CompareOp::Eq,
            value: json!("A"),
        })
        .create();

    let plans = FilterPlan::build_all(&cmd, &registry.registry()).await;
    let p = plans.iter().find(|p| p.column == "name").unwrap();
    assert_eq!(p.operation, Some(CompareOp::Eq));
    let v = p.value.as_ref().unwrap();
    assert!(v.as_str().is_some());
    assert_eq!(v.as_str().unwrap(), "A");
}

#[tokio::test]
async fn custom_time_field_without_since_keeps_time_filter_over_fallback() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields(
            "evt_custom_time",
            &[
                ("context_id", "string"),
                ("created_at", "datetime"),
                ("b", "string"),
            ],
        )
        .await
        .unwrap();

    // No since, but explicit time_field = created_at
    let cmd = CommandFactory::query()
        .with_event_type("evt_custom_time")
        .with_time_field("created_at")
        .create();

    let plans = FilterPlan::build_all(&cmd, &registry.registry()).await;

    // There should be a single created_at filter from time_field (Gte, value None), not a fallback
    let created_filters: Vec<_> = plans.iter().filter(|p| p.column == "created_at").collect();
    assert_eq!(created_filters.len(), 1);
    assert_eq!(created_filters[0].operation, Some(CompareOp::Gte));
    assert!(created_filters[0].value.is_none());
    assert_eq!(created_filters[0].priority, 1);

    // Field 'b' should still receive a fallback
    assert!(
        plans
            .iter()
            .any(|p| p.column == "b" && p.operation.is_none())
    );
}

#[tokio::test]
async fn no_fallback_for_fields_present_in_where_clause() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields(
            "evt_where",
            &[
                ("context_id", "string"),
                ("timestamp", "u64"),
                ("a", "string"),
                ("b", "string"),
            ],
        )
        .await
        .unwrap();

    let cmd = CommandFactory::query()
        .with_event_type("evt_where")
        .with_where_clause(Expr::Compare {
            field: "a".into(),
            op: CompareOp::Eq,
            value: json!(1),
        })
        .create();

    let plans = FilterPlan::build_all(&cmd, &registry.registry()).await;

    // No fallback for 'a' since it's referenced in WHERE
    assert!(
        !plans
            .iter()
            .any(|p| p.column == "a" && p.operation.is_none())
    );
    // But 'b' should have a fallback
    assert!(
        plans
            .iter()
            .any(|p| p.column == "b" && p.operation.is_none())
    );
}

#[tokio::test]
async fn preserves_multiple_where_filters_for_same_column_in_and() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields(
            "subscription2",
            &[
                ("plan", "string"),
                ("context_id", "string"),
                ("timestamp", "u64"),
            ],
        )
        .await
        .unwrap();

    let cmd = CommandFactory::query()
        .with_event_type("subscription2")
        .with_where_clause(Expr::And(
            Box::new(Expr::Compare {
                field: "plan".into(),
                op: CompareOp::Eq,
                value: json!("pro"),
            }),
            Box::new(Expr::Compare {
                field: "plan".into(),
                op: CompareOp::Eq,
                value: json!("premium"),
            }),
        ))
        .create();

    let plans = FilterPlan::build_all(&cmd, &registry.registry()).await;

    let plan_filters: Vec<_> = plans.iter().filter(|p| p.column == "plan").collect();
    assert_eq!(plan_filters.len(), 2);
    let values: std::collections::HashSet<_> = plan_filters
        .iter()
        .map(|p| p.value.as_ref().unwrap().as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        values,
        HashSet::from(["pro".to_string(), "premium".to_string()])
    );
}

#[tokio::test]
async fn using_timestamp_with_where_on_timestamp_keeps_both_filters() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields(
            "evt_ts",
            &[
                ("context_id", "string"),
                ("timestamp", "u64"),
                ("a", "string"),
            ],
        )
        .await
        .unwrap();

    let cmd = CommandFactory::query()
        .with_event_type("evt_ts")
        .with_since("1000")
        .with_where_clause(Expr::Compare {
            field: "timestamp".into(),
            op: CompareOp::Lt,
            value: json!(2000),
        })
        .create();

    let plans = FilterPlan::build_all(&cmd, &registry.registry()).await;

    let ts_filters: Vec<_> = plans.iter().filter(|p| p.column == "timestamp").collect();
    assert_eq!(ts_filters.len(), 2);
    assert!(
        ts_filters
            .iter()
            .any(|p| p.operation == Some(CompareOp::Gte))
    );
    assert!(
        ts_filters
            .iter()
            .any(|p| p.operation == Some(CompareOp::Lt))
    );
}
