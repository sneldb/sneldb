use crate::command::types::{CompareOp, Expr};
use crate::engine::core::ConditionEvaluatorBuilder;
use crate::test_helpers::factories::{
    CommandFactory, EventFactory, QueryPlanFactory, SchemaRegistryFactory,
};
use serde_json::json;

#[tokio::test]
async fn builds_evaluator_from_command_factory_and_filters_correctly() {
    let command = CommandFactory::query()
        .with_event_type("test_event")
        .with_context_id("ctx9")
        .with_since("123000")
        .with_where_clause(Expr::And(
            Box::new(Expr::Compare {
                field: "amount".into(),
                op: CompareOp::Gte,
                value: json!(100),
            }),
            Box::new(Expr::Compare {
                field: "status".into(),
                op: CompareOp::Eq,
                value: json!("confirmed"),
            }),
        ))
        .create();

    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();
    let plan = QueryPlanFactory::new()
        .with_command(command.clone())
        .with_registry(registry)
        .create()
        .await;

    let mut builder = ConditionEvaluatorBuilder::new();
    builder.add_special_fields(&plan);

    if let Some(expr) = match &command {
        crate::command::types::Command::Query { where_clause, .. } => where_clause.as_ref(),
        _ => None,
    } {
        builder.add_where_clause(expr);
    }

    let evaluator = builder.into_evaluator();

    let passing = EventFactory::new()
        .with("event_type", "test_event")
        .with("context_id", "ctx9")
        .with("timestamp", 123456)
        .with("payload", json!({ "amount": 150, "status": "confirmed" }))
        .create();

    let failing = EventFactory::new()
        .with("event_type", "test_event")
        .with("context_id", "ctx9")
        .with("timestamp", 123456)
        .with("payload", json!({ "amount": 50, "status": "failed" }))
        .create();

    assert!(evaluator.evaluate_event(&passing));
    assert!(!evaluator.evaluate_event(&failing));
}

#[tokio::test]
async fn adds_time_condition_on_selected_time_field_with_iso8601_since() {
    // Define a schema with a logical datetime field "created_at"
    let registry_factory = SchemaRegistryFactory::new();
    registry_factory
        .define_with_fields("evt_time", &[("id", "int"), ("created_at", "datetime")])
        .await
        .unwrap();
    let registry = registry_factory.registry();

    // Build a query that uses SINCE with ISO-8601 and selects created_at as time field
    let command = CommandFactory::query()
        .with_event_type("evt_time")
        .with_context_id("ctx1")
        .with_since("2025-01-01T00:00:00Z")
        .with_time_field("created_at")
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(registry.clone())
        .create()
        .await;

    let evaluator = ConditionEvaluatorBuilder::build_from_plan(&plan);

    // Compute epoch seconds for boundaries
    let since_epoch = chrono::DateTime::parse_from_rfc3339("2025-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc)
        .timestamp();
    let before = since_epoch - 1;
    let after = since_epoch + 1;

    // Event older than since on created_at should fail
    let old = EventFactory::new()
        .with("event_type", "evt_time")
        .with("context_id", "ctx1")
        .with("timestamp", 1) // core timestamp irrelevant when USING created_at
        .with("payload", json!({ "id": 1, "created_at": before }))
        .create();
    assert!(!evaluator.evaluate_event(&old));

    // Event newer than since on created_at should pass
    let new = EventFactory::new()
        .with("event_type", "evt_time")
        .with("context_id", "ctx1")
        .with("timestamp", 1)
        .with("payload", json!({ "id": 2, "created_at": after }))
        .create();
    assert!(evaluator.evaluate_event(&new));
}

#[tokio::test]
async fn evaluates_and_expression_correctly() {
    // amount >= 100 AND status == "confirmed"
    let expr = Expr::And(
        Box::new(Expr::Compare {
            field: "amount".into(),
            op: CompareOp::Gte,
            value: json!(100),
        }),
        Box::new(Expr::Compare {
            field: "status".into(),
            op: CompareOp::Eq,
            value: json!("confirmed"),
        }),
    );

    let command = CommandFactory::query()
        .with_context_id("ctx1")
        .with_event_type("test_event")
        .with_where_clause(expr)
        .with_since("123000")
        .create();

    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();
    let plan = QueryPlanFactory::new()
        .with_command(command.clone())
        .with_registry(registry)
        .create()
        .await;

    let mut builder = ConditionEvaluatorBuilder::new();
    builder.add_special_fields(&plan);
    if let Some(expr) = plan.where_clause() {
        builder.add_where_clause(expr);
    }

    let evaluator = builder.into_evaluator();

    let passing = crate::test_helpers::factories::EventFactory::new()
        .with("context_id", "ctx1")
        .with("timestamp", 123456)
        .with("event_type", "test_event")
        .with("payload", json!({ "amount": 150, "status": "confirmed" }))
        .create();

    let failing = crate::test_helpers::factories::EventFactory::new()
        .with("context_id", "ctx1")
        .with("timestamp", 123456)
        .with("event_type", "test_event")
        .with("payload", json!({ "amount": 50, "status": "failed" }))
        .create();

    assert!(evaluator.evaluate_event(&passing));
    assert!(!evaluator.evaluate_event(&failing));
}

#[tokio::test]
async fn evaluates_or_expression_correctly() {
    // amount >= 100 OR status == "confirmed"
    let expr = Expr::Or(
        Box::new(Expr::Compare {
            field: "amount".into(),
            op: CompareOp::Gte,
            value: json!(100),
        }),
        Box::new(Expr::Compare {
            field: "status".into(),
            op: CompareOp::Eq,
            value: json!("confirmed"),
        }),
    );

    let command = CommandFactory::query()
        .with_context_id("ctx1")
        .with_event_type("test_event")
        .with_where_clause(expr)
        .with_since("123000")
        .create();

    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();
    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(registry.clone())
        .create()
        .await;

    let mut builder = ConditionEvaluatorBuilder::new();
    builder.add_special_fields(&plan);
    if let Some(expr) = plan.where_clause() {
        builder.add_where_clause(expr);
    }

    let evaluator = builder.into_evaluator();

    let match_first = crate::test_helpers::factories::EventFactory::new()
        .with("context_id", "ctx1")
        .with("timestamp", 123456)
        .with("event_type", "test_event")
        .with("payload", json!({ "amount": 150, "status": "pending" }))
        .create();

    let match_second = crate::test_helpers::factories::EventFactory::new()
        .with("context_id", "ctx1")
        .with("timestamp", 123456)
        .with("event_type", "test_event")
        .with("payload", json!({ "amount": 50, "status": "confirmed" }))
        .create();

    let fail_both = crate::test_helpers::factories::EventFactory::new()
        .with("context_id", "ctx1")
        .with("timestamp", 123456)
        .with("event_type", "test_event")
        .with("payload", json!({ "amount": 50, "status": "failed" }))
        .create();

    assert!(evaluator.evaluate_event(&match_first));
    assert!(evaluator.evaluate_event(&match_second));
    assert!(!evaluator.evaluate_event(&fail_both));
}

#[tokio::test]
async fn evaluates_not_expression_correctly() {
    // NOT (status == "confirmed")
    let expr = Expr::Not(Box::new(Expr::Compare {
        field: "status".into(),
        op: CompareOp::Eq,
        value: json!("confirmed"),
    }));

    let command = CommandFactory::query()
        .with_context_id("ctx1")
        .with_event_type("test_event")
        .with_where_clause(expr)
        .with_since("123000")
        .create();

    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();
    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(registry.clone())
        .create()
        .await;

    let mut builder = ConditionEvaluatorBuilder::new();
    builder.add_special_fields(&plan);
    if let Some(expr) = plan.where_clause() {
        builder.add_where_clause(expr);
    }

    let evaluator = builder.into_evaluator();

    let pass_event = crate::test_helpers::factories::EventFactory::new()
        .with("context_id", "ctx1")
        .with("timestamp", 123456)
        .with("event_type", "test_event")
        .with("payload", json!({ "status": "failed" }))
        .create();

    let fail_event = crate::test_helpers::factories::EventFactory::new()
        .with("context_id", "ctx1")
        .with("timestamp", 123456)
        .with("event_type", "test_event")
        .with("payload", json!({ "status": "confirmed" }))
        .create();

    assert!(evaluator.evaluate_event(&pass_event));
    assert!(!evaluator.evaluate_event(&fail_event));
}

#[tokio::test]
async fn build_from_plan_adds_special_fields_when_not_aggregated() {
    let command = CommandFactory::query()
        .with_event_type("evt")
        .with_context_id("ctxA")
        .with_since("100")
        .create();

    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();
    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(registry)
        .create()
        .await;

    // Use build_from_plan to implicitly add special fields
    let evaluator = ConditionEvaluatorBuilder::build_from_plan(&plan);

    // Matching special fields -> pass
    let pass = EventFactory::new()
        .with("event_type", "evt")
        .with("context_id", "ctxA")
        .with("timestamp", 150)
        .with("payload", json!({}))
        .create();
    assert!(evaluator.evaluate_event(&pass));

    // Wrong event_type -> fail
    let wrong_evt = EventFactory::new()
        .with("event_type", "other")
        .with("context_id", "ctxA")
        .with("timestamp", 150)
        .with("payload", json!({}))
        .create();
    assert!(!evaluator.evaluate_event(&wrong_evt));

    // Too-early timestamp -> fail
    let early = EventFactory::new()
        .with("event_type", "evt")
        .with("context_id", "ctxA")
        .with("timestamp", 99)
        .with("payload", json!({}))
        .create();
    assert!(!evaluator.evaluate_event(&early));
}

#[tokio::test]
async fn build_from_plan_skips_special_fields_for_aggregation() {
    use crate::command::types::{CompareOp, Expr};

    // Aggregation command: adds COUNT to ensure aggregate_plan is Some
    let command = CommandFactory::query()
        .with_event_type("evt")
        .with_context_id("ctxB")
        .with_since("1000")
        .with_where_clause(Expr::Compare {
            field: "amount".into(),
            op: CompareOp::Gte,
            value: json!(10),
        })
        .add_count()
        .create();

    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();
    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(registry)
        .create()
        .await;

    // In aggregation mode, special fields are skipped
    let evaluator = ConditionEvaluatorBuilder::build_from_plan(&plan);

    // Event violates special fields (event_type/context_id/timestamp), but satisfies where(amount>=10)
    let should_pass = EventFactory::new()
        .with("event_type", "other")
        .with("context_id", "other")
        .with("timestamp", 0)
        .with("payload", json!({"amount": 20}))
        .create();

    assert!(evaluator.evaluate_event(&should_pass));

    // Fails where clause -> should fail regardless of special fields
    let should_fail = EventFactory::new()
        .with("event_type", "other")
        .with("context_id", "other")
        .with("timestamp", 0)
        .with("payload", json!({"amount": 5}))
        .create();

    assert!(!evaluator.evaluate_event(&should_fail));
}

#[tokio::test]
async fn where_temporal_literal_eq_iso8601_matches_created_at_seconds() {
    let registry_factory = SchemaRegistryFactory::new();
    registry_factory
        .define_with_fields("evt_time2", &[("id", "int"), ("created_at", "datetime")])
        .await
        .unwrap();
    let registry = registry_factory.registry();

    // WHERE created_at = "2025-01-01T00:00:01Z"
    let expr = Expr::Compare {
        field: "created_at".into(),
        op: CompareOp::Eq,
        value: json!("2025-01-01T00:00:01Z"),
    };

    let command = CommandFactory::query()
        .with_event_type("evt_time2")
        .with_context_id("ctxA")
        .with_where_clause(expr)
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(registry.clone())
        .create()
        .await;

    let mut builder = ConditionEvaluatorBuilder::new();
    if let Some(expr) = plan.where_clause() {
        builder.add_where_clause(expr);
    }
    let evaluator = builder.into_evaluator();

    let ts_eq = chrono::DateTime::parse_from_rfc3339("2025-01-01T00:00:01Z")
        .unwrap()
        .with_timezone(&chrono::Utc)
        .timestamp();
    let ts_other = ts_eq - 1;

    let pass = EventFactory::new()
        .with("event_type", "evt_time2")
        .with("context_id", "ctxA")
        .with("timestamp", 0)
        .with("payload", json!({"id": 1, "created_at": ts_eq}))
        .create();
    let fail = EventFactory::new()
        .with("event_type", "evt_time2")
        .with("context_id", "ctxA")
        .with("timestamp", 0)
        .with("payload", json!({"id": 2, "created_at": ts_other}))
        .create();

    assert!(evaluator.evaluate_event(&pass));
    assert!(!evaluator.evaluate_event(&fail));
}

#[tokio::test]
async fn where_date_literal_eq_midnight_matches_date_field() {
    let registry_factory = SchemaRegistryFactory::new();
    registry_factory
        .define_with_fields("evt_date", &[("id", "int"), ("on", "date")])
        .await
        .unwrap();
    let registry = registry_factory.registry();

    // WHERE on = "2025-09-06"
    let expr = Expr::Compare {
        field: "on".into(),
        op: CompareOp::Eq,
        value: json!("2025-09-06"),
    };

    let command = CommandFactory::query()
        .with_event_type("evt_date")
        .with_context_id("ctxD")
        .with_where_clause(expr)
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(registry.clone())
        .create()
        .await;

    let mut builder = ConditionEvaluatorBuilder::new();
    if let Some(expr) = plan.where_clause() {
        builder.add_where_clause(expr);
    }
    let evaluator = builder.into_evaluator();

    let date_midnight = chrono::NaiveDate::parse_from_str("2025-09-06", "%Y-%m-%d")
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let ts_midnight = date_midnight.and_utc().timestamp();
    let ts_other = ts_midnight + 86_400;

    let pass = EventFactory::new()
        .with("event_type", "evt_date")
        .with("context_id", "ctxD")
        .with("timestamp", 0)
        .with("payload", json!({"id": 1, "on": ts_midnight}))
        .create();
    let fail = EventFactory::new()
        .with("event_type", "evt_date")
        .with("context_id", "ctxD")
        .with("timestamp", 0)
        .with("payload", json!({"id": 2, "on": ts_other}))
        .create();

    assert!(evaluator.evaluate_event(&pass));
    assert!(!evaluator.evaluate_event(&fail));
}

#[tokio::test]
async fn where_fractional_second_range_collapses_to_empty() {
    let registry_factory = SchemaRegistryFactory::new();
    registry_factory
        .define_with_fields(
            "evt_frac_eval",
            &[("id", "int"), ("created_at", "datetime")],
        )
        .await
        .unwrap();
    let registry = registry_factory.registry();

    // WHERE created_at >= 00:00:00.600Z AND created_at < 00:00:00.800Z
    let expr = Expr::And(
        Box::new(Expr::Compare {
            field: "created_at".into(),
            op: CompareOp::Gte,
            value: json!("2025-01-01T00:00:00.600Z"),
        }),
        Box::new(Expr::Compare {
            field: "created_at".into(),
            op: CompareOp::Lt,
            value: json!("2025-01-01T00:00:00.800Z"),
        }),
    );

    let command = CommandFactory::query()
        .with_event_type("evt_frac_eval")
        .with_context_id("ctxF")
        .with_where_clause(expr)
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(registry.clone())
        .create()
        .await;

    let mut builder = ConditionEvaluatorBuilder::new();
    if let Some(expr) = plan.where_clause() {
        builder.add_where_clause(expr);
    }
    let evaluator = builder.into_evaluator();

    let base_sec = chrono::DateTime::parse_from_rfc3339("2025-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc)
        .timestamp();

    let event = EventFactory::new()
        .with("event_type", "evt_frac_eval")
        .with("context_id", "ctxF")
        .with("timestamp", 0)
        .with("payload", json!({"id": 1, "created_at": base_sec}))
        .create();

    assert!(!evaluator.evaluate_event(&event));
}

#[tokio::test]
async fn since_microseconds_string_normalizes_to_seconds_inclusive() {
    let registry_factory = SchemaRegistryFactory::new();
    registry_factory
        .define_with_fields("evt_us_eval", &[("id", "int"), ("created_at", "datetime")])
        .await
        .unwrap();
    let registry = registry_factory.registry();

    // SINCE "1735689600000000" USING created_at
    let command = CommandFactory::query()
        .with_event_type("evt_us_eval")
        .with_context_id("ctxU")
        .with_since("1735689600000000")
        .with_time_field("created_at")
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(registry.clone())
        .create()
        .await;

    let evaluator = ConditionEvaluatorBuilder::build_from_plan(&plan);

    let since_sec = 1_735_689_600i64; // 2025-01-01T00:00:00Z

    let before = EventFactory::new()
        .with("event_type", "evt_us_eval")
        .with("context_id", "ctxU")
        .with("timestamp", 0)
        .with("payload", json!({"id": 1, "created_at": since_sec - 1}))
        .create();
    let at = EventFactory::new()
        .with("event_type", "evt_us_eval")
        .with("context_id", "ctxU")
        .with("timestamp", 0)
        .with("payload", json!({"id": 2, "created_at": since_sec}))
        .create();

    assert!(!evaluator.evaluate_event(&before));
    assert!(evaluator.evaluate_event(&at));
}

// =============================================================================
// IN OPERATOR TESTS
// =============================================================================

#[tokio::test]
async fn builds_in_numeric_condition_from_integer_values() {
    let expr = Expr::In {
        field: "id".into(),
        values: vec![json!(2), json!(4), json!(6), json!(8)],
    };

    let command = CommandFactory::query()
        .with_event_type("test_event")
        .with_context_id("ctx1")
        .with_where_clause(expr)
        .with_since("123000")
        .create();

    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();
    let plan = QueryPlanFactory::new()
        .with_command(command.clone())
        .with_registry(registry)
        .create()
        .await;

    let mut builder = ConditionEvaluatorBuilder::new();
    builder.add_special_fields(&plan);
    if let Some(expr) = plan.where_clause() {
        builder.add_where_clause(expr);
    }

    let evaluator = builder.into_evaluator();

    // Events with IDs in the IN list should pass
    let pass1 = EventFactory::new()
        .with("event_type", "test_event")
        .with("context_id", "ctx1")
        .with("timestamp", 123456)
        .with("payload", json!({ "id": 2 }))
        .create();
    let pass2 = EventFactory::new()
        .with("event_type", "test_event")
        .with("context_id", "ctx1")
        .with("timestamp", 123456)
        .with("payload", json!({ "id": 6 }))
        .create();

    // Events with IDs not in the IN list should fail
    let fail1 = EventFactory::new()
        .with("event_type", "test_event")
        .with("context_id", "ctx1")
        .with("timestamp", 123456)
        .with("payload", json!({ "id": 1 }))
        .create();
    let fail2 = EventFactory::new()
        .with("event_type", "test_event")
        .with("context_id", "ctx1")
        .with("timestamp", 123456)
        .with("payload", json!({ "id": 10 }))
        .create();

    assert!(evaluator.evaluate_event(&pass1));
    assert!(evaluator.evaluate_event(&pass2));
    assert!(!evaluator.evaluate_event(&fail1));
    assert!(!evaluator.evaluate_event(&fail2));
}

#[tokio::test]
async fn builds_in_string_condition_from_string_values() {
    let expr = Expr::In {
        field: "status".into(),
        values: vec![json!("active"), json!("completed"), json!("pending")],
    };

    let command = CommandFactory::query()
        .with_event_type("test_event")
        .with_context_id("ctx1")
        .with_where_clause(expr)
        .with_since("123000")
        .create();

    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();
    let plan = QueryPlanFactory::new()
        .with_command(command.clone())
        .with_registry(registry)
        .create()
        .await;

    let mut builder = ConditionEvaluatorBuilder::new();
    builder.add_special_fields(&plan);
    if let Some(expr) = plan.where_clause() {
        builder.add_where_clause(expr);
    }

    let evaluator = builder.into_evaluator();

    // Events with status in the IN list should pass
    let pass1 = EventFactory::new()
        .with("event_type", "test_event")
        .with("context_id", "ctx1")
        .with("timestamp", 123456)
        .with("payload", json!({ "status": "active" }))
        .create();
    let pass2 = EventFactory::new()
        .with("event_type", "test_event")
        .with("context_id", "ctx1")
        .with("timestamp", 123456)
        .with("payload", json!({ "status": "completed" }))
        .create();

    // Events with status not in the IN list should fail
    let fail = EventFactory::new()
        .with("event_type", "test_event")
        .with("context_id", "ctx1")
        .with("timestamp", 123456)
        .with("payload", json!({ "status": "cancelled" }))
        .create();

    assert!(evaluator.evaluate_event(&pass1));
    assert!(evaluator.evaluate_event(&pass2));
    assert!(!evaluator.evaluate_event(&fail));
}

#[tokio::test]
async fn builds_in_numeric_condition_from_temporal_values() {
    let registry_factory = SchemaRegistryFactory::new();
    registry_factory
        .define_with_fields("evt_temporal", &[("id", "int"), ("created_at", "datetime")])
        .await
        .unwrap();
    let registry = registry_factory.registry();

    // IN with ISO-8601 datetime strings should be parsed to epoch seconds
    let expr = Expr::In {
        field: "created_at".into(),
        values: vec![
            json!("2025-01-01T00:00:00Z"),
            json!("2025-01-02T00:00:00Z"),
            json!("2025-01-03T00:00:00Z"),
        ],
    };

    let command = CommandFactory::query()
        .with_event_type("evt_temporal")
        .with_context_id("ctx1")
        .with_where_clause(expr)
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(registry.clone())
        .create()
        .await;

    let mut builder = ConditionEvaluatorBuilder::new();
    if let Some(expr) = plan.where_clause() {
        builder.add_where_clause(expr);
    }
    let evaluator = builder.into_evaluator();

    let ts1 = chrono::DateTime::parse_from_rfc3339("2025-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc)
        .timestamp();
    let ts2 = chrono::DateTime::parse_from_rfc3339("2025-01-02T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc)
        .timestamp();
    let ts_other = chrono::DateTime::parse_from_rfc3339("2025-01-05T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc)
        .timestamp();

    // Events with created_at in the IN list should pass
    let pass1 = EventFactory::new()
        .with("event_type", "evt_temporal")
        .with("context_id", "ctx1")
        .with("timestamp", 0)
        .with("payload", json!({ "id": 1, "created_at": ts1 }))
        .create();
    let pass2 = EventFactory::new()
        .with("event_type", "evt_temporal")
        .with("context_id", "ctx1")
        .with("timestamp", 0)
        .with("payload", json!({ "id": 2, "created_at": ts2 }))
        .create();

    // Event with created_at not in the IN list should fail
    let fail = EventFactory::new()
        .with("event_type", "evt_temporal")
        .with("context_id", "ctx1")
        .with("timestamp", 0)
        .with("payload", json!({ "id": 3, "created_at": ts_other }))
        .create();

    assert!(evaluator.evaluate_event(&pass1));
    assert!(evaluator.evaluate_event(&pass2));
    assert!(!evaluator.evaluate_event(&fail));
}

#[tokio::test]
async fn builds_in_string_condition_when_mixed_types_present() {
    // When IN list contains both numeric and non-numeric values, it should fall back to string
    let expr = Expr::In {
        field: "value".into(),
        values: vec![json!(123), json!("abc"), json!(456)], // Mixed: number, string, number
    };

    let command = CommandFactory::query()
        .with_event_type("test_event")
        .with_context_id("ctx1")
        .with_where_clause(expr)
        .with_since("123000")
        .create();

    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();
    let plan = QueryPlanFactory::new()
        .with_command(command.clone())
        .with_registry(registry)
        .create()
        .await;

    let mut builder = ConditionEvaluatorBuilder::new();
    builder.add_special_fields(&plan);
    if let Some(expr) = plan.where_clause() {
        builder.add_where_clause(expr);
    }

    let evaluator = builder.into_evaluator();

    // Events matching string values should pass
    let pass1 = EventFactory::new()
        .with("event_type", "test_event")
        .with("context_id", "ctx1")
        .with("timestamp", 123456)
        .with("payload", json!({ "value": "123" })) // String "123" matches
        .create();
    let pass2 = EventFactory::new()
        .with("event_type", "test_event")
        .with("context_id", "ctx1")
        .with("timestamp", 123456)
        .with("payload", json!({ "value": "abc" }))
        .create();

    // Event not matching any string value should fail
    let fail = EventFactory::new()
        .with("event_type", "test_event")
        .with("context_id", "ctx1")
        .with("timestamp", 123456)
        .with("payload", json!({ "value": "xyz" }))
        .create();

    assert!(evaluator.evaluate_event(&pass1));
    assert!(evaluator.evaluate_event(&pass2));
    assert!(!evaluator.evaluate_event(&fail));
}

#[tokio::test]
async fn builds_in_condition_with_and_operator() {
    // id IN (2, 4, 6) AND status = "active"
    let expr = Expr::And(
        Box::new(Expr::In {
            field: "id".into(),
            values: vec![json!(2), json!(4), json!(6)],
        }),
        Box::new(Expr::Compare {
            field: "status".into(),
            op: CompareOp::Eq,
            value: json!("active"),
        }),
    );

    let command = CommandFactory::query()
        .with_event_type("test_event")
        .with_context_id("ctx1")
        .with_where_clause(expr)
        .with_since("123000")
        .create();

    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();
    let plan = QueryPlanFactory::new()
        .with_command(command.clone())
        .with_registry(registry)
        .create()
        .await;

    let mut builder = ConditionEvaluatorBuilder::new();
    builder.add_special_fields(&plan);
    if let Some(expr) = plan.where_clause() {
        builder.add_where_clause(expr);
    }

    let evaluator = builder.into_evaluator();

    // Both conditions satisfied -> pass
    let pass = EventFactory::new()
        .with("event_type", "test_event")
        .with("context_id", "ctx1")
        .with("timestamp", 123456)
        .with("payload", json!({ "id": 4, "status": "active" }))
        .create();

    // Only IN condition satisfied -> fail
    let fail1 = EventFactory::new()
        .with("event_type", "test_event")
        .with("context_id", "ctx1")
        .with("timestamp", 123456)
        .with("payload", json!({ "id": 4, "status": "inactive" }))
        .create();

    // Only status condition satisfied -> fail
    let fail2 = EventFactory::new()
        .with("event_type", "test_event")
        .with("context_id", "ctx1")
        .with("timestamp", 123456)
        .with("payload", json!({ "id": 1, "status": "active" }))
        .create();

    assert!(evaluator.evaluate_event(&pass));
    assert!(!evaluator.evaluate_event(&fail1));
    assert!(!evaluator.evaluate_event(&fail2));
}

#[tokio::test]
async fn builds_in_condition_with_or_operator() {
    // id IN (1, 2, 3) OR status = "completed"
    let expr = Expr::Or(
        Box::new(Expr::In {
            field: "id".into(),
            values: vec![json!(1), json!(2), json!(3)],
        }),
        Box::new(Expr::Compare {
            field: "status".into(),
            op: CompareOp::Eq,
            value: json!("completed"),
        }),
    );

    let command = CommandFactory::query()
        .with_event_type("test_event")
        .with_context_id("ctx1")
        .with_where_clause(expr)
        .with_since("123000")
        .create();

    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();
    let plan = QueryPlanFactory::new()
        .with_command(command.clone())
        .with_registry(registry)
        .create()
        .await;

    let mut builder = ConditionEvaluatorBuilder::new();
    builder.add_special_fields(&plan);
    if let Some(expr) = plan.where_clause() {
        builder.add_where_clause(expr);
    }

    let evaluator = builder.into_evaluator();

    // IN condition satisfied -> pass
    let pass1 = EventFactory::new()
        .with("event_type", "test_event")
        .with("context_id", "ctx1")
        .with("timestamp", 123456)
        .with("payload", json!({ "id": 2, "status": "pending" }))
        .create();

    // Status condition satisfied -> pass
    let pass2 = EventFactory::new()
        .with("event_type", "test_event")
        .with("context_id", "ctx1")
        .with("timestamp", 123456)
        .with("payload", json!({ "id": 10, "status": "completed" }))
        .create();

    // Neither condition satisfied -> fail
    let fail = EventFactory::new()
        .with("event_type", "test_event")
        .with("context_id", "ctx1")
        .with("timestamp", 123456)
        .with("payload", json!({ "id": 10, "status": "pending" }))
        .create();

    assert!(evaluator.evaluate_event(&pass1));
    assert!(evaluator.evaluate_event(&pass2));
    assert!(!evaluator.evaluate_event(&fail));
}

#[tokio::test]
async fn builds_in_condition_with_not_operator() {
    // NOT id IN (2, 4, 6, 8)
    let expr = Expr::Not(Box::new(Expr::In {
        field: "id".into(),
        values: vec![json!(2), json!(4), json!(6), json!(8)],
    }));

    let command = CommandFactory::query()
        .with_event_type("test_event")
        .with_context_id("ctx1")
        .with_where_clause(expr)
        .with_since("123000")
        .create();

    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();
    let plan = QueryPlanFactory::new()
        .with_command(command.clone())
        .with_registry(registry)
        .create()
        .await;

    let mut builder = ConditionEvaluatorBuilder::new();
    builder.add_special_fields(&plan);
    if let Some(expr) = plan.where_clause() {
        builder.add_where_clause(expr);
    }

    let evaluator = builder.into_evaluator();

    // IDs NOT in the list -> pass
    let pass1 = EventFactory::new()
        .with("event_type", "test_event")
        .with("context_id", "ctx1")
        .with("timestamp", 123456)
        .with("payload", json!({ "id": 1 }))
        .create();
    let pass2 = EventFactory::new()
        .with("event_type", "test_event")
        .with("context_id", "ctx1")
        .with("timestamp", 123456)
        .with("payload", json!({ "id": 5 }))
        .create();

    // IDs in the list -> fail
    let fail1 = EventFactory::new()
        .with("event_type", "test_event")
        .with("context_id", "ctx1")
        .with("timestamp", 123456)
        .with("payload", json!({ "id": 4 }))
        .create();
    let fail2 = EventFactory::new()
        .with("event_type", "test_event")
        .with("context_id", "ctx1")
        .with("timestamp", 123456)
        .with("payload", json!({ "id": 8 }))
        .create();

    assert!(evaluator.evaluate_event(&pass1));
    assert!(evaluator.evaluate_event(&pass2));
    assert!(!evaluator.evaluate_event(&fail1));
    assert!(!evaluator.evaluate_event(&fail2));
}

#[tokio::test]
async fn builds_in_condition_with_single_value() {
    // Single value IN list should work
    let expr = Expr::In {
        field: "id".into(),
        values: vec![json!(42)],
    };

    let command = CommandFactory::query()
        .with_event_type("test_event")
        .with_context_id("ctx1")
        .with_where_clause(expr)
        .with_since("123000")
        .create();

    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();
    let plan = QueryPlanFactory::new()
        .with_command(command.clone())
        .with_registry(registry)
        .create()
        .await;

    let mut builder = ConditionEvaluatorBuilder::new();
    builder.add_special_fields(&plan);
    if let Some(expr) = plan.where_clause() {
        builder.add_where_clause(expr);
    }

    let evaluator = builder.into_evaluator();

    let pass = EventFactory::new()
        .with("event_type", "test_event")
        .with("context_id", "ctx1")
        .with("timestamp", 123456)
        .with("payload", json!({ "id": 42 }))
        .create();

    let fail = EventFactory::new()
        .with("event_type", "test_event")
        .with("context_id", "ctx1")
        .with("timestamp", 123456)
        .with("payload", json!({ "id": 43 }))
        .create();

    assert!(evaluator.evaluate_event(&pass));
    assert!(!evaluator.evaluate_event(&fail));
}
