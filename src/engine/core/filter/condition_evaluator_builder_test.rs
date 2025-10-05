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
