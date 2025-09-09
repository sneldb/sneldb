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

    let ip_plan = plans.iter().find(|f| f.column == "ip").unwrap();
    assert_eq!(ip_plan.operation, Some(CompareOp::Eq));
    assert_eq!(ip_plan.value, Some(json!("127.0.0.1")));
    assert_eq!(ip_plan.priority, 2);
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
