use crate::engine::core::UidResolver;
use crate::test_helpers::factories::{EventFactory, SchemaRegistryFactory, ZonePlanFactory};

#[tokio::test]
async fn test_uid_resolver_with_factories_success() {
    // Prepare schema registry
    let registry_factory = SchemaRegistryFactory::new();
    registry_factory
        .define_with_fields("signup", &[("plan", "string")])
        .await
        .unwrap();
    registry_factory
        .define_with_fields("payment", &[("amount", "integer")])
        .await
        .unwrap();

    let registry = registry_factory.registry();

    // Generate zone plans with known event types
    let zone1 = ZonePlanFactory::new()
        .with(
            "events",
            serde_json::to_value(
                EventFactory::new()
                    .with("event_type", "signup")
                    .create_list(2),
            )
            .unwrap(),
        )
        .create();

    let zone2 = ZonePlanFactory::new()
        .with(
            "events",
            serde_json::to_value(
                EventFactory::new()
                    .with("event_type", "payment")
                    .create_list(1),
            )
            .unwrap(),
        )
        .create();

    let zone_plans = vec![zone1, zone2];

    // Run resolver
    let resolver = UidResolver::from_events(&zone_plans, &registry)
        .await
        .expect("UID resolution should succeed");

    // Assertions
    assert!(resolver.get("signup").is_some());
    assert!(resolver.get("payment").is_some());
    assert_eq!(resolver.get("ghost"), None);
}

#[tokio::test]
async fn test_uid_resolver_with_unknown_event_type() {
    // Prepare schema registry with only "signup"
    let registry_factory = SchemaRegistryFactory::new();
    registry_factory
        .define_with_fields("signup", &[("plan", "string")])
        .await
        .unwrap();

    let registry = registry_factory.registry();

    // One known, one unknown event type
    let zone = ZonePlanFactory::new()
        .with(
            "events",
            serde_json::to_value(vec![
                EventFactory::new().with("event_type", "signup").create(),
                EventFactory::new().with("event_type", "ghost").create(),
            ])
            .unwrap(),
        )
        .create();

    let result = UidResolver::from_events(&[zone], &registry).await;

    assert!(result.is_err());
    let err = result.err().unwrap().to_string();
    assert!(
        err.contains("UID not found for event_type: ghost"),
        "Unexpected error: {}",
        err
    );
}
