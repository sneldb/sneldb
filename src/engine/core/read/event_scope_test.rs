use super::event_scope::EventScope;
use crate::test_helpers::factories::SchemaRegistryFactory;

#[tokio::test]
async fn specific_scope_captures_uid() {
    let factory = SchemaRegistryFactory::new();
    let registry = factory.registry();
    factory
        .define_with_fields("login", &[("device", "string")])
        .await
        .unwrap();

    let guard = registry.read().await;
    let scope = EventScope::from_command("login", &guard);

    match scope {
        EventScope::Specific {
            event_type,
            uid: Some(uid),
        } => {
            assert_eq!(event_type, "login");
            assert!(!uid.is_empty());
        }
        _ => panic!("expected specific scope with uid"),
    }
}

#[tokio::test]
async fn wildcard_scope_collects_all_known_uids() {
    let factory = SchemaRegistryFactory::new();
    let registry = factory.registry();
    factory
        .define_with_fields("login", &[("device", "string")])
        .await
        .unwrap();
    factory
        .define_with_fields("signup", &[("plan", "string")])
        .await
        .unwrap();

    let guard = registry.read().await;
    let scope = EventScope::from_command("*", &guard);

    match scope {
        EventScope::Wildcard { pairs } => {
            assert_eq!(pairs.len(), 2);
            let uids: Vec<_> = pairs.iter().map(|(_, uid)| uid).collect();
            assert!(uids.iter().all(|uid| !uid.is_empty()));
        }
        _ => panic!("expected wildcard scope"),
    }
}
