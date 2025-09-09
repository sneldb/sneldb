use crate::test_helpers::factories::ResolverFactory;

#[test]
fn test_resolver_factory_creates_expected_mapping() {
    let resolver = ResolverFactory::new()
        .with("login", "uid-login")
        .with("purchase", "uid-purchase")
        .create();

    assert_eq!(resolver.get("login"), Some(&"uid-login".to_string()));
    assert_eq!(resolver.get("purchase"), Some(&"uid-purchase".to_string()));
    assert_eq!(resolver.get("missing"), None);
}
