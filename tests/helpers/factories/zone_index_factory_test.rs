use crate::test_helpers::factory::Factory;

#[cfg(test)]
#[test]
fn test_zone_index_factory() {
    let index = Factory::zone_index()
        .with_entry("login", "ctx1", 0)
        .with_entry("login", "ctx2", 1)
        .with_entry("signup", "ctx3", 2)
        .create();

    assert!(index.index.contains_key("login"));
    assert_eq!(index.index["login"]["ctx1"], vec![0]);
    assert_eq!(index.index["signup"]["ctx3"], vec![2]);
}
