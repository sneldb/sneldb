use crate::engine::core::column::format::PhysicalType;
use crate::engine::core::column::type_catalog::ColumnTypeCatalog;

fn key(event: &str, field: &str) -> (String, String) {
    (event.to_string(), field.to_string())
}

#[test]
fn record_and_lookup() {
    let mut catalog = ColumnTypeCatalog::new();
    let k = key("login", "timestamp");
    catalog.record(k.clone(), PhysicalType::I64);

    assert_eq!(catalog.get(&k), Some(PhysicalType::I64));
    assert_eq!(catalog.len(), 1);
    assert!(!catalog.is_empty());
}

#[test]
fn record_ref_inserts_clone() {
    let mut catalog = ColumnTypeCatalog::new();
    let k = key("login", "metric");
    catalog.record_ref(&k, PhysicalType::F64);

    assert_eq!(catalog.get(&k), Some(PhysicalType::F64));
    assert_eq!(catalog.len(), 1);
}

#[test]
fn record_if_absent_preserves_existing_value() {
    let mut catalog = ColumnTypeCatalog::new();
    let k = key("login", "device");
    catalog.record(k.clone(), PhysicalType::VarBytes);
    catalog.record_if_absent(&k, PhysicalType::Bool);

    // The original VarBytes entry should remain
    assert_eq!(catalog.get(&k), Some(PhysicalType::VarBytes));
}

#[test]
fn record_if_absent_inserts_when_missing() {
    let mut catalog = ColumnTypeCatalog::new();
    let k = key("login", "success");
    catalog.record_if_absent(&k, PhysicalType::Bool);

    assert_eq!(catalog.get(&k), Some(PhysicalType::Bool));
}

#[test]
fn iter_exposes_all_entries() {
    let mut catalog = ColumnTypeCatalog::with_capacity(2);
    catalog.record(key("login", "timestamp"), PhysicalType::I64);
    catalog.record(key("login", "amount"), PhysicalType::F64);

    let mut types = catalog
        .iter()
        .map(|(k, v)| (k.clone(), *v))
        .collect::<Vec<_>>();
    types.sort_by(|a, b| a.0.cmp(&b.0));

    assert_eq!(
        types,
        vec![
            (key("login", "amount"), PhysicalType::F64),
            (key("login", "timestamp"), PhysicalType::I64),
        ]
    );
}
