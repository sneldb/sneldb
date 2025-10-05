use crate::test_helpers::factories::ColumnSpecFactory;

#[test]
fn builds_column_spec_with_defaults_and_helpers() {
    let c1 = ColumnSpecFactory::new().create();
    assert_eq!(c1.name, "col");
    assert_eq!(c1.logical_type, "String");

    let c2 = ColumnSpecFactory::new()
        .with_name("amount")
        .with_logical_type("Integer")
        .create();
    assert_eq!(c2.name, "amount");
    assert_eq!(c2.logical_type, "Integer");

    let c3 = ColumnSpecFactory::float("score");
    assert_eq!(c3.name, "score");
    assert_eq!(c3.logical_type, "Float");

    let c4 = ColumnSpecFactory::timestamp("bucket");
    assert_eq!(c4.name, "bucket");
    assert_eq!(c4.logical_type, "Timestamp");
}
