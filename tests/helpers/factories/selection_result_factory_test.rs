use serde_json::json;

use crate::test_helpers::factories::{ColumnSpecFactory, SelectionResultFactory};

#[test]
fn builds_selection_result_with_builder_and_simple_helper() {
    // Builder path
    let cols = vec![
        ColumnSpecFactory::string("a"),
        ColumnSpecFactory::integer("b"),
    ];
    let sel = SelectionResultFactory::new()
        .with_columns(cols)
        .with_row(vec![json!("x"), json!(1)])
        .with_row(vec![json!("y"), json!(2)])
        .create();
    assert_eq!(sel.columns.len(), 2);
    assert_eq!(sel.rows.len(), 2);

    // Simple helper path
    let sel2 = SelectionResultFactory::simple(
        &[("country", "String"), ("amount", "Integer")],
        &[&[json!("US"), json!(10)], &[json!("DE"), json!(5)]],
    );
    use crate::engine::types::ScalarValue;
    assert_eq!(sel2.columns[0].name, "country");
    assert_eq!(sel2.rows[1][0], ScalarValue::from(json!("DE")));
}
