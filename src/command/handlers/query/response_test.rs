
use tokio::io::{AsyncReadExt, duplex};

use crate::command::handlers::query::response::QueryResponseFormatter;
use crate::engine::core::read::result::{ColumnSpec, QueryResult, SelectionResult};
use crate::engine::types::ScalarValue;
use crate::shared::response::JsonRenderer;

#[tokio::test]
async fn selection_without_rows_emits_no_results_message() {
    let selection = SelectionResult {
        columns: vec![ColumnSpec {
            name: "id".to_string(),
            logical_type: "Integer".to_string(),
        }],
        rows: Vec::new(),
    };

    let (mut writer, mut reader) = duplex(1024);
    let mut formatter = QueryResponseFormatter::new(&JsonRenderer);

    formatter
        .write(&mut writer, QueryResult::Selection(selection))
        .await
        .expect("formatter should succeed");
    drop(writer);

    let mut buf = Vec::new();
    reader
        .read_to_end(&mut buf)
        .await
        .expect("read formatter output");
    let body = String::from_utf8(buf).expect("utf8 output");

    assert!(
        body.contains("No matching events found"),
        "expected empty selection message, got: {body}"
    );
}

#[tokio::test]
async fn selection_with_rows_renders_table() {
    let selection = SelectionResult {
        columns: vec![
            ColumnSpec {
                name: "id".to_string(),
                logical_type: "Integer".to_string(),
            },
            ColumnSpec {
                name: "value".to_string(),
                logical_type: "String".to_string(),
            },
        ],
        rows: vec![vec![
            ScalarValue::Int64(42),
            ScalarValue::Utf8("hello".to_string()),
        ]],
    };

    let (mut writer, mut reader) = duplex(1024);
    let mut formatter = QueryResponseFormatter::new(&JsonRenderer);

    formatter
        .write(&mut writer, QueryResult::Selection(selection))
        .await
        .expect("formatter should succeed");
    drop(writer);

    let mut buf = Vec::new();
    reader
        .read_to_end(&mut buf)
        .await
        .expect("read formatter output");
    let body = String::from_utf8(buf).expect("utf8 output");

    assert!(
        body.contains("\"columns\":["),
        "expected columns array, got: {body}"
    );
    assert!(
        body.contains("\"rows\":[[42"),
        "expected row data, got: {body}"
    );
}
