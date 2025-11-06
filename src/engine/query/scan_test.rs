use crate::command::types::{CompareOp, Expr};
use crate::engine::core::Flusher;
use crate::engine::core::memory::passive_buffer_set::PassiveBufferSet;
use crate::engine::core::read::result::QueryResult;
use crate::engine::query::scan::scan;
use crate::engine::types::ScalarValue;
use crate::test_helpers::factories::{
    CommandFactory, EventFactory, MemTableFactory, SchemaRegistryFactory,
};

use serde_json::json;
use std::sync::Arc;
use std::sync::RwLock as StdRwLock;
use tempfile::tempdir;

#[tokio::test]
async fn scan_query_returns_expected_events() {
    let tmp_dir = tempdir().unwrap();
    let segment_base_dir = tmp_dir.path().join("shard-0");
    std::fs::create_dir_all(&segment_base_dir).unwrap();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();

    schema_factory
        .define_with_fields("test_event", &[("context_id", "string"), ("key", "string")])
        .await
        .unwrap();

    let event1 = EventFactory::new()
        .with("context_id", "ctx1")
        .with("payload", json!({ "key": "value1" }))
        .create();

    let event2 = EventFactory::new()
        .with("context_id", "ctx2")
        .with("payload", json!({ "key": "value2" }))
        .create();

    let memtable = MemTableFactory::new()
        .with_capacity(2)
        .with_events(vec![event1.clone(), event2.clone()])
        .create()
        .unwrap();

    let passive_buffers = Arc::new(PassiveBufferSet::new(8));

    let flusher = Flusher::new(
        memtable.clone(),
        1,
        &segment_base_dir,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    );
    flusher.flush().await.expect("Flush failed");

    let command = CommandFactory::query()
        .with_context_id("ctx1")
        .with_event_type("test_event")
        .create();

    let segment_ids = Arc::new(StdRwLock::new(vec!["001".into()]));
    let result = scan(
        &command,
        None,
        &registry,
        &segment_base_dir,
        &segment_ids,
        &memtable,
        &passive_buffers,
    )
    .await
    .expect("scan failed");
    let table = match result {
        QueryResult::Selection(_) | QueryResult::Aggregation(_) => result.finalize_table(),
    };
    // columns: context_id, event_type, timestamp, payload
    assert_eq!(table.rows.len(), 1);
    let row = &table.rows[0];
    assert_eq!(row[0], ScalarValue::from(json!("ctx1")));
    assert_eq!(row[3].to_json()["key"], json!("value1"));
}

#[tokio::test]
async fn scan_where_expr_logic() {
    let tmp_dir = tempdir().unwrap();
    let segment_base_dir = tmp_dir.path().join("shard-0");
    std::fs::create_dir_all(&segment_base_dir).unwrap();

    let segment_ids = Arc::new(std::sync::RwLock::new(vec![]));

    let registry_factory = SchemaRegistryFactory::new();
    registry_factory
        .define_with_fields("test_event", &[("key", "string"), ("value", "int")])
        .await
        .unwrap();
    let registry = registry_factory.registry();

    let event1 = EventFactory::new()
        .with("context_id", "ctx1")
        .with("payload", json!({"key": "a", "value": 1}))
        .create();
    let event2 = EventFactory::new()
        .with("context_id", "ctx2")
        .with("payload", json!({"key": "b", "value": 5}))
        .create();
    let event3 = EventFactory::new()
        .with("context_id", "ctx3")
        .with("payload", json!({"key": "c", "value": 9}))
        .create();

    let memtable = MemTableFactory::new()
        .with_events(vec![event1.clone(), event2.clone(), event3.clone()])
        .create()
        .unwrap();

    let passive_buffers = Arc::new(PassiveBufferSet::new(8));

    let cases: Vec<(Expr, Vec<&str>)> = vec![
        (
            Expr::Compare {
                field: "key".into(),
                op: CompareOp::Eq,
                value: json!("b"),
            },
            vec!["ctx2"],
        ),
        (
            Expr::Compare {
                field: "key".into(),
                op: CompareOp::Neq,
                value: json!("a"),
            },
            vec!["ctx2", "ctx3"],
        ),
        (
            Expr::Compare {
                field: "value".into(),
                op: CompareOp::Gt,
                value: json!(5),
            },
            vec!["ctx3"],
        ),
        (
            Expr::Compare {
                field: "value".into(),
                op: CompareOp::Gte,
                value: json!(5),
            },
            vec!["ctx2", "ctx3"],
        ),
        (
            Expr::Compare {
                field: "value".into(),
                op: CompareOp::Lt,
                value: json!(5),
            },
            vec!["ctx1"],
        ),
        (
            Expr::Compare {
                field: "value".into(),
                op: CompareOp::Lte,
                value: json!(5),
            },
            vec!["ctx1", "ctx2"],
        ),
        (
            Expr::Or(
                Box::new(Expr::Compare {
                    field: "key".into(),
                    op: CompareOp::Eq,
                    value: json!("a"),
                }),
                Box::new(Expr::Compare {
                    field: "key".into(),
                    op: CompareOp::Eq,
                    value: json!("c"),
                }),
            ),
            vec!["ctx1", "ctx3"],
        ),
        (
            Expr::Not(Box::new(Expr::Compare {
                field: "key".into(),
                op: CompareOp::Eq,
                value: json!("c"),
            })),
            vec!["ctx1", "ctx2"],
        ),
        (
            Expr::And(
                Box::new(Expr::Compare {
                    field: "value".into(),
                    op: CompareOp::Gt,
                    value: json!(1),
                }),
                Box::new(Expr::Compare {
                    field: "value".into(),
                    op: CompareOp::Lt,
                    value: json!(10),
                }),
            ),
            vec!["ctx2", "ctx3"],
        ),
    ];

    for (expr, expected_ids) in cases {
        let command = CommandFactory::query().with_where_clause(expr).create();

        let result = scan(
            &command,
            None,
            &registry,
            tmp_dir.path(),
            &segment_ids,
            &memtable,
            &passive_buffers,
        )
        .await
        .unwrap();
        let table = match result {
            QueryResult::Selection(_) | QueryResult::Aggregation(_) => result.finalize_table(),
        };
        let found_ids: Vec<String> = table
            .rows
            .iter()
            .map(|r| r[0].as_str().unwrap().to_string())
            .collect();
        assert_eq!(
            found_ids, expected_ids,
            "failed for expression: {:?}",
            command
        );
    }
}

#[tokio::test]
async fn scan_query_with_context_id_and_expr_logic() {
    let dir = tempdir().unwrap();
    let segment_ids = Arc::new(std::sync::RwLock::new(vec![]));

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    schema_factory
        .define_with_fields("test_event", &[("key", "string"), ("value", "int")])
        .await
        .unwrap();

    let events = vec![
        EventFactory::new()
            .with("context_id", "ctx1")
            .with("payload", json!({"key": "a", "value": 1}))
            .create(),
        EventFactory::new()
            .with("context_id", "ctx2")
            .with("payload", json!({"key": "b", "value": 5}))
            .create(),
        EventFactory::new()
            .with("context_id", "ctx3")
            .with("payload", json!({"key": "c", "value": 9}))
            .create(),
    ];
    let memtable = MemTableFactory::new().with_events(events).create().unwrap();
    let passive_buffers = Arc::new(PassiveBufferSet::new(8));

    // 1. Filter by context_id only
    let cmd = CommandFactory::query().with_context_id("ctx1").create();
    let result = scan(
        &cmd,
        None,
        &registry,
        dir.path(),
        &segment_ids,
        &memtable,
        &passive_buffers,
    )
    .await
    .unwrap();
    let table = match result {
        QueryResult::Selection(_) | QueryResult::Aggregation(_) => result.finalize_table(),
    };
    assert_eq!(table.rows.len(), 1);
    assert_eq!(table.rows[0][0], ScalarValue::from(json!("ctx1")));

    // 2. Filter by context_id + key = "b"
    let expr = Expr::Compare {
        field: "key".into(),
        op: CompareOp::Eq,
        value: json!("b"),
    };
    let cmd = CommandFactory::query()
        .with_context_id("ctx2")
        .with_where_clause(expr)
        .create();
    let result = scan(
        &cmd,
        None,
        &registry,
        dir.path(),
        &segment_ids,
        &memtable,
        &passive_buffers,
    )
    .await
    .unwrap();
    let table = match result {
        QueryResult::Selection(_) | QueryResult::Aggregation(_) => result.finalize_table(),
    };
    assert_eq!(table.rows.len(), 1);
    assert_eq!(table.rows[0][0], ScalarValue::from(json!("ctx2")));

    // 3. context_id = ctx3 and value > 5
    let expr = Expr::Compare {
        field: "value".into(),
        op: CompareOp::Gt,
        value: json!(5),
    };
    let cmd = CommandFactory::query()
        .with_context_id("ctx3")
        .with_where_clause(expr)
        .create();
    let result = scan(
        &cmd,
        None,
        &registry,
        dir.path(),
        &segment_ids,
        &memtable,
        &passive_buffers,
    )
    .await
    .unwrap();
    let table = match result {
        QueryResult::Selection(_) | QueryResult::Aggregation(_) => result.finalize_table(),
    };
    assert_eq!(table.rows.len(), 1);
    assert_eq!(table.rows[0][0], ScalarValue::from(json!("ctx3")));

    // 4. context_id = ctx1 but value > 5 (should fail)
    let expr = Expr::Compare {
        field: "value".into(),
        op: CompareOp::Gt,
        value: json!(5),
    };
    let cmd = CommandFactory::query()
        .with_context_id("ctx1")
        .with_where_clause(expr)
        .create();
    let result = scan(
        &cmd,
        None,
        &registry,
        dir.path(),
        &segment_ids,
        &memtable,
        &passive_buffers,
    )
    .await
    .unwrap();
    let table = match result {
        QueryResult::Selection(_) | QueryResult::Aggregation(_) => result.finalize_table(),
    };
    assert_eq!(table.rows.len(), 0);

    // 5. context_id = ctx2 and (value == 5 OR key == "a")
    let expr = Expr::Or(
        Box::new(Expr::Compare {
            field: "value".into(),
            op: CompareOp::Eq,
            value: json!(5),
        }),
        Box::new(Expr::Compare {
            field: "key".into(),
            op: CompareOp::Eq,
            value: json!("a"),
        }),
    );
    let cmd = CommandFactory::query()
        .with_context_id("ctx2")
        .with_where_clause(expr)
        .create();
    let result = scan(
        &cmd,
        None,
        &registry,
        dir.path(),
        &segment_ids,
        &memtable,
        &passive_buffers,
    )
    .await
    .unwrap();
    let table = match result {
        QueryResult::Selection(_) | QueryResult::Aggregation(_) => result.finalize_table(),
    };
    assert_eq!(table.rows.len(), 1);
    assert_eq!(table.rows[0][0], ScalarValue::from(json!("ctx2")));
}

#[tokio::test]
async fn scan_aggregate_count_by_country() {
    let tmp_dir = tempdir().unwrap();
    let dir = tmp_dir.path();
    std::fs::create_dir_all(&dir).unwrap();

    let segment_ids = Arc::new(std::sync::RwLock::new(vec![]));
    let registry_factory = SchemaRegistryFactory::new();
    registry_factory
        .define_with_fields("orders", &[("country", "string"), ("amount", "int")])
        .await
        .unwrap();
    let registry = registry_factory.registry();

    let events = vec![
        EventFactory::new()
            .with("context_id", "u1")
            .with("event_type", "orders")
            .with("payload", json!({"country":"US","amount": 10}))
            .create(),
        EventFactory::new()
            .with("context_id", "u2")
            .with("event_type", "orders")
            .with("payload", json!({"country":"US","amount": 20}))
            .create(),
        EventFactory::new()
            .with("context_id", "u3")
            .with("event_type", "orders")
            .with("payload", json!({"country":"DE","amount": 5}))
            .create(),
    ];
    let memtable = MemTableFactory::new().with_events(events).create().unwrap();
    let passive_buffers = Arc::new(PassiveBufferSet::new(8));

    let cmd = CommandFactory::query()
        .with_event_type("orders")
        .add_count()
        .with_group_by(vec!["country"])
        .create();

    let result = scan(
        &cmd,
        None,
        &registry,
        dir,
        &segment_ids,
        &memtable,
        &passive_buffers,
    )
    .await
    .unwrap();
    let table = result.finalize_table();
    // Expect 2 rows (US, DE) and columns include country, count
    assert!(table.columns.iter().any(|c| c.name == "country"));
    assert!(table.columns.iter().any(|c| c.name == "count"));
    assert_eq!(table.rows.len(), 2);
}

#[tokio::test]
async fn scan_aggregate_count_unique_and_avg_by_country() {
    let tmp_dir = tempdir().unwrap();
    let dir = tmp_dir.path();
    std::fs::create_dir_all(&dir).unwrap();

    let segment_ids = Arc::new(std::sync::RwLock::new(vec![]));
    let registry_factory = SchemaRegistryFactory::new();
    registry_factory
        .define_with_fields("orders", &[("country", "string"), ("amount", "int")])
        .await
        .unwrap();
    let registry = registry_factory.registry();

    let events = vec![
        EventFactory::new()
            .with("context_id", "u1")
            .with("event_type", "orders")
            .with("payload", json!({"country":"US","amount": 10}))
            .create(),
        EventFactory::new()
            .with("context_id", "u2")
            .with("event_type", "orders")
            .with("payload", json!({"country":"US","amount": 30}))
            .create(),
        EventFactory::new()
            .with("context_id", "u1")
            .with("event_type", "orders")
            .with("payload", json!({"country":"DE","amount": 5}))
            .create(),
    ];
    let memtable = MemTableFactory::new().with_events(events).create().unwrap();
    let passive_buffers = Arc::new(PassiveBufferSet::new(8));

    let cmd = CommandFactory::query()
        .with_event_type("orders")
        .add_count_unique("context_id")
        .add_avg("amount")
        .with_group_by(vec!["country"])
        .create();

    let result = scan(
        &cmd,
        None,
        &registry,
        dir,
        &segment_ids,
        &memtable,
        &passive_buffers,
    )
    .await
    .unwrap();
    let table = result.finalize_table();
    // Expect two rows; verify columns exist
    assert!(
        table
            .columns
            .iter()
            .any(|c| c.name.starts_with("count_unique_"))
    );
    assert!(table.columns.iter().any(|c| c.name.starts_with("avg_")));
    assert_eq!(table.rows.len(), 2);
}
