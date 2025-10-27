use crate::engine::core::column::column_values::ColumnValues;
use crate::engine::core::read::cache::DecompressedBlock;
use crate::engine::core::read::sink::ResultSink;
use crate::engine::core::read::sink::event_sink::EventSink;
use crate::test_helpers::factories::{DecompressedBlockFactory, EventFactory};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;

fn make_block(bytes: Vec<u8>) -> Arc<DecompressedBlock> {
    Arc::new(DecompressedBlock::from_bytes(bytes))
}

#[test]
fn event_sink_emits_typed_and_strings_and_nulls() {
    // Prepare typed columns
    // i64: [42]
    let mut i_bytes = Vec::new();
    i_bytes.extend_from_slice(&42i64.to_le_bytes());
    let i_col = ColumnValues::new_typed_i64(make_block(i_bytes), 0, 1, None);

    // u64: [123]
    let mut u_bytes = Vec::new();
    u_bytes.extend_from_slice(&123u64.to_le_bytes());
    let u_col = ColumnValues::new_typed_u64(make_block(u_bytes), 0, 1, None);

    // f64: [1.25]
    let mut f_bytes = Vec::new();
    f_bytes.extend_from_slice(&1.25f64.to_le_bytes());
    let f_col = ColumnValues::new_typed_f64(make_block(f_bytes), 0, 1, None);

    // bool bitset: value true for index 0
    let b_bytes = vec![0b0000_0001u8];
    let b_col = ColumnValues::new_typed_bool(make_block(b_bytes), 0, 1, None);

    // string range: "hello"
    let s_bytes = b"hello".to_vec();
    let s_block = make_block(s_bytes);
    let s_col = ColumnValues::new(Arc::clone(&s_block), vec![(0, 5)]);

    // null via bitmap: mark index 0 null
    let nulls = vec![0b0000_0001u8];
    let mut nu_bytes = nulls.clone();
    nu_bytes.extend_from_slice(&55u64.to_le_bytes());
    let nu_block = make_block(nu_bytes);
    let nu_col = ColumnValues::new_typed_u64(
        Arc::clone(&nu_block),
        nulls.len(),
        1,
        Some((0, nulls.len())),
    );

    let mut cols: HashMap<String, ColumnValues> = HashMap::new();
    cols.insert("i".into(), i_col);
    cols.insert("u".into(), u_col);
    cols.insert("f".into(), f_col);
    cols.insert("b".into(), b_col);
    cols.insert("s".into(), s_col);
    cols.insert("nu".into(), nu_col);

    let mut sink = EventSink::new();
    sink.on_row(0, &cols);
    let events = sink.into_events();
    assert_eq!(events.len(), 1);
    let ev = &events[0];

    let obj = ev.payload.as_object().expect("payload object");
    assert_eq!(obj.get("i").and_then(|v| v.as_i64()), Some(42));
    assert_eq!(obj.get("u").and_then(|v| v.as_u64()), Some(123));
    assert!((obj.get("f").and_then(|v| v.as_f64()).unwrap() - 1.25).abs() < 1e-9);
    assert_eq!(obj.get("b").and_then(|v| v.as_bool()), Some(true));
    assert_eq!(obj.get("s").and_then(|v| v.as_str()), Some("hello"));
    assert!(obj.get("nu").unwrap().is_null());
}

#[test]
fn event_sink_sets_timestamp_from_u64_and_omits_from_payload() {
    // timestamp as u64 typed
    let mut t_bytes = Vec::new();
    t_bytes.extend_from_slice(&9999u64.to_le_bytes());
    let t_col = ColumnValues::new_typed_u64(make_block(t_bytes), 0, 1, None);

    let mut cols: HashMap<String, ColumnValues> = HashMap::new();
    cols.insert("timestamp".into(), t_col);

    let mut sink = EventSink::new();
    sink.on_row(0, &cols);
    let events = sink.into_events();
    assert_eq!(events.len(), 1);
    let ev = &events[0];
    assert_eq!(ev.timestamp, 9999);
    assert!(ev.payload.get("timestamp").is_none());
}

#[test]
fn event_sink_adds_null_for_missing_values() {
    // Values map contains a field but no value for the row (empty ranges)
    let empty = ColumnValues::new(make_block(Vec::new()), Vec::new());
    let mut cols: HashMap<String, ColumnValues> = HashMap::new();
    cols.insert("x".into(), empty);

    let mut sink = EventSink::new();
    sink.on_row(0, &cols);
    let events = sink.into_events();
    let ev = &events[0];
    assert!(ev.payload.get("x").is_some());
    assert!(ev.payload.get("x").unwrap().is_null());
}

#[test]
fn event_sink_on_row_builds_event_from_columns() {
    use serde_json::json;
    fn make_columns(field_rows: &[(&str, Vec<&str>)]) -> HashMap<String, ColumnValues> {
        let mut map: HashMap<String, ColumnValues> = HashMap::new();
        for (name, rows) in field_rows.iter() {
            let (block, ranges) = DecompressedBlockFactory::create_with_ranges(rows);
            map.insert((*name).to_string(), ColumnValues::new(block, ranges));
        }
        map
    }

    let columns = make_columns(&[
        ("event_type", vec!["order"]),
        ("context_id", vec!["ctx-1"]),
        ("timestamp", vec!["3000000"]),
        ("country", vec!["US"]),
        ("amount", vec!["42"]),
    ]);

    let mut sink = EventSink::new();
    sink.on_row(0, &columns);
    let events = sink.into_events();

    assert_eq!(events.len(), 1);
    let e = &events[0];
    assert_eq!(e.event_type, "order");
    assert_eq!(e.context_id, "ctx-1");
    assert_eq!(e.timestamp, 3_000_000);
    assert_eq!(e.payload["country"], json!("US"));
    assert_eq!(e.payload["amount"], json!(42));
}

#[test]
fn event_sink_on_event_clones_event() {
    let event = EventFactory::new()
        .with("event_type", json!("evt"))
        .with("context_id", json!("c1"))
        .with("timestamp", json!(123))
        .with("payload", json!({"x":"y"}))
        .create();

    let mut sink = EventSink::new();
    sink.on_event(&event);
    let events = sink.into_events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0], event);
}
