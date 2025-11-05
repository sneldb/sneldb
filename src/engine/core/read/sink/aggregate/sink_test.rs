use crate::command::types::TimeGranularity;
use crate::engine::core::column::column_values::ColumnValues;
use crate::engine::core::read::aggregate::plan::{AggregateOpSpec, AggregatePlan};
use crate::engine::core::read::sink::AggregateSink as ReExportedAggregateSink;
use crate::engine::core::read::sink::ResultSink;
use crate::engine::core::read::sink::aggregate::sink::AggregateSink;
use crate::test_helpers::factories::{
    CommandFactory, DecompressedBlockFactory, EventFactory, QueryPlanFactory, SchemaRegistryFactory,
};
use serde_json::{Value, json};
use std::collections::HashMap;

fn payload_map(event: &crate::engine::core::Event) -> serde_json::Map<String, Value> {
    match event.payload_as_json() {
        Value::Object(map) => map,
        _ => panic!("expected object payload"),
    }
}

fn make_columns(field_rows: &[(&str, Vec<&str>)]) -> HashMap<String, ColumnValues> {
    let mut map: HashMap<String, ColumnValues> = HashMap::new();
    for (name, rows) in field_rows.iter() {
        let (block, ranges) = DecompressedBlockFactory::create_with_ranges(rows);
        map.insert((*name).to_string(), ColumnValues::new(block, ranges));
    }
    map
}

async fn make_plan(event_type: &str, context_id: Option<&str>) -> crate::engine::core::QueryPlan {
    let registry_factory = SchemaRegistryFactory::new();
    registry_factory
        .define_with_fields(event_type, &[("dummy", "int")])
        .await
        .unwrap();
    let registry = registry_factory.registry();
    let mut cf = CommandFactory::query().with_event_type(event_type);
    if let Some(cid) = context_id {
        cf = cf.with_context_id(cid);
    }
    let cmd = cf.create();
    QueryPlanFactory::new()
        .with_command(cmd)
        .with_registry(registry)
        .create()
        .await
}

fn month_bucket(ts: u64) -> u64 {
    use chrono::{DateTime, Datelike, Utc};
    let dt = DateTime::from_timestamp(ts as i64, 0).unwrap();
    let month_start = dt
        .date_naive()
        .with_day(1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_local_timezone(Utc)
        .unwrap();
    month_start.timestamp() as u64
}

// Row path ---------------------------------------------------------------

#[tokio::test]
async fn aggregate_sink_row_no_grouping_computes_all_metrics() {
    let specs = vec![
        AggregateOpSpec::CountAll,
        AggregateOpSpec::CountField {
            field: "visits".into(),
        },
        AggregateOpSpec::CountUnique {
            field: "user".into(),
        },
        AggregateOpSpec::Total {
            field: "amount".into(),
        },
        AggregateOpSpec::Avg {
            field: "amount".into(),
        },
        AggregateOpSpec::Min {
            field: "name".into(),
        },
        AggregateOpSpec::Max {
            field: "name".into(),
        },
    ];
    let mut sink = AggregateSink::new(specs.clone());

    let columns = make_columns(&[
        ("user", vec!["u1", "u2", "u1"]),
        ("amount", vec!["10", "20", "30"]),
        ("name", vec!["bob", "amy", "zoe"]),
    ]);

    sink.on_row(0, &columns);
    sink.on_row(1, &columns);
    sink.on_row(2, &columns);

    let plan = make_plan("order", None).await;
    let events = sink.into_events(&plan);
    assert_eq!(events.len(), 1);
    let p = payload_map(&events[0]);
    assert_eq!(p["count"], json!(3));
    assert_eq!(p["count_unique_user"], json!(2));
    assert_eq!(p["total_amount"], json!(60));
    assert_eq!(p["avg_amount"].as_f64().unwrap(), 20.0);
    assert_eq!(p["min_name"], json!("amy"));
    assert_eq!(p["max_name"], json!("zoe"));
    assert_eq!(p["count_visits"], json!(0));
}

#[tokio::test]
async fn aggregate_sink_row_group_by_and_month_bucket() {
    let specs = vec![
        AggregateOpSpec::CountAll,
        AggregateOpSpec::Total {
            field: "amount".into(),
        },
    ];
    let plan_spec = AggregatePlan {
        ops: specs.clone(),
        group_by: Some(vec!["country".into()]),
        time_bucket: Some(TimeGranularity::Month),
    };
    let mut sink = AggregateSink::from_plan(&plan_spec);

    let ts1 = 3_000_000u64;
    let ts2 = 3_000_100u64;
    let ts3 = 3_000_200u64;
    let columns = make_columns(&[
        (
            "timestamp",
            vec![&ts1.to_string(), &ts2.to_string(), &ts3.to_string()],
        ),
        ("country", vec!["US", "US", "DE"]),
        ("amount", vec!["10", "5", "3"]),
    ]);

    sink.on_row(0, &columns);
    sink.on_row(1, &columns);
    sink.on_row(2, &columns);

    let plan = make_plan("order", None).await;
    let events = sink.into_events(&plan);
    assert_eq!(events.len(), 2);
    let mut seen_us = false;
    let mut seen_de = false;
    for e in events {
        let p = payload_map(&e);
        let country = p["country"].as_str().unwrap();
        if country == "US" {
            seen_us = true;
            assert_eq!(p["bucket"], json!(month_bucket(ts1)));
            assert_eq!(p["count"], json!(2));
            assert_eq!(p["total_amount"], json!(15));
        } else if country == "DE" {
            seen_de = true;
        }
    }
    assert!(seen_us && seen_de);
}

#[tokio::test]
async fn aggregate_sink_row_month_bucket_using_created_at() {
    let registry_factory = SchemaRegistryFactory::new();
    registry_factory
        .define_with_fields(
            "order2",
            &[
                ("created_at", "datetime"),
                ("country", "string"),
                ("amount", "int"),
            ],
        )
        .await
        .unwrap();
    let registry = registry_factory.registry();
    let cmd = CommandFactory::query()
        .with_event_type("order2")
        .with_time_field("created_at")
        .create();
    let plan = QueryPlanFactory::new()
        .with_command(cmd)
        .with_registry(registry.clone())
        .create()
        .await;

    let specs = vec![
        AggregateOpSpec::CountAll,
        AggregateOpSpec::Total {
            field: "amount".into(),
        },
    ];
    let plan_spec = AggregatePlan {
        ops: specs.clone(),
        group_by: Some(vec!["country".into()]),
        time_bucket: Some(TimeGranularity::Month),
    };
    let mut sink = ReExportedAggregateSink::from_query_plan(&plan, &plan_spec);

    let ts1 = 3_000_000u64;
    let ts2 = 3_000_100u64;
    let ts3 = 3_200_000u64;
    let columns = make_columns(&[
        (
            "created_at",
            vec![&ts1.to_string(), &ts2.to_string(), &ts3.to_string()],
        ),
        ("country", vec!["US", "US", "DE"]),
        ("amount", vec!["10", "5", "3"]),
    ]);

    sink.on_row(0, &columns);
    sink.on_row(1, &columns);
    sink.on_row(2, &columns);

    let events = sink.into_events(&plan);
    assert_eq!(events.len(), 2);
}

#[tokio::test]
async fn aggregate_sink_row_missing_groupby_field_emits_empty_string() {
    let specs = vec![AggregateOpSpec::CountAll];
    let plan_spec = AggregatePlan {
        ops: specs.clone(),
        group_by: Some(vec!["country".into(), "plan".into()]),
        time_bucket: None,
    };
    let mut sink = AggregateSink::from_plan(&plan_spec);
    let columns = make_columns(&[("country", vec!["US"])]);
    sink.on_row(0, &columns);
    let plan = make_plan("order", None).await;
    let events = sink.into_events(&plan);
    assert_eq!(events.len(), 1);
    let p = payload_map(&events[0]);
    assert_eq!(p["country"], json!("US"));
    assert_eq!(p["plan"], json!(""));
}

#[test]
fn aggregate_sink_group_count_debug_reports_groups() {
    let specs = vec![AggregateOpSpec::CountAll];
    let plan_spec = AggregatePlan {
        ops: specs.clone(),
        group_by: Some(vec!["country".into()]),
        time_bucket: None,
    };
    let mut sink = AggregateSink::from_plan(&plan_spec);
    let columns = make_columns(&[("country", vec!["US", "US", "DE"])]);
    sink.on_row(0, &columns);
    sink.on_row(1, &columns);
    sink.on_row(2, &columns);
    assert_eq!(sink.group_count_debug(), 2);
}

#[tokio::test]
async fn aggregate_sink_into_events_with_no_updates_emits_default_zero_event() {
    let specs = vec![AggregateOpSpec::CountAll];
    let sink = AggregateSink::new(specs.clone());
    let plan = make_plan("evt", Some("ctx")).await;
    let events = sink.into_events(&plan);
    assert_eq!(events.len(), 1);
    let p = payload_map(&events[0]);
    assert_eq!(p["count"], json!(0));
}

// Event path -------------------------------------------------------------

#[tokio::test]
async fn aggregate_sink_event_group_by_and_day_bucket() {
    let specs = vec![
        AggregateOpSpec::CountAll,
        AggregateOpSpec::Total {
            field: "amount".into(),
        },
    ];
    let plan_spec = AggregatePlan {
        ops: specs.clone(),
        group_by: Some(vec!["country".into()]),
        time_bucket: Some(TimeGranularity::Day),
    };
    let mut sink = AggregateSink::from_plan(&plan_spec);

    let e1 = EventFactory::new()
        .with("timestamp", json!(86_401))
        .with("payload", json!({"country":"US", "amount": 10}))
        .create();
    let e2 = EventFactory::new()
        .with("timestamp", json!(86_450))
        .with("payload", json!({"country":"US", "amount": 5}))
        .create();
    let e3 = EventFactory::new()
        .with("timestamp", json!(172_800))
        .with("payload", json!({"country":"US", "amount": 2}))
        .create();
    let e4 = EventFactory::new()
        .with("timestamp", json!(86_430))
        .with("payload", json!({"country":"DE", "amount": 3}))
        .create();

    sink.on_event(&e1);
    sink.on_event(&e2);
    sink.on_event(&e3);
    sink.on_event(&e4);

    let plan = make_plan("evt", None).await;
    let mut events = sink.into_events(&plan);
    events.sort_by(|a, b| {
        let a_country = a
            .payload
            .get("country")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let b_country = b
            .payload
            .get("country")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        a_country.cmp(b_country)
    });
    assert_eq!(events.len(), 3);

    let mut by_key: HashMap<(String, u64), (i64, i64)> = HashMap::new();
    for e in &events {
        let p = payload_map(e);
        let country = p["country"].as_str().unwrap().to_string();
        let bucket = p["bucket"].as_u64().unwrap();
        let count = p["count"].as_i64().unwrap();
        let total = p["total_amount"].as_i64().unwrap();
        by_key.insert((country, bucket), (count, total));
    }
    assert_eq!(by_key.get(&("US".into(), 86_400)).cloned(), Some((2, 15)));
    assert_eq!(by_key.get(&("US".into(), 172_800)).cloned(), Some((1, 2)));
    assert_eq!(by_key.get(&("DE".into(), 86_400)).cloned(), Some((1, 3)));
}

#[tokio::test]
async fn aggregate_sink_event_bucket_using_created_at() {
    let registry_factory = SchemaRegistryFactory::new();
    registry_factory
        .define_with_fields(
            "evt_time_bucket",
            &[
                ("created_at", "datetime"),
                ("country", "string"),
                ("amount", "int"),
            ],
        )
        .await
        .unwrap();
    let registry = registry_factory.registry();
    let cmd = CommandFactory::query()
        .with_event_type("evt_time_bucket")
        .with_time_field("created_at")
        .create();
    let plan = QueryPlanFactory::new()
        .with_command(cmd)
        .with_registry(registry)
        .create()
        .await;

    let specs = vec![AggregateOpSpec::CountAll];
    let agg = AggregatePlan {
        ops: specs.clone(),
        group_by: Some(vec!["country".into()]),
        time_bucket: Some(TimeGranularity::Day),
    };
    let mut sink = ReExportedAggregateSink::from_query_plan(&plan, &agg);

    let e1 = EventFactory::new()
        .with("timestamp", json!(1))
        .with(
            "payload",
            json!({"country":"US", "amount": 10, "created_at": 86_401}),
        )
        .create();
    let e2 = EventFactory::new()
        .with("timestamp", json!(1))
        .with(
            "payload",
            json!({"country":"US", "amount": 5, "created_at": 86_450}),
        )
        .create();
    let e3 = EventFactory::new()
        .with("timestamp", json!(1))
        .with(
            "payload",
            json!({"country":"US", "amount": 2, "created_at": 172_800}),
        )
        .create();
    sink.on_event(&e1);
    sink.on_event(&e2);
    sink.on_event(&e3);

    let mut events = sink.into_events(&plan);
    events.sort_by(|a, b| {
        let a_bucket = a
            .payload
            .get("bucket")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let b_bucket = b
            .payload
            .get("bucket")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        a_bucket.cmp(&b_bucket)
    });
    assert_eq!(events.len(), 2);
    let first_payload = payload_map(&events[0]);
    assert_eq!(first_payload["bucket"], json!(86_400));
    assert_eq!(first_payload["count"], json!(2));
    let second_payload = payload_map(&events[1]);
    assert_eq!(second_payload["bucket"], json!(172_800));
    assert_eq!(second_payload["count"], json!(1));
}

// Edge cases -------------------------------------------------------------

#[tokio::test]
async fn aggregate_sink_row_avg_without_rows_is_zero() {
    let specs = vec![AggregateOpSpec::Avg {
        field: "amount".into(),
    }];
    let sink = AggregateSink::new(specs.clone());
    let plan = make_plan("evt", None).await;
    let events = sink.into_events(&plan);
    assert_eq!(events.len(), 1);
    let p = payload_map(&events[0]);
    assert_eq!(p["avg_amount"].as_f64().unwrap(), 0.0);
}

#[tokio::test]
async fn aggregate_sink_row_count_field_counts_empty_strings() {
    let specs = vec![AggregateOpSpec::CountField {
        field: "visits".into(),
    }];
    let mut sink = AggregateSink::new(specs.clone());
    let columns = make_columns(&[("visits", vec!["", ""])]);
    sink.on_row(0, &columns);
    sink.on_row(1, &columns);
    let plan = make_plan("evt", None).await;
    let events = sink.into_events(&plan);
    let p = payload_map(&events[0]);
    assert_eq!(p["count_visits"], json!(2));
}

#[tokio::test]
async fn aggregate_sink_row_sum_avg_ignore_non_numeric() {
    let specs = vec![
        AggregateOpSpec::Total {
            field: "amount".into(),
        },
        AggregateOpSpec::Avg {
            field: "amount".into(),
        },
    ];
    let mut sink = AggregateSink::new(specs.clone());
    let columns = make_columns(&[("amount", vec!["x", "10"])]);
    sink.on_row(0, &columns);
    sink.on_row(1, &columns);
    let plan = make_plan("evt", None).await;
    let events = sink.into_events(&plan);
    let p = payload_map(&events[0]);
    assert_eq!(p["total_amount"], json!(10));
    assert_eq!(p["avg_amount"].as_f64().unwrap(), 10.0);
}

#[tokio::test]
async fn aggregate_sink_row_min_max_mixed_types_prefers_numeric() {
    let specs = vec![
        AggregateOpSpec::Min {
            field: "name".into(),
        },
        AggregateOpSpec::Max {
            field: "name".into(),
        },
    ];
    let mut sink = AggregateSink::new(specs.clone());
    let columns = make_columns(&[("name", vec!["10", "2", "bob"])]);
    sink.on_row(0, &columns);
    sink.on_row(1, &columns);
    sink.on_row(2, &columns);
    let plan = make_plan("evt", None).await;
    let events = sink.into_events(&plan);
    let p = payload_map(&events[0]);
    assert_eq!(p["min_name"], json!("2"));
    assert_eq!(p["max_name"], json!("10"));
}

#[tokio::test]
async fn aggregate_sink_event_bucket_by_hour() {
    let specs = vec![AggregateOpSpec::CountAll];
    let plan_spec = AggregatePlan {
        ops: specs.clone(),
        group_by: Some(vec!["country".into()]),
        time_bucket: Some(TimeGranularity::Hour),
    };
    let mut sink = AggregateSink::from_plan(&plan_spec);
    let e1 = EventFactory::new()
        .with("timestamp", json!(3_600))
        .with("payload", json!({"country":"US"}))
        .create();
    let e2 = EventFactory::new()
        .with("timestamp", json!(3_650))
        .with("payload", json!({"country":"US"}))
        .create();
    let e3 = EventFactory::new()
        .with("timestamp", json!(7_200))
        .with("payload", json!({"country":"US"}))
        .create();
    sink.on_event(&e1);
    sink.on_event(&e2);
    sink.on_event(&e3);
    let plan = make_plan("evt", None).await;
    let events = sink.into_events(&plan);
    assert_eq!(events.len(), 2);
}

#[tokio::test]
async fn aggregate_sink_event_count_unique_empty_and_missing_collapsed() {
    let specs = vec![AggregateOpSpec::CountUnique {
        field: "user".into(),
    }];
    let plan_spec = AggregatePlan {
        ops: specs.clone(),
        group_by: None,
        time_bucket: None,
    };
    let mut sink = AggregateSink::from_plan(&plan_spec);
    let e1 = EventFactory::new().with("payload", json!({})).create();
    let e2 = EventFactory::new()
        .with("payload", json!({"user":""}))
        .create();
    sink.on_event(&e1);
    sink.on_event(&e2);
    let plan = make_plan("evt", None).await;
    let events = sink.into_events(&plan);
    let p = payload_map(&events[0]);
    assert_eq!(p["count_unique_user"], json!(1));
}

// Group limit behavior ---------------------------------------------------

#[tokio::test]
async fn aggregate_sink_row_respects_group_limit() {
    let specs = vec![AggregateOpSpec::CountAll];
    let mut sink = AggregateSink::from_plan(&AggregatePlan {
        ops: specs.clone(),
        group_by: Some(vec!["country".into()]),
        time_bucket: None,
    })
    .with_group_limit(Some(2));
    let columns = make_columns(&[("country", vec!["US", "DE", "FR"])]);
    sink.on_row(0, &columns);
    sink.on_row(1, &columns);
    sink.on_row(2, &columns);
    let plan = make_plan("evt_limit_row", None).await;
    let events = sink.into_events(&plan);
    assert_eq!(events.len(), 2);
}

#[tokio::test]
async fn aggregate_sink_event_respects_group_limit() {
    let specs = vec![AggregateOpSpec::CountAll];
    let mut sink = AggregateSink::from_plan(&AggregatePlan {
        ops: specs.clone(),
        group_by: Some(vec!["country".into()]),
        time_bucket: None,
    })
    .with_group_limit(Some(1));
    let e1 = EventFactory::new()
        .with("payload", json!({"country":"US"}))
        .create();
    let e2 = EventFactory::new()
        .with("payload", json!({"country":"DE"}))
        .create();
    sink.on_event(&e1);
    sink.on_event(&e2);
    let plan = make_plan("evt_limit_event", None).await;
    let events = sink.into_events(&plan);
    assert_eq!(events.len(), 1);
}

#[tokio::test]
async fn aggregate_sink_deduplicates_events_by_id() {
    let specs = vec![AggregateOpSpec::CountAll];
    let mut sink = AggregateSink::from_plan(&AggregatePlan {
        ops: specs.clone(),
        group_by: None,
        time_bucket: None,
    });

    let event = EventFactory::new()
        .with("payload", json!({"value": 1}))
        .with("event_id", json!(123_u64))
        .create();

    // Same event twice should be counted once
    sink.on_event(&event);
    sink.on_event(&event);

    let plan = make_plan("evt_dedupe_event", None).await;
    let events = sink.into_events(&plan);
    assert_eq!(events.len(), 1);
    let payload = payload_map(&events[0]);
    assert_eq!(payload["count"], json!(1));
}

#[tokio::test]
async fn aggregate_sink_deduplicates_rows_by_event_id_column() {
    let specs = vec![AggregateOpSpec::CountAll];
    let mut sink = AggregateSink::from_plan(&AggregatePlan {
        ops: specs.clone(),
        group_by: None,
        time_bucket: None,
    });

    // Two rows share the same event_id; only the first should be counted
    let columns = make_columns(&[
        ("event_id", vec!["555", "555"]),
        ("value", vec!["10", "20"]),
    ]);

    sink.on_row(0, &columns);
    sink.on_row(1, &columns);

    let plan = make_plan("evt_dedupe_rows", None).await;
    let events = sink.into_events(&plan);
    assert_eq!(events.len(), 1);
    let payload = payload_map(&events[0]);
    assert_eq!(payload["count"], json!(1));
}
