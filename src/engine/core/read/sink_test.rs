use std::collections::HashMap;

use serde_json::json;

use crate::command::types::TimeGranularity;
use crate::engine::core::QueryPlan;
use crate::engine::core::column::column_values::ColumnValues;
use crate::engine::core::read::aggregate::plan::{AggregateOpSpec, AggregatePlan};
use crate::engine::core::read::sink::{AggregateSink, EventSink, ResultSink};
use crate::test_helpers::factories::{
    CommandFactory, DecompressedBlockFactory, EventFactory, QueryPlanFactory, SchemaRegistryFactory,
};

fn make_columns(field_rows: &[(&str, Vec<&str>)]) -> HashMap<String, ColumnValues> {
    let mut map: HashMap<String, ColumnValues> = HashMap::new();
    for (name, rows) in field_rows.iter() {
        let (block, ranges) = DecompressedBlockFactory::create_with_ranges(rows);
        map.insert((*name).to_string(), ColumnValues::new(block, ranges));
    }
    map
}

async fn make_plan(event_type: &str, context_id: Option<&str>) -> QueryPlan {
    let registry_factory = SchemaRegistryFactory::new();
    // Ensure event type exists in registry so QueryPlanFactory::create() succeeds
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
    (ts / 2_592_000) * 2_592_000
}

// EventSink -------------------------------------------------------------

#[test]
fn event_sink_on_row_builds_event_from_columns() {
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

// AggregateSink - row path ---------------------------------------------

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

    // 3 matching rows
    let columns = make_columns(&[
        ("user", vec!["u1", "u2", "u1"]),
        ("amount", vec!["10", "20", "30"]),
        ("name", vec!["bob", "amy", "zoe"]),
        // intentionally omit "visits" column to keep its count at 0
    ]);

    sink.on_row(0, &columns);
    sink.on_row(1, &columns);
    sink.on_row(2, &columns);

    let plan = make_plan("order", None).await;
    let events = sink.into_events(&plan);
    assert_eq!(events.len(), 1);
    // Synthetic event metadata should match plan
    assert_eq!(events[0].event_type, "order");
    assert_eq!(events[0].context_id, "");
    let payload = events[0].payload.as_object().unwrap();

    assert_eq!(payload["count"], json!(3));
    assert_eq!(payload["count_unique_user"], json!(2));
    assert_eq!(payload["total_amount"], json!(60));
    assert_eq!(payload["avg_amount"].as_f64().unwrap(), 20.0);
    assert_eq!(payload["min_name"], json!("amy"));
    assert_eq!(payload["max_name"], json!("zoe"));
    // CountField for missing column stays 0
    assert_eq!(payload["count_visits"], json!(0));
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

    // Same month bucket
    let ts1 = 3_000_000u64; // month bucket: 2_592_000
    let ts2 = 3_000_100u64; // same bucket
    let ts3 = 3_000_200u64; // same bucket
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

    // Validate per country
    let mut seen_us = false;
    let mut seen_de = false;
    for e in events {
        let p = e.payload.as_object().unwrap();
        let country = p["country"].as_str().unwrap();
        if country == "US" {
            seen_us = true;
            assert_eq!(p["bucket"], json!(month_bucket(ts1)));
            assert_eq!(p["count"], json!(2));
            assert_eq!(p["total_amount"], json!(15));
        } else if country == "DE" {
            seen_de = true;
            assert_eq!(p["bucket"], json!(month_bucket(ts1)));
            assert_eq!(p["count"], json!(1));
            assert_eq!(p["total_amount"], json!(3));
        }
    }
    assert!(seen_us && seen_de);
}

#[tokio::test]
async fn aggregate_sink_row_month_bucket_using_created_at() {
    // Prepare registry and plan with selected time_field = created_at
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
    let mut sink =
        crate::engine::core::read::sink::AggregateSink::from_query_plan(&plan, &plan_spec);

    // Same month bucket computed from created_at
    let ts1 = 3_000_000u64; // month bucket: 2_592_000
    let ts2 = 3_000_100u64; // same bucket
    let ts3 = 3_200_000u64; // may be same or next depending; keep same bucket boundary
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
    let mut seen_us = false;
    let mut seen_de = false;
    for e in events {
        let p = e.payload.as_object().unwrap();
        let country = p["country"].as_str().unwrap();
        if country == "US" {
            seen_us = true;
            assert_eq!(p["bucket"], json!(month_bucket(ts1)));
            assert_eq!(p["count"], json!(2));
            assert_eq!(p["total_amount"], json!(15));
        } else if country == "DE" {
            seen_de = true;
            assert_eq!(p["bucket"], json!(month_bucket(ts1)));
            assert_eq!(p["count"], json!(1));
            assert_eq!(p["total_amount"], json!(3));
        }
    }
    assert!(seen_us && seen_de);
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

    let columns = make_columns(&[("country", vec!["US"])]); // no "plan" column
    sink.on_row(0, &columns);

    let plan = make_plan("order", None).await;
    let events = sink.into_events(&plan);
    assert_eq!(events.len(), 1);
    let p = events[0].payload.as_object().unwrap();
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
    let p = events[0].payload.as_object().unwrap();
    assert_eq!(p["count"], json!(0));
}

// AggregateSink - event path -------------------------------------------

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

    // Day bucket is 86400 seconds
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
    // Expect 3 groups: US@day1, US@day2, DE@day1
    events.sort_by(|a, b| {
        a.payload["country"]
            .as_str()
            .cmp(&b.payload["country"].as_str())
    });
    assert_eq!(events.len(), 3);

    let mut by_key: HashMap<(String, u64), (i64, i64)> = HashMap::new();
    for e in &events {
        let p = e.payload.as_object().unwrap();
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
    // Registry with created_at
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
    let mut sink = crate::engine::core::read::sink::AggregateSink::from_query_plan(&plan, &agg);

    // Day bucket 86400s computed from created_at in payload
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
    // Expect two buckets for US: day1 and day2
    events.sort_by(|a, b| {
        a.payload["bucket"]
            .as_u64()
            .cmp(&b.payload["bucket"].as_u64())
    });
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].payload["bucket"], json!(86_400));
    assert_eq!(events[0].payload["count"], json!(2));
    assert_eq!(events[1].payload["bucket"], json!(172_800));
    assert_eq!(events[1].payload["count"], json!(1));
}

// AggregateSink - partial snapshot -------------------------------------

#[test]
fn aggregate_sink_into_partial_converts_groups_and_states() {
    let specs = vec![
        AggregateOpSpec::CountAll,
        AggregateOpSpec::Avg {
            field: "amount".into(),
        },
        AggregateOpSpec::Min {
            field: "name".into(),
        },
        AggregateOpSpec::Max {
            field: "name".into(),
        },
        AggregateOpSpec::CountUnique {
            field: "user".into(),
        },
    ];
    let mut sink = AggregateSink::new(specs.clone());

    let columns = make_columns(&[
        ("country", vec!["US", "DE", "US"]),
        ("amount", vec!["10", "3", "20"]),
        ("name", vec!["bob", "amy", "zoe"]),
        ("user", vec!["u1", "u3", "u1"]),
    ]);

    sink.on_row(0, &columns); // US
    sink.on_row(1, &columns); // DE
    sink.on_row(2, &columns); // US

    // Create a grouping sink by country for more than one group before snapshotting
    // Migrate state into partial
    let partial = sink.into_partial();

    // partial should carry specs and (empty) group_by/time_bucket from sink
    assert_eq!(partial.specs, specs);
    // At least one group is expected
    assert!(!partial.groups.is_empty());

    // Find a group with country = "US"; since sink didn't have group_by set, keys will be based on internal grouping (none).
    // We just validate that states vector aligns with specs lengths and types are present.
    for (_k, states) in partial.groups.iter() {
        assert_eq!(states.len(), 5);
        // CountAll present
        match &states[0] {
            crate::engine::core::read::aggregate::partial::AggState::CountAll { .. } => {}
            _ => panic!("expected CountAll state"),
        }
        // Avg snapshot uses approximate (sum as avg, count=1)
        match &states[1] {
            crate::engine::core::read::aggregate::partial::AggState::Avg { .. } => {}
            _ => panic!("expected Avg state"),
        }
        // Min/Max and CountUnique present
        match &states[2] {
            crate::engine::core::read::aggregate::partial::AggState::Min { .. } => {}
            _ => panic!("expected Min state"),
        }
        match &states[3] {
            crate::engine::core::read::aggregate::partial::AggState::Max { .. } => {}
            _ => panic!("expected Max state"),
        }
        match &states[4] {
            crate::engine::core::read::aggregate::partial::AggState::CountUnique { .. } => {}
            _ => panic!("expected CountUnique state"),
        }
        break;
    }
}

// Additional edge cases -------------------------------------------------

#[tokio::test]
async fn aggregate_sink_row_avg_without_rows_is_zero() {
    let specs = vec![AggregateOpSpec::Avg {
        field: "amount".into(),
    }];
    let sink = AggregateSink::new(specs.clone());
    let plan = make_plan("evt", None).await;
    let events = sink.into_events(&plan);
    assert_eq!(events.len(), 1);
    let p = events[0].payload.as_object().unwrap();
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
    let p = events[0].payload.as_object().unwrap();
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
    let p = events[0].payload.as_object().unwrap();
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
    let p = events[0].payload.as_object().unwrap();
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
        .with("timestamp", json!(3_600)) // 1h
        .with("payload", json!({"country":"US"}))
        .create();
    let e2 = EventFactory::new()
        .with("timestamp", json!(3_650)) // same hour
        .with("payload", json!({"country":"US"}))
        .create();
    let e3 = EventFactory::new()
        .with("timestamp", json!(7_200)) // next hour
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
    // Missing user -> get_field_value(unknown) => ""
    let e1 = EventFactory::new().with("payload", json!({})).create();
    // Explicit empty user -> ""
    let e2 = EventFactory::new()
        .with("payload", json!({"user":""}))
        .create();
    sink.on_event(&e1);
    sink.on_event(&e2);
    let plan = make_plan("evt", None).await;
    let events = sink.into_events(&plan);
    let p = events[0].payload.as_object().unwrap();
    assert_eq!(p["count_unique_user"], json!(1));
}

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
    sink.on_row(0, &columns); // US
    sink.on_row(1, &columns); // DE
    sink.on_row(2, &columns); // FR -> should be ignored due to limit 2

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
    sink.on_event(&e2); // should be dropped as new group beyond limit

    let plan = make_plan("evt_limit_event", None).await;
    let events = sink.into_events(&plan);
    assert_eq!(events.len(), 1);
    let p = events[0].payload.as_object().unwrap();
    assert!(p["country"].as_str().unwrap() == "US" || p["country"].as_str().unwrap() == "DE");
}
