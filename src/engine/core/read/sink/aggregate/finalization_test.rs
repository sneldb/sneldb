use super::finalization::{into_events, into_partial};
use super::group_key::GroupKey;
use crate::command::types::TimeGranularity;
use crate::engine::core::read::aggregate::ops::AggregatorImpl;
use crate::engine::core::read::aggregate::plan::AggregateOpSpec;
use crate::engine::core::{Event, QueryPlan};
use crate::test_helpers::factories::{
    CommandFactory, QueryPlanFactory, SchemaRegistryFactory,
};
use ahash::RandomState as AHashRandomState;
use serde_json::{Value, json};
use std::collections::HashMap;

fn make_group_key(prehash: u64, bucket: Option<u64>, groups: Vec<String>) -> GroupKey {
    GroupKey {
        prehash,
        bucket,
        groups: groups
            .iter()
            .map(|s| {
                if let Ok(i) = s.parse::<i64>() {
                    super::group_key::GroupValue::Int(i)
                } else {
                    super::group_key::GroupValue::Str(s.clone())
                }
            })
            .collect(),
        groups_str: Some(groups),
    }
}

fn make_aggregator(spec: &AggregateOpSpec) -> AggregatorImpl {
    AggregatorImpl::from_spec(spec)
}

async fn make_plan(event_type: &str, context_id: Option<&str>) -> QueryPlan {
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

fn payload_map(event: &Event) -> serde_json::Map<String, Value> {
    match event.payload_as_json() {
        Value::Object(map) => map,
        _ => panic!("expected object payload"),
    }
}

// into_events tests ---------------------------------------------------------

#[tokio::test]
async fn finalization_into_events_empty_groups_creates_default_event() {
    let specs = vec![AggregateOpSpec::CountAll];
    let groups: HashMap<GroupKey, Vec<AggregatorImpl>, AHashRandomState> =
        HashMap::with_hasher(AHashRandomState::new());
    let plan = make_plan("test_event", None).await;

    let events = into_events(groups, specs, None, &plan);

    assert_eq!(events.len(), 1);
    let p = payload_map(&events[0]);
    assert_eq!(p["count"], json!(0));
    assert_eq!(events[0].event_type, "test_event");
}

#[tokio::test]
async fn finalization_into_events_single_group_count_all() {
    let specs = vec![AggregateOpSpec::CountAll];
    let mut groups: HashMap<GroupKey, Vec<AggregatorImpl>, AHashRandomState> =
        HashMap::with_hasher(AHashRandomState::new());
    let key = make_group_key(1, None, vec![]);
    let mut agg = make_aggregator(&specs[0]);
    // Simulate some updates
    let columns = HashMap::new();
    agg.update(0, &columns);
    agg.update(1, &columns);
    groups.insert(key, vec![agg]);

    let plan = make_plan("test_event", None).await;
    let events = into_events(groups, specs, None, &plan);

    assert_eq!(events.len(), 1);
    let p = payload_map(&events[0]);
    assert_eq!(p["count"], json!(2));
}

#[tokio::test]
async fn finalization_into_events_with_bucket() {
    let specs = vec![AggregateOpSpec::CountAll];
    let mut groups: HashMap<GroupKey, Vec<AggregatorImpl>, AHashRandomState> =
        HashMap::with_hasher(AHashRandomState::new());
    let key = make_group_key(1, Some(86400), vec![]);
    let agg = make_aggregator(&specs[0]);
    groups.insert(key, vec![agg]);

    let plan = make_plan("test_event", None).await;
    let events = into_events(groups, specs, None, &plan);

    assert_eq!(events.len(), 1);
    let p = payload_map(&events[0]);
    assert_eq!(p["bucket"], json!(86400));
}

#[tokio::test]
async fn finalization_into_events_with_group_by() {
    let specs = vec![AggregateOpSpec::CountAll];
    let mut groups: HashMap<GroupKey, Vec<AggregatorImpl>, AHashRandomState> =
        HashMap::with_hasher(AHashRandomState::new());
    let key = make_group_key(1, None, vec!["US".to_string()]);
    let agg = make_aggregator(&specs[0]);
    groups.insert(key, vec![agg]);

    let plan = make_plan("test_event", None).await;
    let group_by = Some(vec!["country".to_string()]);
    let events = into_events(groups, specs, group_by, &plan);

    assert_eq!(events.len(), 1);
    let p = payload_map(&events[0]);
    assert_eq!(p["country"], json!("US"));
}

#[tokio::test]
async fn finalization_into_events_multiple_groups() {
    let specs = vec![AggregateOpSpec::CountAll];
    let mut groups: HashMap<GroupKey, Vec<AggregatorImpl>, AHashRandomState> =
        HashMap::with_hasher(AHashRandomState::new());

    let key1 = make_group_key(1, None, vec!["US".to_string()]);
    let key2 = make_group_key(2, None, vec!["DE".to_string()]);
    groups.insert(key1, vec![make_aggregator(&specs[0])]);
    groups.insert(key2, vec![make_aggregator(&specs[0])]);

    let plan = make_plan("test_event", None).await;
    let group_by = Some(vec!["country".to_string()]);
    let events = into_events(groups, specs, group_by, &plan);

    assert_eq!(events.len(), 2);
    let countries: Vec<String> = events
        .iter()
        .map(|e| {
            let p = payload_map(e);
            p["country"].as_str().unwrap().to_string()
        })
        .collect();
    assert!(countries.contains(&"US".to_string()));
    assert!(countries.contains(&"DE".to_string()));
}

#[tokio::test]
async fn finalization_into_events_all_aggregation_types() {
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
    let mut groups: HashMap<GroupKey, Vec<AggregatorImpl>, AHashRandomState> =
        HashMap::with_hasher(AHashRandomState::new());
    let key = make_group_key(1, None, vec![]);
    let aggs: Vec<AggregatorImpl> = specs.iter().map(|s| make_aggregator(s)).collect();
    groups.insert(key, aggs);

    let plan = make_plan("test_event", None).await;
    let events = into_events(groups, specs, None, &plan);

    assert_eq!(events.len(), 1);
    let p = payload_map(&events[0]);
    assert!(p.contains_key("count"));
    assert!(p.contains_key("count_visits"));
    assert!(p.contains_key("count_unique_user"));
    assert!(p.contains_key("total_amount"));
    assert!(p.contains_key("avg_amount"));
    assert!(p.contains_key("min_name"));
    assert!(p.contains_key("max_name"));
}

#[tokio::test]
async fn finalization_into_events_with_context_id() {
    let specs = vec![AggregateOpSpec::CountAll];
    let groups: HashMap<GroupKey, Vec<AggregatorImpl>, AHashRandomState> =
        HashMap::with_hasher(AHashRandomState::new());
    let plan = make_plan("test_event", Some("ctx123")).await;

    let events = into_events(groups, specs, None, &plan);

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].context_id, "ctx123");
}

#[tokio::test]
async fn finalization_into_events_multiple_group_by_fields() {
    let specs = vec![AggregateOpSpec::CountAll];
    let mut groups: HashMap<GroupKey, Vec<AggregatorImpl>, AHashRandomState> =
        HashMap::with_hasher(AHashRandomState::new());
    let key = make_group_key(1, None, vec!["US".to_string(), "CA".to_string()]);
    groups.insert(key, vec![make_aggregator(&specs[0])]);

    let plan = make_plan("test_event", None).await;
    let group_by = Some(vec!["country".to_string(), "state".to_string()]);
    let events = into_events(groups, specs, group_by, &plan);

    assert_eq!(events.len(), 1);
    let p = payload_map(&events[0]);
    assert_eq!(p["country"], json!("US"));
    assert_eq!(p["state"], json!("CA"));
}

// into_partial tests --------------------------------------------------------

#[test]
fn finalization_into_partial_empty_groups() {
    let specs = vec![AggregateOpSpec::CountAll];
    let groups: HashMap<GroupKey, Vec<AggregatorImpl>, AHashRandomState> =
        HashMap::with_hasher(AHashRandomState::new());

    let partial = into_partial(groups, specs.clone(), None, None);

    assert_eq!(partial.specs, specs);
    assert_eq!(partial.group_by, None);
    assert_eq!(partial.time_bucket, None);
    assert_eq!(partial.groups.len(), 0);
}

#[test]
fn finalization_into_partial_single_group() {
    let specs = vec![AggregateOpSpec::CountAll];
    let mut groups: HashMap<GroupKey, Vec<AggregatorImpl>, AHashRandomState> =
        HashMap::with_hasher(AHashRandomState::new());
    let key = make_group_key(1, Some(86400), vec!["US".to_string()]);
    groups.insert(key, vec![make_aggregator(&specs[0])]);

    let partial = into_partial(groups, specs.clone(), None, Some(TimeGranularity::Day));

    assert_eq!(partial.specs, specs);
    assert_eq!(partial.time_bucket, Some(TimeGranularity::Day));
    assert_eq!(partial.groups.len(), 1);

    let partial_key = partial.groups.keys().next().unwrap();
    assert_eq!(partial_key.bucket, Some(86400));
    assert_eq!(partial_key.groups, vec!["US".to_string()]);
}

#[test]
fn finalization_into_partial_multiple_groups() {
    let specs = vec![AggregateOpSpec::CountAll];
    let mut groups: HashMap<GroupKey, Vec<AggregatorImpl>, AHashRandomState> =
        HashMap::with_hasher(AHashRandomState::new());

    let key1 = make_group_key(1, None, vec!["US".to_string()]);
    let key2 = make_group_key(2, None, vec!["DE".to_string()]);
    groups.insert(key1, vec![make_aggregator(&specs[0])]);
    groups.insert(key2, vec![make_aggregator(&specs[0])]);

    let group_by = Some(vec!["country".to_string()]);
    let partial = into_partial(groups, specs.clone(), group_by.clone(), None);

    assert_eq!(partial.group_by, group_by);
    assert_eq!(partial.groups.len(), 2);
}

#[test]
fn finalization_into_partial_preserves_specs() {
    let specs = vec![
        AggregateOpSpec::CountAll,
        AggregateOpSpec::Total {
            field: "amount".into(),
        },
    ];
    let groups: HashMap<GroupKey, Vec<AggregatorImpl>, AHashRandomState> =
        HashMap::with_hasher(AHashRandomState::new());

    let partial = into_partial(groups, specs.clone(), None, None);

    assert_eq!(partial.specs.len(), 2);
    assert_eq!(partial.specs[0], specs[0]);
    assert_eq!(partial.specs[1], specs[1]);
}

#[test]
fn finalization_into_partial_converts_group_key_correctly() {
    let specs = vec![AggregateOpSpec::CountAll];
    let mut groups: HashMap<GroupKey, Vec<AggregatorImpl>, AHashRandomState> =
        HashMap::with_hasher(AHashRandomState::new());
    let key = make_group_key(100, Some(172800), vec!["US".to_string(), "NY".to_string()]);
    groups.insert(key, vec![make_aggregator(&specs[0])]);

    let partial = into_partial(groups, specs, None, None);

    let partial_key = partial.groups.keys().next().unwrap();
    assert_eq!(partial_key.bucket, Some(172800));
    assert_eq!(partial_key.groups, vec!["US".to_string(), "NY".to_string()]);
}

