use crate::engine::core::{WriteJob, ZonePlan};
use crate::test_helpers::factories::{EventFactory, ResolverFactory};
use serde_json::json;
use std::collections::HashSet;
use tempfile::tempdir;

#[test]
fn builds_jobs_for_zone_plan_with_events() {
    let event_type = "login";
    let uid = "uid-login";

    let events = EventFactory::new()
        .with("event_type", event_type)
        .with("payload", json!({ "device": "mobile", "ip": "127.0.0.1" }))
        .create_list(2);

    let zone = ZonePlan {
        id: 42,
        start_index: 0,
        end_index: 1,
        events,
        uid: uid.to_string(),
        event_type: event_type.to_string(),
        segment_id: 9,
    };

    let resolver = ResolverFactory::new().with(event_type, uid).create();

    let dir = tempdir().unwrap();
    let jobs = WriteJob::build(&[zone], dir.path(), &resolver);

    assert_eq!(jobs.len(), 14);

    let expected_fields = vec![
        "device",
        "ip",
        "index", // this is specific to the factory
        "timestamp",
        "context_id",
        "event_type",
        "event_id",
    ];

    for job in &jobs {
        assert_eq!(job.key.0, "login");
        assert_eq!(job.zone_id, 42);

        let suffix = format!("{}_{}.col", uid, job.key.1);
        assert!(
            job.path.ends_with(&suffix),
            "Unexpected path: {:?}, expected suffix: {:?}",
            job.path,
            suffix
        );

        assert!(
            expected_fields.contains(&job.key.1.as_str()),
            "Unexpected field: {}",
            job.key.1
        );
    }
}

#[test]
fn builds_jobs_with_empty_strings_for_missing_fields() {
    // Test that columnar storage writes empty strings for missing fields
    // This is critical for maintaining consistent column offsets
    let event_type = "action";
    let uid = "uid-action";

    // First event has field 'country', second doesn't
    let event1 = EventFactory::new()
        .with("event_type", event_type)
        .with("payload", json!({"country": "US", "score": 100}))
        .create();

    let event2 = EventFactory::new()
        .with("event_type", event_type)
        .with("payload", json!({"score": 200})) // Missing 'country'
        .create();

    let zone = ZonePlan {
        id: 1,
        start_index: 0,
        end_index: 1,
        events: vec![event1, event2],
        uid: uid.to_string(),
        event_type: event_type.to_string(),
        segment_id: 1,
    };

    let resolver = ResolverFactory::new().with(event_type, uid).create();
    let dir = tempdir().unwrap();
    let jobs = WriteJob::build(&[zone], dir.path(), &resolver);

    // Collect all unique field names
    let mut field_names: HashSet<String> = HashSet::new();
    for job in &jobs {
        field_names.insert(job.key.1.clone());
    }

    // Verify 'country' field is present for BOTH events
    assert!(
        field_names.contains("country"),
        "country field should be present"
    );
    assert!(
        field_names.contains("score"),
        "score field should be present"
    );

    // Count how many jobs exist for 'country' field
    let country_jobs: Vec<_> = jobs.iter().filter(|j| j.key.1 == "country").collect();

    assert_eq!(
        country_jobs.len(),
        2,
        "Should have 2 country jobs (one per event)"
    );

    // Missing field now recorded as explicit null in typed pipeline
    let country_values: HashSet<_> = country_jobs.iter().map(|j| j.value.clone()).collect();
    assert!(
        country_values.contains(&json!("US")),
        "Should have US value"
    );
    assert!(
        country_values.contains(&serde_json::Value::Null),
        "Should have null for missing field"
    );
}
