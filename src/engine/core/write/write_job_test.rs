use crate::engine::core::{WriteJob, ZonePlan};
use crate::test_helpers::factories::{EventFactory, ResolverFactory};
use serde_json::json;
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

    assert_eq!(jobs.len(), 12);

    let expected_fields = vec![
        "device",
        "ip",
        "index", // this is specific to the factory
        "timestamp",
        "context_id",
        "event_type",
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
