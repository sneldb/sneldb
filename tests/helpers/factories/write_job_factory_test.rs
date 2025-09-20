use crate::test_helpers::factories::WriteJobFactory;

#[test]
fn builds_single_write_job_with_expected_fields() {
    let f = WriteJobFactory::new()
        .with_key("login", "device")
        .with_zone_id(42)
        .with_path("/tmp/test.col");

    let job = f.create_with_value("laptop");

    assert_eq!(job.key.0, "login");
    assert_eq!(job.key.1, "device");
    assert_eq!(job.zone_id, 42);
    assert_eq!(job.value, "laptop");
    assert!(job.path.ends_with("test.col"));
}

#[test]
fn builds_many_jobs_from_values() {
    let f = WriteJobFactory::new()
        .with_key("signup", "ip")
        .with_zone_id(7);

    let jobs = f.create_many_with_values(&["1.1.1.1", "2.2.2.2"]);
    assert_eq!(jobs.len(), 2);
    assert_eq!(jobs[0].key.0, "signup");
    assert_eq!(jobs[0].key.1, "ip");
    assert_eq!(jobs[0].zone_id, 7);
    assert_eq!(jobs[0].value, "1.1.1.1");
    assert_eq!(jobs[1].value, "2.2.2.2");
}
