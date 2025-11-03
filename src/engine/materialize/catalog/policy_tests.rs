use super::policy::RetentionPolicy;

#[test]
fn keep_all_is_noop() {
    let policy = RetentionPolicy::keep_all();
    assert!(policy.is_noop());
}

#[test]
fn policy_detects_limits() {
    let policy = RetentionPolicy {
        max_rows: Some(500),
        max_age_seconds: None,
    };
    assert!(!policy.is_noop());

    let policy = RetentionPolicy {
        max_rows: None,
        max_age_seconds: Some(3600),
    };
    assert!(!policy.is_noop());
}

