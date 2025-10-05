use crate::test_helpers::factories::GroupKeyFactory;

#[test]
fn builds_group_key_with_bucket_and_groups() {
    let gk = GroupKeyFactory::new()
        .with_bucket(1725148800)
        .with_groups(&["US", "CA"])
        .create();
    assert_eq!(gk.bucket, Some(1725148800));
    assert_eq!(gk.groups, vec!["US", "CA"]);

    let gk2 = GroupKeyFactory::us_in_month(1725148800);
    assert_eq!(gk2.bucket, Some(1725148800));
    assert_eq!(gk2.groups, vec!["US"]);
}
