use std::path::PathBuf;

use super::segment_id::{LEVEL_SPAN, SEGMENT_ID_PAD, SegmentId};

#[test]
fn level_is_derived_by_range() {
    let s0 = SegmentId::new(0);
    let s_last_l0 = SegmentId::new(LEVEL_SPAN - 1);
    let s_l1_start = SegmentId::new(LEVEL_SPAN);
    let s_l2_example = SegmentId::new(LEVEL_SPAN * 2 + 123);

    assert_eq!(s0.level(), 0);
    assert_eq!(s_last_l0.level(), 0);
    assert_eq!(s_l1_start.level(), 1);
    assert_eq!(s_l2_example.level(), 2);
}

#[test]
fn dir_name_is_zero_padded() {
    let s = SegmentId::new(42);
    let name = s.dir_name();
    assert_eq!(name.len(), SEGMENT_ID_PAD);
    assert_eq!(name, format!("{:0width$}", 42, width = SEGMENT_ID_PAD));
}

#[test]
fn join_dir_appends_dir_name() {
    let s = SegmentId::new(7);
    let base = PathBuf::from("/tmp/shard-1");
    let joined = s.join_dir(&base);
    assert_eq!(joined, base.join(s.dir_name()));
}

#[test]
fn from_str_parses_zero_padded_numeric() {
    assert_eq!(SegmentId::from_str("00000"), Some(SegmentId::new(0)));
    assert_eq!(SegmentId::from_str("00123"), Some(SegmentId::new(123)));
    assert_eq!(SegmentId::from_str("abc"), None);
    assert_eq!(SegmentId::from_str("segment-001"), None);
}

#[test]
fn display_formats_like_dir_name() {
    let s = SegmentId::new(9);
    let disp = s.to_string();
    assert_eq!(disp, s.dir_name());
}

#[test]
fn segment_id_ordering_is_by_numeric_value() {
    let mut v = vec![SegmentId::new(10000), SegmentId::new(3), SegmentId::new(42)];
    v.sort();
    assert_eq!(
        v,
        vec![SegmentId::new(3), SegmentId::new(42), SegmentId::new(10000)]
    );
}
