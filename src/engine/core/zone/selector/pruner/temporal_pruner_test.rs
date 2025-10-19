use super::PruneArgs;
use super::temporal_pruner::TemporalPruner;
use crate::command::types::CompareOp;
use crate::engine::core::CandidateZone;
use crate::engine::core::time::{CalendarDir, ZoneTemporalIndex};
use crate::engine::core::zone::zone_artifacts::ZoneArtifacts;
use crate::engine::core::zone::zone_meta::ZoneMeta;
use serde_json::json;
use std::fs;
use std::path::PathBuf;

fn write_calendar(dir: &PathBuf, uid: &str, entries: &[(u32, u64, u64)]) {
    let mut cal = CalendarDir::new();
    for (z, lo, hi) in entries {
        cal.add_zone_range(*z, *lo, *hi);
    }
    cal.save(uid, dir).unwrap();
}

fn write_temporal(dir: &PathBuf, uid: &str, zone_id: u32, ts: &[i64]) {
    let zti = ZoneTemporalIndex::from_timestamps(ts.to_vec(), 1, 8);
    zti.save(uid, zone_id, dir).unwrap();
}

fn artifacts_for(base_dir: &PathBuf) -> ZoneArtifacts {
    ZoneArtifacts {
        base_dir,
        caches: None,
    }
}

#[test]
fn temporal_pruner_eq_hits_only_exact() {
    let tmp = tempfile::tempdir().unwrap();
    let seg_dir = tmp.path().join("00001");
    fs::create_dir_all(&seg_dir).unwrap();
    let uid = "u";

    // Calendar: zone 1 covers the day; ZTI: timestamps [100..105]
    write_calendar(&seg_dir, uid, &[(1, 100, 200)]);
    write_temporal(&seg_dir, uid, 1, &[100, 101, 104, 105]);

    let artifacts = artifacts_for(&tmp.path().to_path_buf());
    let pruner = TemporalPruner { artifacts };

    let val = json!(104u64);
    let args = PruneArgs {
        segment_id: "00001",
        uid,
        column: "timestamp",
        value: Some(&val),
        op: Some(&CompareOp::Eq),
    };
    let out = pruner.apply(&args).unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].zone_id, 1);

    // Non-existent ts should yield empty vector (not None)
    let val = json!(103u64);
    let args = PruneArgs {
        segment_id: "00001",
        uid,
        column: "timestamp",
        value: Some(&val),
        op: Some(&CompareOp::Eq),
    };
    let out = pruner.apply(&args).unwrap();
    assert!(out.is_empty());
}

#[test]
fn temporal_pruner_range_overlaps() {
    let tmp = tempfile::tempdir().unwrap();
    let seg_dir = tmp.path().join("00002");
    fs::create_dir_all(&seg_dir).unwrap();
    let uid = "u";

    // Calendar: two zones same day; z1: [100..150], z2: [200..250]
    write_calendar(&seg_dir, uid, &[(1, 100, 150), (2, 200, 250)]);
    write_temporal(&seg_dir, uid, 1, &[100, 120, 150]);
    write_temporal(&seg_dir, uid, 2, &[200, 225, 250]);

    let artifacts = artifacts_for(&tmp.path().to_path_buf());
    let pruner = TemporalPruner { artifacts };

    // ts=140: Gt and Gte should include z1; Lt/Lte should include z1; Eq yields empty
    let val = json!(140u64);
    for op in [CompareOp::Gt, CompareOp::Gte, CompareOp::Lt, CompareOp::Lte] {
        let args = PruneArgs {
            segment_id: "00002",
            uid,
            column: "timestamp",
            value: Some(&val),
            op: Some(&op),
        };
        let out = pruner.apply(&args).unwrap();
        assert!(out.iter().any(|z| z.zone_id == 1));
    }
    // Eq
    let args = PruneArgs {
        segment_id: "00002",
        uid,
        column: "timestamp",
        value: Some(&val),
        op: Some(&CompareOp::Eq),
    };
    let out = pruner.apply(&args).unwrap();
    assert!(out.is_empty());

    // ts=225: should target z2 for ranges
    let val = json!(225u64);
    for op in [CompareOp::Gt, CompareOp::Gte, CompareOp::Lt, CompareOp::Lte] {
        let args = PruneArgs {
            segment_id: "00002",
            uid,
            column: "timestamp",
            value: Some(&val),
            op: Some(&op),
        };
        let out = pruner.apply(&args).unwrap();
        assert!(out.iter().any(|z| z.zone_id == 2));
    }
}

#[test]
fn temporal_pruner_non_time_column_noop() {
    let tmp = tempfile::tempdir().unwrap();
    let seg_dir = tmp.path().join("00003");
    fs::create_dir_all(&seg_dir).unwrap();
    let uid = "u";
    write_calendar(&seg_dir, uid, &[(1, 10, 20)]);
    write_temporal(&seg_dir, uid, 1, &[10, 15, 20]);

    let artifacts = artifacts_for(&tmp.path().to_path_buf());
    let pruner = TemporalPruner { artifacts };

    let val = json!(10u64);
    let args = PruneArgs {
        segment_id: "00003",
        uid,
        column: "amount",
        value: Some(&val),
        op: Some(&CompareOp::Eq),
    };
    let out = pruner.apply(&args);
    assert!(out.is_none());
}

#[test]
fn temporal_pruner_missing_value_or_op_returns_none() {
    let tmp = tempfile::tempdir().unwrap();
    let seg_dir = tmp.path().join("00004");
    fs::create_dir_all(&seg_dir).unwrap();
    let uid = "u";
    write_calendar(&seg_dir, uid, &[(1, 1, 2)]);
    write_temporal(&seg_dir, uid, 1, &[1, 2]);

    let artifacts = artifacts_for(&tmp.path().to_path_buf());
    let pruner = TemporalPruner { artifacts };
    let val = json!(1u64);

    // missing value
    let args = PruneArgs {
        segment_id: "00004",
        uid,
        column: "timestamp",
        value: None,
        op: Some(&CompareOp::Eq),
    };
    assert!(pruner.apply(&args).is_none());
    // missing op
    let args = PruneArgs {
        segment_id: "00004",
        uid,
        column: "timestamp",
        value: Some(&val),
        op: None,
    };
    assert!(pruner.apply(&args).is_none());
}
