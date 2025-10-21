use super::PruneArgs;
use super::temporal_pruner::TemporalPruner;
use crate::command::types::CompareOp;
use crate::engine::core::time::{
    temporal_calendar_index::TemporalCalendarIndex, zone_temporal_index::ZoneTemporalIndex,
};
use crate::engine::core::zone::zone_artifacts::ZoneArtifacts;
use serde_json::json;
use std::fs;
use std::path::PathBuf;

fn write_field_calendar(dir: &PathBuf, uid: &str, field: &str, entries: &[(u32, u64, u64)]) {
    let mut cal = TemporalCalendarIndex::new(field);
    for (z, lo, hi) in entries {
        cal.add_zone_range(*z, *lo, *hi);
    }
    cal.save(uid, dir).unwrap();
}

fn write_field_temporal_slab(dir: &PathBuf, uid: &str, field: &str, zones: &[(u32, &[i64])]) {
    let mut built: Vec<(u32, ZoneTemporalIndex)> = Vec::with_capacity(zones.len());
    for (zid, ts) in zones.iter() {
        let zti = ZoneTemporalIndex::from_timestamps(ts.to_vec(), 1, 8);
        built.push((*zid, zti));
    }
    let entries: Vec<(u32, &ZoneTemporalIndex)> = built.iter().map(|(z, zti)| (*z, zti)).collect();
    ZoneTemporalIndex::save_field_slab(uid, field, dir, &entries).unwrap();
}

fn artifacts_for(base_dir: &PathBuf) -> ZoneArtifacts<'_> {
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

    write_field_calendar(&seg_dir, uid, "timestamp", &[(1, 100, 200)]);
    write_field_temporal_slab(&seg_dir, uid, "timestamp", &[(1, &[100, 101, 104, 105])]);

    let base_dir = tmp.path().to_path_buf();
    let artifacts = artifacts_for(&base_dir);
    let pruner = TemporalPruner { artifacts };

    let val = json!(104u64);
    let args = PruneArgs {
        segment_id: "00001",
        uid,
        column: "timestamp",
        value: Some(&val),
        op: Some(&CompareOp::Eq),
    };
    let out = pruner.apply_temporal_only(&args).unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].zone_id, 1);

    let val = json!(103u64);
    let args = PruneArgs {
        segment_id: "00001",
        uid,
        column: "timestamp",
        value: Some(&val),
        op: Some(&CompareOp::Eq),
    };
    let out = pruner.apply_temporal_only(&args).unwrap();
    assert!(out.is_empty());
}

#[test]
fn temporal_pruner_range_overlaps() {
    let tmp = tempfile::tempdir().unwrap();
    let seg_dir = tmp.path().join("00002");
    fs::create_dir_all(&seg_dir).unwrap();
    let uid = "u";

    write_field_calendar(&seg_dir, uid, "timestamp", &[(1, 100, 150), (2, 200, 250)]);
    write_field_temporal_slab(
        &seg_dir,
        uid,
        "timestamp",
        &[(1, &[100, 120, 150]), (2, &[200, 225, 250])],
    );

    let base_dir = tmp.path().to_path_buf();
    let artifacts = artifacts_for(&base_dir);
    let pruner = TemporalPruner { artifacts };

    let val = json!(140u64);
    for op in [CompareOp::Gt, CompareOp::Gte, CompareOp::Lt, CompareOp::Lte] {
        let args = PruneArgs {
            segment_id: "00002",
            uid,
            column: "timestamp",
            value: Some(&val),
            op: Some(&op),
        };
        let out = pruner.apply_temporal_only(&args).unwrap();
        assert!(out.iter().any(|z| z.zone_id == 1));
    }
    let args = PruneArgs {
        segment_id: "00002",
        uid,
        column: "timestamp",
        value: Some(&val),
        op: Some(&CompareOp::Eq),
    };
    let out = pruner.apply_temporal_only(&args).unwrap();
    assert!(out.is_empty());

    let val = json!(225u64);
    for op in [CompareOp::Gt, CompareOp::Gte, CompareOp::Lt, CompareOp::Lte] {
        let args = PruneArgs {
            segment_id: "00002",
            uid,
            column: "timestamp",
            value: Some(&val),
            op: Some(&op),
        };
        let out = pruner.apply_temporal_only(&args).unwrap();
        assert!(out.iter().any(|z| z.zone_id == 2));
    }
}

#[test]
fn temporal_pruner_non_time_column_noop() {
    let tmp = tempfile::tempdir().unwrap();
    let seg_dir = tmp.path().join("00003");
    fs::create_dir_all(&seg_dir).unwrap();
    let uid = "u";
    write_field_calendar(&seg_dir, uid, "timestamp", &[(1, 10, 20)]);
    write_field_temporal_slab(&seg_dir, uid, "timestamp", &[(1, &[10, 15, 20])]);

    let base_dir = tmp.path().to_path_buf();
    let artifacts = artifacts_for(&base_dir);
    let pruner = TemporalPruner { artifacts };

    let val = json!(10u64);
    let args = PruneArgs {
        segment_id: "00003",
        uid,
        column: "amount",
        value: Some(&val),
        op: Some(&CompareOp::Eq),
    };
    let out = pruner.apply_temporal_only(&args);
    assert!(out.is_none());
}

#[test]
fn temporal_pruner_missing_value_or_op_returns_none() {
    let tmp = tempfile::tempdir().unwrap();
    let seg_dir = tmp.path().join("00004");
    fs::create_dir_all(&seg_dir).unwrap();
    let uid = "u";
    write_field_calendar(&seg_dir, uid, "timestamp", &[(1, 1, 2)]);
    write_field_temporal_slab(&seg_dir, uid, "timestamp", &[(1, &[1, 2])]);

    let base_dir = tmp.path().to_path_buf();
    let artifacts = artifacts_for(&base_dir);
    let pruner = TemporalPruner { artifacts };
    let val = json!(1u64);

    let args = PruneArgs {
        segment_id: "00004",
        uid,
        column: "timestamp",
        value: None,
        op: Some(&CompareOp::Eq),
    };
    assert!(pruner.apply_temporal_only(&args).is_none());
    let args = PruneArgs {
        segment_id: "00004",
        uid,
        column: "timestamp",
        value: Some(&val),
        op: None,
    };
    assert!(pruner.apply_temporal_only(&args).is_none());
}

#[test]
fn temporal_pruner_works_for_non_timestamp_temporal_field() {
    let tmp = tempfile::tempdir().unwrap();
    let seg_dir = tmp.path().join("00005");
    fs::create_dir_all(&seg_dir).unwrap();
    let uid = "u";

    // Per-field artifacts for a non-timestamp temporal field "created_at"
    write_field_calendar(&seg_dir, uid, "created_at", &[(7, 1_000, 2_000)]);
    write_field_temporal_slab(&seg_dir, uid, "created_at", &[(7, &[1_234, 1_500, 1_999])]);

    let base_dir = tmp.path().to_path_buf();
    let artifacts = artifacts_for(&base_dir);
    let pruner = TemporalPruner { artifacts };

    // Eq hits exact
    let val = json!(1_500u64);
    let args = PruneArgs {
        segment_id: "00005",
        uid,
        column: "created_at",
        value: Some(&val),
        op: Some(&CompareOp::Eq),
    };
    let out = pruner.apply_temporal_only(&args).unwrap();
    assert_eq!(out.iter().map(|z| z.zone_id).collect::<Vec<u32>>(), vec![7]);

    // Gt should include the zone due to max_ts > 1499
    let val = json!(1_499u64);
    let args = PruneArgs {
        segment_id: "00005",
        uid,
        column: "created_at",
        value: Some(&val),
        op: Some(&CompareOp::Gt),
    };
    let out = pruner.apply_temporal_only(&args).unwrap();
    assert!(out.iter().any(|z| z.zone_id == 7));
}

#[test]
fn temporal_pruner_handles_string_datetime_values() {
    let tmp = tempfile::tempdir().unwrap();
    let seg_dir = tmp.path().join("00006");
    fs::create_dir_all(&seg_dir).unwrap();
    let uid = "u";

    write_field_calendar(
        &seg_dir,
        uid,
        "timestamp",
        &[(3, 1_600_000_000, 1_700_000_000)],
    );
    write_field_temporal_slab(&seg_dir, uid, "timestamp", &[(3, &[1_650_000_000])]);

    let base_dir = tmp.path().to_path_buf();
    let artifacts = artifacts_for(&base_dir);
    let pruner = TemporalPruner { artifacts };

    // Use ISO-like string parsed as DateTime
    let val = json!("2020-09-15T00:00:00Z");
    let args = PruneArgs {
        segment_id: "00006",
        uid,
        column: "timestamp",
        value: Some(&val),
        op: Some(&CompareOp::Gte),
    };
    let out = pruner.apply_temporal_only(&args).unwrap();
    assert!(out.iter().any(|z| z.zone_id == 3));
}
