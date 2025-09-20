#![cfg(test)]

use crate::engine::core::filter::surf_encoding::encode_value;
use crate::engine::core::filter::zone_surf_filter::ZoneSurfFilter;
use crate::test_helpers::factory::Factory;
use serde_json::json;
use tempfile::tempdir;

fn zone_with_events(
    uid: &str,
    zone_id: u32,
    ids: &[i64],
) -> crate::engine::core::zone::zone_plan::ZonePlan {
    let events: Vec<_> = ids
        .iter()
        .map(|v| Factory::event().with("payload", json!({"id": v})).create())
        .collect();
    Factory::zone_plan()
        .with("id", zone_id)
        .with("uid", uid)
        .with("segment_id", 42u64)
        .with("events", json!(events))
        .with("start_index", 0usize)
        .with("end_index", (ids.len().saturating_sub(1)) as usize)
        .create()
}

#[test]
fn zone_surf_numeric_selection() {
    // Two zones: z0 has ids < 10; z1 has ids >= 10
    let z0 = zone_with_events("uid123", 0, &[1, 5, 8]);
    let z1 = zone_with_events("uid123", 1, &[15, 20]);

    let dir = tempdir().unwrap();
    ZoneSurfFilter::build_all(&[z0.clone(), z1.clone()], dir.path()).unwrap();

    let path = dir.path().join("uid123_id.zsrf");
    assert!(path.exists());
    let zsf = ZoneSurfFilter::load(&path).unwrap();

    let b10 = encode_value(&json!(10)).unwrap();
    let seg = "seg1";

    // > 10 selects z1 only
    let ge = zsf
        .zones_overlapping_ge(&b10, false, seg)
        .into_iter()
        .map(|z| z.zone_id)
        .collect::<Vec<_>>();
    assert_eq!(ge, vec![1u32]);

    // <= 9 selects z0 only
    let b9 = encode_value(&json!(9)).unwrap();
    let le = zsf
        .zones_overlapping_le(&b9, true, seg)
        .into_iter()
        .map(|z| z.zone_id)
        .collect::<Vec<_>>();
    assert_eq!(le, vec![0u32]);
}

#[test]
fn zone_surf_boundary_inclusive_exclusive() {
    // z0 has exact boundary 20; z1 has values > 20
    let z0 = zone_with_events("uidB", 0, &[10, 20]);
    let z1 = zone_with_events("uidB", 1, &[30]);

    let dir = tempdir().unwrap();
    ZoneSurfFilter::build_all(&[z0, z1], dir.path()).unwrap();

    let path = dir.path().join("uidB_id.zsrf");
    let zsf = ZoneSurfFilter::load(&path).unwrap();
    let seg = "segB";

    let b20 = encode_value(&json!(20)).unwrap();

    // >= 20 (inclusive) selects z0 and z1
    let ge_inc = zsf
        .zones_overlapping_ge(&b20, true, seg)
        .into_iter()
        .map(|z| z.zone_id)
        .collect::<Vec<_>>();
    assert_eq!(ge_inc, vec![0u32, 1u32]);

    // > 20 (exclusive) selects z1 only
    let ge_exc = zsf
        .zones_overlapping_ge(&b20, false, seg)
        .into_iter()
        .map(|z| z.zone_id)
        .collect::<Vec<_>>();
    assert_eq!(ge_exc, vec![1u32]);

    // <= 20 (inclusive) selects z0
    let le_inc = zsf
        .zones_overlapping_le(&b20, true, seg)
        .into_iter()
        .map(|z| z.zone_id)
        .collect::<Vec<_>>();
    assert_eq!(le_inc, vec![0u32]);

    // < 20 (exclusive) selects z0 (because 10 < 20)
    let le_exc = zsf
        .zones_overlapping_le(&b20, false, seg)
        .into_iter()
        .map(|z| z.zone_id)
        .collect::<Vec<_>>();
    assert_eq!(le_exc, vec![0u32]);
}

#[test]
fn zone_surf_float_ranges() {
    // z0 floats are negative; z1 floats are positive
    let z0_events = vec![
        Factory::event()
            .with("payload", json!({"scoreID": -3.5}))
            .create(),
        Factory::event()
            .with("payload", json!({"scoreID": -1.25}))
            .create(),
    ];
    let z1_events = vec![
        Factory::event()
            .with("payload", json!({"scoreID": 2.25}))
            .create(),
        Factory::event()
            .with("payload", json!({"scoreID": 10.5}))
            .create(),
    ];

    let z0 = Factory::zone_plan()
        .with("id", 0u32)
        .with("uid", "uidF")
        .with("segment_id", 9u64)
        .with("events", json!(z0_events))
        .with("start_index", 0usize)
        .with("end_index", 1usize)
        .create();
    let z1 = Factory::zone_plan()
        .with("id", 1u32)
        .with("uid", "uidF")
        .with("segment_id", 9u64)
        .with("events", json!(z1_events))
        .with("start_index", 0usize)
        .with("end_index", 1usize)
        .create();

    let dir = tempdir().unwrap();
    ZoneSurfFilter::build_all(&[z0, z1], dir.path()).unwrap();
    let path = dir.path().join("uidF_scoreID.zsrf");
    let zsf = ZoneSurfFilter::load(&path).unwrap();
    let seg = "segF";

    // > -1.0 selects z1 only
    let b = encode_value(&json!(-1.0)).unwrap();
    let ge = zsf
        .zones_overlapping_ge(&b, false, seg)
        .into_iter()
        .map(|z| z.zone_id)
        .collect::<Vec<_>>();
    assert_eq!(ge, vec![1u32]);

    // <= -2.0 selects z0 only
    let b2 = encode_value(&json!(-2.0)).unwrap();
    let le = zsf
        .zones_overlapping_le(&b2, true, seg)
        .into_iter()
        .map(|z| z.zone_id)
        .collect::<Vec<_>>();
    assert_eq!(le, vec![0u32]);
}

#[test]
fn zone_surf_bool_ranges() {
    // z0 has flag=false, z1 has flag=true
    let z0_events = vec![
        Factory::event()
            .with("payload", json!({"flag_id": false}))
            .create(),
        Factory::event()
            .with("payload", json!({"flag_id": false}))
            .create(),
    ];
    let z1_events = vec![
        Factory::event()
            .with("payload", json!({"flag_id": true}))
            .create(),
        Factory::event()
            .with("payload", json!({"flag_id": true}))
            .create(),
    ];

    let z0 = Factory::zone_plan()
        .with("id", 0u32)
        .with("uid", "uidBool")
        .with("segment_id", 11u64)
        .with("events", json!(z0_events))
        .with("start_index", 0usize)
        .with("end_index", 1usize)
        .create();
    let z1 = Factory::zone_plan()
        .with("id", 1u32)
        .with("uid", "uidBool")
        .with("segment_id", 11u64)
        .with("events", json!(z1_events))
        .with("start_index", 0usize)
        .with("end_index", 1usize)
        .create();

    let dir = tempdir().unwrap();
    ZoneSurfFilter::build_all(&[z0, z1], dir.path()).unwrap();
    let path = dir.path().join("uidBool_flag_id.zsrf");
    let zsf = ZoneSurfFilter::load(&path).unwrap();
    let seg = "segBool";

    // > false (exclusive) should pick true-zone only
    let b_false = encode_value(&json!(false)).unwrap();
    let ge = zsf
        .zones_overlapping_ge(&b_false, false, seg)
        .into_iter()
        .map(|z| z.zone_id)
        .collect::<Vec<_>>();
    assert_eq!(ge, vec![1u32]);

    // <= false (inclusive) should pick false-zone only
    let le = zsf
        .zones_overlapping_le(&b_false, true, seg)
        .into_iter()
        .map(|z| z.zone_id)
        .collect::<Vec<_>>();
    assert_eq!(le, vec![0u32]);
}

#[test]
fn zone_surf_gt_boundary_byte_carry() {
    // Craft values that cross 0xFF carry boundaries when encoded in big-endian
    // Zone 0 contains 255 and 256; >255 must still select the zone
    let z0 = zone_with_events("uidCarry", 0, &[255, 256]);

    // Also include a larger boundary to catch multi-byte carries: 65535 -> 65536
    let z1 = zone_with_events("uidCarry", 1, &[65535, 65536]);

    let dir = tempdir().unwrap();
    ZoneSurfFilter::build_all(&[z0, z1], dir.path()).unwrap();
    let path = dir.path().join("uidCarry_id.zsrf");
    let zsf = ZoneSurfFilter::load(&path).unwrap();

    let seg = "segC";
    // > 255 should include zone 0
    let b255 = encode_value(&json!(255)).unwrap();
    let zones = zsf
        .zones_overlapping_ge(&b255, false, seg)
        .into_iter()
        .map(|z| z.zone_id)
        .collect::<Vec<_>>();
    assert!(zones.contains(&0u32));

    // > 65535 should include zone 1
    let b65535 = encode_value(&json!(65535)).unwrap();
    let zones2 = zsf
        .zones_overlapping_ge(&b65535, false, seg)
        .into_iter()
        .map(|z| z.zone_id)
        .collect::<Vec<_>>();
    assert!(zones2.contains(&1u32));
}

#[test]
fn zone_surf_gt_prefix_terminal_has_greater_descendants() {
    // Zone has exact boundary value plus strictly larger values
    // Lower bound equals a terminal key but there are greater keys -> zone must be selected for GT
    let z0 = zone_with_events("uidPref", 0, &[10, 1000, 1001]);

    let dir = tempdir().unwrap();
    ZoneSurfFilter::build_all(&[z0], dir.path()).unwrap();
    let path = dir.path().join("uidPref_id.zsrf");
    let zsf = ZoneSurfFilter::load(&path).unwrap();

    let seg = "segP";
    let b10 = encode_value(&json!(10)).unwrap();
    let zones = zsf
        .zones_overlapping_ge(&b10, false, seg)
        .into_iter()
        .map(|z| z.zone_id)
        .collect::<Vec<_>>();
    assert_eq!(zones, vec![0u32]);
}

#[test]
fn zone_surf_gt_many_bounds_monotonic() {
    // Single zone with a spread of values across byte boundaries
    let values = [
        0i64, 1, 2, 255, 256, 257, 1023, 1024, 4095, 4096, 65535, 65536, 1_000_000,
    ];
    let z0 = zone_with_events("uidMono", 0, &values);

    let dir = tempdir().unwrap();
    ZoneSurfFilter::build_all(&[z0], dir.path()).unwrap();
    let path = dir.path().join("uidMono_id.zsrf");
    let zsf = ZoneSurfFilter::load(&path).unwrap();
    let seg = "segM";

    // For every bound b strictly less than max, > b must select the zone
    for w in 0..values.len() - 1 {
        let b = encode_value(&json!(values[w])).unwrap();
        let zones = zsf
            .zones_overlapping_ge(&b, false, seg)
            .into_iter()
            .map(|z| z.zone_id)
            .collect::<Vec<_>>();
        assert_eq!(zones, vec![0u32], "> {} should hit zone", values[w]);
    }
}
