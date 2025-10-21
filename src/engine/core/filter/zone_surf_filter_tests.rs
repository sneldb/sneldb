#![cfg(test)]

use crate::engine::core::filter::surf_encoding::encode_value;
use crate::engine::core::filter::zone_surf_filter::ZoneSurfFilter;
use crate::test_helpers::factory::Factory;
use serde_json::json;
use std::collections::HashSet;
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
    let mut allowed: HashSet<String> = HashSet::new();
    allowed.insert("id".to_string());
    ZoneSurfFilter::build_all_filtered(&[z0.clone(), z1.clone()], dir.path(), &allowed).unwrap();

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
    let mut allowed: HashSet<String> = HashSet::new();
    allowed.insert("id".to_string());
    ZoneSurfFilter::build_all_filtered(&[z0, z1], dir.path(), &allowed).unwrap();

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
    let mut allowed: HashSet<String> = HashSet::new();
    allowed.insert("scoreID".to_string());
    ZoneSurfFilter::build_all_filtered(&[z0, z1], dir.path(), &allowed).unwrap();
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
fn zone_surf_flag_numeric_ranges() {
    // Use numeric 0/1 flags (bools are no longer included by default)
    let z0_events = vec![
        Factory::event()
            .with("payload", json!({"flag_id": 0}))
            .create(),
        Factory::event()
            .with("payload", json!({"flag_id": 0}))
            .create(),
    ];
    let z1_events = vec![
        Factory::event()
            .with("payload", json!({"flag_id": 1}))
            .create(),
        Factory::event()
            .with("payload", json!({"flag_id": 1}))
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
    let mut allowed: HashSet<String> = HashSet::new();
    allowed.insert("flag_id".to_string());
    ZoneSurfFilter::build_all_filtered(&[z0, z1], dir.path(), &allowed).unwrap();
    let path = dir.path().join("uidBool_flag_id.zsrf");
    let zsf = ZoneSurfFilter::load(&path).unwrap();
    let seg = "segBool";

    // > 0 selects zone with 1 only
    let b0 = encode_value(&json!(0)).unwrap();
    let ge = zsf
        .zones_overlapping_ge(&b0, false, seg)
        .into_iter()
        .map(|z| z.zone_id)
        .collect::<Vec<_>>();
    assert_eq!(ge, vec![1u32]);

    // <= 0 selects zone with 0 only
    let le = zsf
        .zones_overlapping_le(&b0, true, seg)
        .into_iter()
        .map(|z| z.zone_id)
        .collect::<Vec<_>>();
    assert_eq!(le, vec![0u32]);
}

#[test]
fn zone_surf_schema_time_fields_skipped() {
    // Two zones with numeric epoch seconds in payload
    let z0_events = vec![
        Factory::event()
            .with("payload", json!({"ts": 100, "d": 50}))
            .create(),
        Factory::event()
            .with("payload", json!({"ts": 200, "d": 50}))
            .create(),
    ];
    let z1_events = vec![
        Factory::event()
            .with("payload", json!({"ts": 300, "d": 150}))
            .create(),
    ];
    let z0 = Factory::zone_plan()
        .with("id", 0u32)
        .with("uid", "uidTS")
        .with("event_type", "evt")
        .with("segment_id", 1u64)
        .with("events", json!(z0_events))
        .with("start_index", 0usize)
        .with("end_index", 1usize)
        .create();
    let z1 = Factory::zone_plan()
        .with("id", 1u32)
        .with("uid", "uidTS")
        .with("event_type", "evt")
        .with("segment_id", 1u64)
        .with("events", json!(z1_events))
        .with("start_index", 0usize)
        .with("end_index", 0usize)
        .create();

    let dir = tempdir().unwrap();
    let mut allowed: HashSet<String> = HashSet::new();
    allowed.insert("a".to_string());
    ZoneSurfFilter::build_all_filtered(&[z0, z1], dir.path(), &allowed).unwrap();

    // Time fields are skipped by SuRF builder
    let ts_path = dir.path().join("uidTS_ts.zsrf");
    assert!(std::fs::metadata(&ts_path).is_err());
    let d_path = dir.path().join("uidTS_d.zsrf");
    assert!(std::fs::metadata(&d_path).is_err());
}

#[test]
fn zone_surf_skips_fixed_timestamp_field() {
    // Two zones with timestamp values; SuRF should not be built for fixed 'timestamp'
    let z0_events = vec![
        Factory::event()
            .with("timestamp", json!(100))
            .with("payload", json!({"id": 1}))
            .create(),
        Factory::event()
            .with("timestamp", json!(110))
            .with("payload", json!({"id": 2}))
            .create(),
    ];
    let z1_events = vec![
        Factory::event()
            .with("timestamp", json!(200))
            .with("payload", json!({"id": 3}))
            .create(),
    ];

    let z0 = Factory::zone_plan()
        .with("id", 0u32)
        .with("uid", "uidT")
        .with("segment_id", 2u64)
        .with("events", json!(z0_events))
        .with("start_index", 0usize)
        .with("end_index", 1usize)
        .create();
    let z1 = Factory::zone_plan()
        .with("id", 1u32)
        .with("uid", "uidT")
        .with("segment_id", 2u64)
        .with("events", json!(z1_events))
        .with("start_index", 0usize)
        .with("end_index", 0usize)
        .create();

    let dir = tempdir().unwrap();
    let mut allowed: HashSet<String> = HashSet::new();
    allowed.insert("id".to_string());
    ZoneSurfFilter::build_all_filtered(&[z0, z1], dir.path(), &allowed).unwrap();

    let ts_path = dir.path().join("uidT_timestamp.zsrf");
    assert!(std::fs::metadata(&ts_path).is_err());
}

#[test]
fn zone_surf_skips_mixed_numeric_field() {
    // z0 has numeric, z1 has string for the same field -> should skip
    let z0 = Factory::zone_plan()
        .with("id", 0u32)
        .with("uid", "uidMix")
        .with("segment_id", 1u64)
        .with(
            "events",
            json!([Factory::event()
                .with("payload", json!({"mix": 10}))
                .create()]),
        )
        .with("start_index", 0usize)
        .with("end_index", 0usize)
        .create();
    let z1 = Factory::zone_plan()
        .with("id", 1u32)
        .with("uid", "uidMix")
        .with("segment_id", 1u64)
        .with(
            "events",
            json!([Factory::event()
                .with("payload", json!({"mix": "x"}))
                .create()]),
        )
        .with("start_index", 0usize)
        .with("end_index", 0usize)
        .create();

    let dir = tempdir().unwrap();
    let mut allowed: HashSet<String> = HashSet::new();
    allowed.insert("id".to_string());
    ZoneSurfFilter::build_all_filtered(&[z0, z1], dir.path(), &allowed).unwrap();
    let mix_path = dir.path().join("uidMix_mix.zsrf");
    assert!(
        std::fs::metadata(&mix_path).is_err(),
        "mixed field should not have SuRF"
    );
}

#[test]
fn zone_surf_gt_boundary_byte_carry() {
    // Craft values that cross 0xFF carry boundaries when encoded in big-endian
    // Zone 0 contains 255 and 256; >255 must still select the zone
    let z0 = zone_with_events("uidCarry", 0, &[255, 256]);

    // Also include a larger boundary to catch multi-byte carries: 65535 -> 65536
    let z1 = zone_with_events("uidCarry", 1, &[65535, 65536]);

    let dir = tempdir().unwrap();
    let mut allowed: HashSet<String> = HashSet::new();
    allowed.insert("id".to_string());
    ZoneSurfFilter::build_all_filtered(&[z0, z1], dir.path(), &allowed).unwrap();
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
    let mut allowed: HashSet<String> = HashSet::new();
    allowed.insert("id".to_string());
    ZoneSurfFilter::build_all_filtered(&[z0], dir.path(), &allowed).unwrap();
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
    let mut allowed: HashSet<String> = HashSet::new();
    allowed.insert("id".to_string());
    ZoneSurfFilter::build_all_filtered(&[z0], dir.path(), &allowed).unwrap();
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

#[test]
fn zonesurf_compressed_header_and_algo_lz4() {
    use crate::engine::core::column::compression::compression_codec::{ALGO_LZ4, FLAG_COMPRESSED};
    use crate::shared::storage_header::{BinaryHeader, FileKind};
    use std::fs::OpenOptions;
    use std::io::Read;

    // Save a minimal filter
    let dir = tempdir().unwrap();
    let path = dir.path().join("u_hdr_field.zsrf");
    let filter = ZoneSurfFilter { entries: vec![] };
    filter.save(&path).unwrap();

    // Read back header and algo id
    let mut f = OpenOptions::new().read(true).open(&path).unwrap();
    let hdr = BinaryHeader::read_from(&mut f).unwrap();
    assert_eq!(hdr.magic, FileKind::ZoneSurfFilter.magic());
    assert_ne!(
        hdr.flags & FLAG_COMPRESSED,
        0,
        "compressed flag must be set"
    );

    // Next 2 bytes should be algo id
    let mut algo_bytes = [0u8; 2];
    f.read_exact(&mut algo_bytes).unwrap();
    let algo = u16::from_le_bytes(algo_bytes);
    assert_eq!(algo, ALGO_LZ4, "expected LZ4 algo id");
}

#[test]
fn zonesurf_legacy_uncompressed_roundtrip_loads() {
    use crate::shared::storage_header::{BinaryHeader, FileKind};
    use std::fs::OpenOptions;
    use std::io::Write;

    // Build a trivial filter
    let filter = ZoneSurfFilter { entries: vec![] };
    let bytes = bincode::serialize(&filter).unwrap();

    let dir = tempdir().unwrap();
    let path = dir.path().join("legacy.zsrf");
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&path)
        .unwrap();

    // Write legacy header (no compression flag)
    let header = BinaryHeader::new(FileKind::ZoneSurfFilter.magic(), 1, 0);
    header.write_to(&mut f).unwrap();
    f.write_all(&bytes).unwrap();
    f.flush().unwrap();

    // Load via format-aware loader
    let loaded = ZoneSurfFilter::load(&path).unwrap();
    assert_eq!(loaded.entries.len(), 0);
}

#[test]
fn zonesurf_rejects_unsupported_algo() {
    use crate::engine::core::column::compression::compression_codec::FLAG_COMPRESSED;
    use crate::shared::storage_header::{BinaryHeader, FileKind};
    use std::fs::OpenOptions;
    use std::io::Write;

    let dir = tempdir().unwrap();
    let path = dir.path().join("bad_algo.zsrf");
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&path)
        .unwrap();

    // Write header with compressed flag set
    let header = BinaryHeader::new(FileKind::ZoneSurfFilter.magic(), 1, FLAG_COMPRESSED);
    header.write_to(&mut f).unwrap();
    // Write wrong algo id
    let bad_algo: u16 = 0x7FFF; // not ALGO_LZ4
    f.write_all(&bad_algo.to_le_bytes()).unwrap();
    // No payload needed; loader should fail on algo id check
    f.flush().unwrap();

    let err = ZoneSurfFilter::load(&path).unwrap_err();
    let msg = format!("{}", err);
    assert!(
        msg.contains("unsupported SuRF compression algo"),
        "msg was: {}",
        msg
    );
}

#[test]
fn zonesurf_truncated_compressed_file_errors() {
    use crate::engine::core::column::compression::compression_codec::FLAG_COMPRESSED;
    use crate::shared::storage_header::{BinaryHeader, FileKind};
    use std::fs::OpenOptions;

    let dir = tempdir().unwrap();
    let path = dir.path().join("truncated.zsrf");
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&path)
        .unwrap();

    // Write header with compressed flag but no algo bytes
    let header = BinaryHeader::new(FileKind::ZoneSurfFilter.magic(), 1, FLAG_COMPRESSED);
    header.write_to(&mut f).unwrap();
    drop(f);

    let err = ZoneSurfFilter::load(&path).unwrap_err();
    let msg = format!("{}", err);
    assert!(
        msg.contains("truncated compressed zonesurffilter"),
        "msg was: {}",
        msg
    );
}

#[test]
fn zonesurf_no_panic_le_on_exact_length_equal() {
    use crate::engine::core::filter::surf_trie::SurfTrie;
    use crate::engine::core::filter::zone_surf_filter::ZoneSurfEntry;

    // Build a trie with keys where one key equals the bound exactly
    let keys = vec![
        encode_value(&json!("a")).unwrap(),
        encode_value(&json!("ab")).unwrap(),
        encode_value(&json!("ac")).unwrap(),
    ];
    let trie = SurfTrie::build_from_sorted(&keys);
    let zsf = ZoneSurfFilter {
        entries: vec![ZoneSurfEntry { zone_id: 7, trie }],
    };

    // Upper bound equals "a"; exclusive (< a) should not panic and returns empty
    let upper = encode_value(&json!("a")).unwrap();
    let seg = "segNP";
    let out = zsf.zones_overlapping_le(&upper, false, seg);
    assert!(out.is_empty());

    // Inclusive (<= a) should include the zone (since key "a" exists)
    let out2 = zsf
        .zones_overlapping_le(&upper, true, seg)
        .into_iter()
        .map(|z| z.zone_id)
        .collect::<Vec<_>>();
    assert_eq!(out2, vec![7u32]);
}

#[test]
fn zonesurf_no_panic_ge_simd_tail_alignment() {
    use crate::engine::core::filter::surf_trie::SurfTrie;
    use crate::engine::core::filter::zone_surf_filter::ZoneSurfEntry;

    // Build >16 distinct single-byte keys to exercise SIMD chunk + tail path
    let mut keys: Vec<Vec<u8>> = (0u8..=20u8).map(|b| vec![b]).collect();
    // Ensure ascending and unique as required by builder
    keys.sort();
    keys.dedup();

    let trie = SurfTrie::build_from_sorted(&keys);
    let zsf = ZoneSurfFilter {
        entries: vec![ZoneSurfEntry { zone_id: 3, trie }],
    };

    // lower = 9 (just before a SIMD lane boundary), exclusive, should not panic
    let lower = vec![9u8];
    let seg = "segTail";
    let out = zsf
        .zones_overlapping_ge(&lower, false, seg)
        .into_iter()
        .map(|z| z.zone_id)
        .collect::<Vec<_>>();
    // We have keys up to 20, so should include the zone
    assert_eq!(out, vec![3u32]);
}
