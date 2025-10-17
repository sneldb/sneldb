use crate::engine::core::ZoneWriter;
use crate::engine::core::column::column_loader::ColumnLoader;
use crate::engine::core::column::compression::{CompressionCodec, Lz4Codec};
use crate::engine::core::column::format::{ColumnBlockHeader, PhysicalType};
use crate::engine::core::zone::candidate_zone::CandidateZone;
use crate::shared::config::CONFIG;
use crate::shared::storage_header::BinaryHeader;
use crate::test_helpers::factories::column_factory::ColumnFactory;
use crate::test_helpers::factories::{
    CandidateZoneFactory, EventFactory, SchemaRegistryFactory, ZonePlannerFactory,
};
use serde_json::json;
use std::fs::OpenOptions;
use std::fs::create_dir_all;
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;
use tempfile::tempdir;

fn make_varbytes_block(values: &[&str]) -> Vec<u8> {
    let mut aux: Vec<u8> = Vec::with_capacity(values.len() * 4);
    let mut payload: Vec<u8> = Vec::new();
    for v in values {
        let b = v.as_bytes();
        aux.extend_from_slice(&(b.len() as u32).to_le_bytes());
        payload.extend_from_slice(b);
    }
    let hdr = ColumnBlockHeader::new(
        PhysicalType::VarBytes,
        false,
        values.len() as u32,
        aux.len() as u32,
    );
    let mut buf: Vec<u8> = Vec::with_capacity(ColumnBlockHeader::LEN + aux.len() + payload.len());
    hdr.write_to(&mut buf);
    buf.extend_from_slice(&aux);
    buf.extend_from_slice(&payload);
    buf
}

#[test]
fn column_loader_loads_columns_for_zone() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = "segment-00000";
    let uid = "uid_u";
    let field = "f";
    let seg_dir = base_dir.join(segment_id);
    std::fs::create_dir_all(&seg_dir).unwrap();

    // Build decompressed payload and compress
    let decomp = make_varbytes_block(&["a", "bb"]);
    let codec = Lz4Codec;
    let comp = CompressionCodec::compress(&codec, &decomp).expect("compress");
    let block_start = BinaryHeader::TOTAL_LEN as u64;
    let zone_id = 7u32;

    // Write minimal zfc and col files
    let _ = ColumnFactory::new()
        .with_segment_dir(&seg_dir)
        .with_uid(uid)
        .with_field(field)
        .with_zfc_entry(
            zone_id,
            block_start,
            comp.len() as u32,
            decomp.len() as u32,
            2,
            vec![],
        )
        .write_minimal();

    // Append compressed block at the specified offset
    let col_path = seg_dir.join(format!("{}_{}.col", uid, field));
    let mut f = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&col_path)
        .unwrap();
    f.seek(SeekFrom::Start(block_start)).unwrap();
    f.write_all(&comp).unwrap();
    f.flush().unwrap();

    // Build CandidateZone
    let zone = CandidateZone::new(zone_id, segment_id.to_string());

    // Load values
    let loader = ColumnLoader::new(base_dir.clone(), uid.to_string());
    let cols = loader.load_all_columns(&zone, &[field.to_string()]);
    let vals = cols.get(field).expect("values");
    assert_eq!(vals.len(), 2);
    assert_eq!(vals.get_str_at(0).unwrap(), "a");
    assert_eq!(vals.get_str_at(1).unwrap(), "bb");
}

#[test]
fn column_loader_returns_empty_on_missing_files() {
    let tmp = tempdir().unwrap();
    let base_dir: PathBuf = tmp.path().to_path_buf();
    let segment_id = "segment-missing";
    let uid = "uid_x";
    let field = "f";
    // do not create segment dir/files

    let zone = CandidateZone::new(1, segment_id.to_string());
    let loader = ColumnLoader::new(base_dir, uid.to_string());
    let cols = loader.load_all_columns(&zone, &[field.to_string()]);
    let vals = cols.get(field).expect("values entry present");
    assert_eq!(
        vals.len(),
        0,
        "missing files should yield empty ColumnValues"
    );
}

#[tokio::test]
async fn loads_column_values_for_zone() {
    crate::logging::init_for_tests();

    // Setup test segment dir and schema registry
    let dir = tempdir().unwrap();
    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();
    registry_factory
        .define_with_fields(
            "login",
            &[("ip", "string"), ("device", "string"), ("timestamp", "u64")],
        )
        .await
        .unwrap();

    // Create events and zone plans
    let event_1 = EventFactory::new()
        .with("event_type", "login")
        .with(
            "payload",
            json!({
                "ip": "127.0.0.1",
                "device": "mobile",
                "timestamp": 123456
            }),
        )
        .create();

    let event_2 = EventFactory::new()
        .with("event_type", "login")
        .with(
            "payload",
            json!({
                "ip": "127.0.0.1",
                "device": "web",
                "timestamp": 123456
            }),
        )
        .create();

    let events = vec![event_1, event_2];
    let uid = registry.read().await.get_uid("login").unwrap();

    let planner = ZonePlannerFactory::new(events, &uid).with_uid(&uid);
    let zones = planner.plan();

    let segment_dir = dir.path().join("00001");
    create_dir_all(&segment_dir).unwrap();

    // Write columns to disk
    let writer = ZoneWriter::new(&uid, &segment_dir, registry.clone());
    writer.write_all(&zones).await.unwrap();

    // Create a candidate zone for reading
    let candidate = CandidateZoneFactory::new()
        .with("zone_id", 0)
        .with("segment_id", "00001")
        .create();

    // Load columns
    let loader = ColumnLoader::new(dir.path().to_path_buf(), uid.clone());
    let result = loader.load_all_columns(&candidate, &["device".into()]);

    // Assertions
    assert!(result.contains_key("device"));

    let devices = result.get("device").unwrap();
    let expected_len = std::cmp::min(CONFIG.engine.event_per_zone, 2);
    assert_eq!(devices.len(), expected_len);
    assert!(devices.get_str_at(0).unwrap() == "mobile");
    if expected_len > 1 {
        assert!(devices.get_str_at(1).unwrap() == "web");
    }
}
