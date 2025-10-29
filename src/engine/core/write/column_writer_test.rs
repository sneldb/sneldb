use crate::engine::core::column::compression::{CompressedColumnIndex, CompressionCodec, Lz4Codec};
use crate::engine::core::column::format::{ColumnBlockHeader, PhysicalType};
use crate::engine::core::column::type_catalog::ColumnTypeCatalog;
use crate::engine::core::{ColumnWriter, ZonePlan};
use crate::shared::storage_header::BinaryHeader;
use crate::test_helpers::factories::{EventFactory, SchemaRegistryFactory};
use serde_json::json;
use std::io::{Read, Seek, SeekFrom};
use tempfile::tempdir;
#[tokio::test]
async fn writes_columns() {
    // Setup temporary dir and registry
    let dir = tempdir().unwrap();
    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();

    // Define schema for "login" event type
    registry_factory
        .define_with_fields(
            "login",
            &[
                ("device", "string"),
                ("ip", "string"),
                ("timestamp", "u64"),
                ("event_type", "string"),
                ("context_id", "string"),
                ("index", "u32"),
            ],
        )
        .await
        .expect("Failed to define schema");

    // Generate events and wrap in a ZonePlan
    let events = EventFactory::new()
        .with("event_type", "login")
        .with("payload", json!({ "device": "mobile", "ip": "127.0.0.1" }))
        .create_list(2);

    let zone = ZonePlan {
        id: 42,
        start_index: 0,
        end_index: 1,
        events,
        uid: "uid-login".into(),
        event_type: "login".into(),
        segment_id: 7,
    };

    let writer = ColumnWriter::new(dir.path().to_path_buf(), registry);

    // Run the writer
    writer.write_all(&[zone]).await.expect("write failed");

    // Verify some expected files exist
    let file_names = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();

    assert!(file_names.iter().any(|f| f.ends_with("_device.col")));
    assert!(file_names.iter().any(|f| f.ends_with("_ip.col")));
    assert!(file_names.iter().any(|f| f.ends_with("_timestamp.col")));

    // Also expect zone index files (.zfc) to exist
    assert!(file_names.iter().any(|f| f.ends_with("_device.zfc")));
    assert!(file_names.iter().any(|f| f.ends_with("_ip.zfc")));
    assert!(file_names.iter().any(|f| f.ends_with("_timestamp.zfc")));

    // Check that timestamp column, once decompressed, has a typed header (I64 phys)
    let ts_col = file_names
        .iter()
        .find(|p| p.ends_with("_timestamp.col"))
        .map(|p| dir.path().join(p))
        .expect("timestamp col");
    let ts_zfc = file_names
        .iter()
        .find(|p| p.ends_with("_timestamp.zfc"))
        .map(|p| dir.path().join(p))
        .expect("timestamp zfc");
    let index = CompressedColumnIndex::load_from_path(&ts_zfc).expect("zfc load");
    // Our zone id was 42
    let entry = index.entries.get(&42).expect("zfc entry");
    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .open(&ts_col)
        .unwrap();
    let _bin = BinaryHeader::read_from(&mut f).expect("bin hdr");
    // block_start is an absolute file offset; do not add BinaryHeader again
    f.seek(SeekFrom::Start(entry.block_start)).unwrap();
    let mut comp = vec![0u8; entry.comp_len as usize];
    f.read_exact(&mut comp).unwrap();
    let codec = Lz4Codec;
    let decomp =
        CompressionCodec::decompress(&codec, &comp, entry.uncomp_len as usize).expect("decompress");
    let col_hdr = ColumnBlockHeader::read_from(&decomp[..ColumnBlockHeader::LEN]).expect("col hdr");
    assert_eq!(PhysicalType::from(col_hdr.phys), PhysicalType::I64);
}

#[tokio::test]
async fn uses_type_hints_when_schema_falls_back_to_varbytes() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let dir = tempdir().unwrap();
    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();

    registry_factory
        .define_with_fields(
            "hinted",
            &[
                ("context_id", "string"),
                ("event_type", "string"),
                ("metric", "string"), // schema says string => writer defaults to VarBytes
            ],
        )
        .await
        .expect("schema define");

    let events = EventFactory::new()
        .with("event_type", "hinted")
        .with("payload", json!({ "metric": 7 }))
        .create_list(1);

    let zone = ZonePlan {
        id: 11,
        start_index: 0,
        end_index: 0,
        events,
        uid: "uid-hinted".into(),
        event_type: "hinted".into(),
        segment_id: 2,
    };

    let mut catalog = ColumnTypeCatalog::new();
    catalog.record(
        ("hinted".to_string(), "metric".to_string()),
        PhysicalType::U64,
    );

    let writer = ColumnWriter::new(dir.path().to_path_buf(), registry).with_type_hints(catalog);
    writer.write_all(&[zone]).await.expect("write_all");

    let metric_col = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| {
            p.extension().map(|ext| ext == "col").unwrap_or(false)
                && p.file_name()
                    .map(|name| name.to_string_lossy().contains("metric"))
                    .unwrap_or(false)
        })
        .expect("metric col");
    let metric_zfc = metric_col.with_extension("zfc");

    let index = CompressedColumnIndex::load_from_path(&metric_zfc).expect("load zfc");
    let entry = index.entries.get(&11).expect("zfc entry");

    let mut file = std::fs::File::open(&metric_col).expect("open col");
    let _hdr = BinaryHeader::read_from(&mut file).expect("bin hdr");
    file.seek(SeekFrom::Start(entry.block_start)).unwrap();
    let mut compressed = vec![0u8; entry.comp_len as usize];
    file.read_exact(&mut compressed).unwrap();

    let codec = Lz4Codec;
    let decompressed = CompressionCodec::decompress(&codec, &compressed, entry.uncomp_len as usize)
        .expect("decompress");
    let col_hdr =
        ColumnBlockHeader::read_from(&decompressed[..ColumnBlockHeader::LEN]).expect("column hdr");
    assert_eq!(PhysicalType::from(col_hdr.phys), PhysicalType::U64);
}
