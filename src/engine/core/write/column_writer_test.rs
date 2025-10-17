use crate::engine::core::column::compression::{CompressedColumnIndex, CompressionCodec, Lz4Codec};
use crate::engine::core::column::format::{ColumnBlockHeader, PhysicalType};
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
