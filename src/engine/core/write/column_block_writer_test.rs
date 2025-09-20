use tempfile::tempdir;

use crate::engine::core::column::column_reader::ColumnReader;
use crate::engine::core::column::compression::compressed_column_index::CompressedColumnIndex;
use crate::engine::core::column::compression::compression_codec::Lz4Codec;
use crate::engine::core::write::column_block_writer::ColumnBlockWriter;
use crate::engine::core::write::column_group_builder::ColumnGroupBuilder;
use crate::test_helpers::factories::WriteJobFactory;

#[test]
fn writes_blocks_and_reads_them_back_via_index() {
    let tmp = tempdir().unwrap();
    let segment_dir = tmp.path().to_path_buf();

    // Build grouped uncompressed blocks via ColumnGroupBuilder
    let mut builder = ColumnGroupBuilder::new();

    // device column, zone 1 and 2
    for j in WriteJobFactory::new()
        .with_key("uid-login", "device")
        .with_zone_id(1)
        .create_many_with_values(&["laptop", "mobile", "tablet"])
    {
        builder.add(&j);
    }
    for j in WriteJobFactory::new()
        .with_key("uid-login", "device")
        .with_zone_id(2)
        .create_many_with_values(&["desktop", "phone"])
    {
        builder.add(&j);
    }

    // ip column, zone 1
    for j in WriteJobFactory::new()
        .with_key("uid-login", "ip")
        .with_zone_id(1)
        .create_many_with_values(&["1.1.1.1", "2.2.2.2"])
    {
        builder.add(&j);
    }

    let groups = builder.finish();

    // Write compressed blocks to .col and record index entries
    let codec = Lz4Codec;
    let mut writer = ColumnBlockWriter::new(segment_dir.clone());
    let mut indexes: std::collections::HashMap<(String, String), CompressedColumnIndex> =
        std::collections::HashMap::new();

    for ((key, zone_id), (buf, offs, _values)) in groups {
        let index = indexes.entry(key.clone()).or_default();
        writer
            .append_zone(index, key.clone(), zone_id, &buf, offs, &codec)
            .expect("append_zone failed");
    }
    writer.finish().expect("finish failed");

    // Persist .zfc so ColumnReader can use it
    for ((uid, field), index) in &indexes {
        let path = CompressedColumnIndex::path_for(uid, field, &segment_dir);
        index.write_to_path(&path).expect("write zfc failed");
        assert!(path.exists());
    }

    // Validate index metadata and that we can read back values
    // device, zone 1
    {
        let idx = indexes.get(&("uid-login".into(), "device".into())).unwrap();
        let z1 = idx.entries.get(&1).expect("missing zone 1");
        assert!(z1.comp_len > 0);
        assert!(z1.uncomp_len as usize > 0);
        assert_eq!(z1.num_rows, 3);

        let vals = ColumnReader::load_for_zone(&segment_dir, "001", "uid-login", "device", 1)
            .expect("read device z1");
        assert_eq!(vals, vec!["laptop", "mobile", "tablet"]);
    }

    // device, zone 2 should start after zone 1 in the same .col
    {
        let idx = indexes.get(&("uid-login".into(), "device".into())).unwrap();
        let z1 = idx.entries.get(&1).unwrap();
        let z2 = idx.entries.get(&2).unwrap();
        assert!(z2.block_start > z1.block_start);

        let vals = ColumnReader::load_for_zone(&segment_dir, "001", "uid-login", "device", 2)
            .expect("read device z2");
        assert_eq!(vals, vec!["desktop", "phone"]);
    }

    // ip, zone 1
    {
        let idx = indexes.get(&("uid-login".into(), "ip".into())).unwrap();
        let z1 = idx.entries.get(&1).expect("missing ip z1");
        assert_eq!(z1.num_rows, 2);

        let vals = ColumnReader::load_for_zone(&segment_dir, "001", "uid-login", "ip", 1)
            .expect("read ip z1");
        assert_eq!(vals, vec!["1.1.1.1", "2.2.2.2"]);
    }
}
