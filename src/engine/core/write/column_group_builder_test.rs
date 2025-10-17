use crate::engine::core::column::format::ColumnBlockHeader;
use crate::engine::core::write::column_group_builder::ColumnGroupBuilder;
use crate::test_helpers::factories::WriteJobFactory;

#[test]
fn groups_jobs_by_key_and_zone_and_tracks_offsets() {
    let mut builder = ColumnGroupBuilder::new();

    // Same column key and zone -> same group
    let jobs = WriteJobFactory::new()
        .with_key("login", "device")
        .with_zone_id(7)
        .create_many_with_values(&["laptop", "mobile", "tablet"]);

    for j in &jobs {
        builder.add(j);
    }

    let groups = builder.finish();
    assert_eq!(groups.len(), 1);

    let ((_key, _zone), (buf, lens, values)) = groups.into_iter().next().unwrap();
    assert_eq!(values, vec!["laptop", "mobile", "tablet"]);

    // Lengths should match the strings
    assert_eq!(lens, vec![6, 6, 6]);

    // Verify new format: header + aux (u32 lengths) + payload
    let hdr = ColumnBlockHeader::read_from(&buf[..ColumnBlockHeader::LEN]).expect("hdr");
    assert_eq!(hdr.phys, 0); // VarBytes
    assert_eq!(hdr.row_count, 3);
    assert_eq!(hdr.aux_len, 3 * 4);
    let aux_start = ColumnBlockHeader::LEN;
    let aux_end = aux_start + (hdr.aux_len as usize);
    for i in 0..3 {
        let base = aux_start + i * 4;
        let len = u32::from_le_bytes(buf[base..base + 4].try_into().unwrap()) as usize;
        assert_eq!(len, lens[i] as usize);
    }
    let payload = &buf[aux_end..];
    let concatenated = b"laptopmobiletablet".to_vec();
    assert_eq!(payload, &concatenated);
}

#[test]
fn creates_separate_groups_per_zone_or_column() {
    let mut builder = ColumnGroupBuilder::new();

    let base = WriteJobFactory::new().with_key("signup", "ip");
    let j1 = base.clone().with_zone_id(1).create_with_value("1.1.1.1");
    let j2 = base.clone().with_zone_id(2).create_with_value("2.2.2.2");
    let j3 = WriteJobFactory::new()
        .with_key("signup", "device")
        .with_zone_id(1)
        .create_with_value("desktop");

    for j in [&j1, &j2, &j3] {
        builder.add(j);
    }

    let groups = builder.finish();
    // Expect three groups: (ip,z1), (ip,z2), (device,z1)
    assert_eq!(groups.len(), 3);
}
