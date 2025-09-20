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

    let ((_key, _zone), (buf, offs, values)) = groups.into_iter().next().unwrap();
    assert_eq!(values, vec!["laptop", "mobile", "tablet"]);

    // Offsets are within the buffer and monotonically increasing
    assert_eq!(offs.len(), 3);
    assert!(offs[0] < buf.len() as u32);
    assert!(offs[1] < buf.len() as u32);
    assert!(offs[2] < buf.len() as u32);
    assert!(offs[0] < offs[1] && offs[1] < offs[2]);

    // Verify serialization format: [u16 len][bytes] repeated
    let mut cursor = 0usize;
    for expected in ["laptop", "mobile", "tablet"] {
        let len = u16::from_le_bytes([buf[cursor], buf[cursor + 1]]) as usize;
        cursor += 2;
        let bytes = &buf[cursor..cursor + len];
        cursor += len;
        assert_eq!(std::str::from_utf8(bytes).unwrap(), expected);
    }
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
