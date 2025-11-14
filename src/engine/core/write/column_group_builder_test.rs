use crate::engine::core::column::format::{ColumnBlockHeader, PhysicalType};
use crate::engine::core::write::column_group_builder::ColumnGroupBuilder;
use crate::engine::core::WriteJob;
use crate::engine::types::ScalarValue;
use crate::test_helpers::factories::WriteJobFactory;
use std::collections::HashMap;
use std::path::PathBuf;

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

#[test]
fn event_id_handles_negative_int64_as_large_u64() {
    // Test the fix for event_id > i64::MAX
    // When event_id.raw() > i64::MAX, casting to i64 wraps to negative
    // The fix should convert negative i64 back to u64 string representation

    let mut types = HashMap::new();
    types.insert(
        ("login".to_string(), "event_id".to_string()),
        PhysicalType::U64,
    );

    let mut builder = ColumnGroupBuilder::with_types(types);

    // Create event_id values that would wrap when cast to i64
    // i64::MAX = 9223372036854775807
    // u64 value > i64::MAX, e.g., 9223372036854775808 (i64::MAX + 1)
    let large_u64 = i64::MAX as u64 + 1;
    let negative_i64 = large_u64 as i64; // This wraps to negative

    let job = WriteJob {
        key: ("login".to_string(), "event_id".to_string()),
        zone_id: 0,
        path: PathBuf::from("/tmp/test.col"),
        value: ScalarValue::Int64(negative_i64),
    };

    builder.add(&job);

    let groups = builder.finish();
    let ((_key, _zone), (_buf, _lens, values)) = groups.into_iter().next().unwrap();

    // The value should be converted to u64 string, not negative string
    assert_eq!(values[0], large_u64.to_string());
    assert_ne!(values[0], negative_i64.to_string()); // Should not be negative
}

#[test]
fn event_id_handles_normal_int64_values() {
    // Test that normal (non-negative) Int64 values for event_id work correctly
    let mut types = HashMap::new();
    types.insert(
        ("login".to_string(), "event_id".to_string()),
        PhysicalType::U64,
    );

    let mut builder = ColumnGroupBuilder::with_types(types);

    let normal_i64 = 12345i64;
    let job = WriteJob {
        key: ("login".to_string(), "event_id".to_string()),
        zone_id: 0,
        path: PathBuf::from("/tmp/test.col"),
        value: ScalarValue::Int64(normal_i64),
    };

    builder.add(&job);

    let groups = builder.finish();
    let ((_key, _zone), (_buf, _lens, values)) = groups.into_iter().next().unwrap();

    // Normal values should be converted to string as-is
    assert_eq!(values[0], normal_i64.to_string());
}

#[test]
fn event_id_handles_multiple_values_including_large_u64() {
    // Test mixing normal and large u64 values
    let mut types = HashMap::new();
    types.insert(
        ("login".to_string(), "event_id".to_string()),
        PhysicalType::U64,
    );

    let mut builder = ColumnGroupBuilder::with_types(types);

    let normal = 100i64;
    let large_u64 = i64::MAX as u64 + 100;
    let negative_i64 = large_u64 as i64; // Wraps to negative

    let jobs = vec![
        WriteJob {
            key: ("login".to_string(), "event_id".to_string()),
            zone_id: 0,
            path: PathBuf::from("/tmp/test.col"),
            value: ScalarValue::Int64(normal),
        },
        WriteJob {
            key: ("login".to_string(), "event_id".to_string()),
            zone_id: 0,
            path: PathBuf::from("/tmp/test.col"),
            value: ScalarValue::Int64(negative_i64),
        },
    ];

    for job in &jobs {
        builder.add(job);
    }

    let groups = builder.finish();
    let ((_key, _zone), (_buf, _lens, values)) = groups.into_iter().next().unwrap();

    assert_eq!(values.len(), 2);
    assert_eq!(values[0], normal.to_string());
    assert_eq!(values[1], large_u64.to_string());
}

#[test]
fn non_event_id_int64_fields_not_affected() {
    // Test that non-event_id Int64 fields are not affected by the special handling
    let mut types = HashMap::new();
    types.insert(
        ("login".to_string(), "timestamp".to_string()),
        PhysicalType::I64,
    );

    let mut builder = ColumnGroupBuilder::with_types(types);

    let negative_i64 = -100i64;
    let job = WriteJob {
        key: ("login".to_string(), "timestamp".to_string()),
        zone_id: 0,
        path: PathBuf::from("/tmp/test.col"),
        value: ScalarValue::Int64(negative_i64),
    };

    builder.add(&job);

    let groups = builder.finish();
    let ((_key, _zone), (_buf, _lens, values)) = groups.into_iter().next().unwrap();

    // Non-event_id fields should keep negative values as-is
    assert_eq!(values[0], negative_i64.to_string());
}
