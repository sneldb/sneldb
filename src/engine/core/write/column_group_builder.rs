use std::collections::BTreeMap;

use crate::engine::core::column::format::{ColumnBlockHeader, PhysicalType};
use crate::engine::core::{ColumnKey, WriteJob};
use crate::engine::types::ScalarValue;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};

pub struct ColumnGroupBuilder {
    // Accumulate raw string values per (column, zone); we'll assemble typed blocks in finish()
    groups: BTreeMap<(ColumnKey, u32), Vec<String>>,
    types_by_key: std::collections::HashMap<(String, String), PhysicalType>,
}

impl ColumnGroupBuilder {
    pub fn new() -> Self {
        Self {
            groups: BTreeMap::new(),
            types_by_key: std::collections::HashMap::new(),
        }
    }

    pub fn with_types(
        types_by_key: std::collections::HashMap<(String, String), PhysicalType>,
    ) -> Self {
        Self {
            groups: BTreeMap::new(),
            types_by_key,
        }
    }

    pub fn add(&mut self, job: &WriteJob) {
        let values = self
            .groups
            .entry((job.key.clone(), job.zone_id))
            .or_insert_with(|| Vec::new());
        let s = match &job.value {
            ScalarValue::Utf8(s) => s.clone(),
            ScalarValue::Int64(i) => {
                // For event_id field, convert to u64 string to match PhysicalType::U64
                // This handles the case where event_id > i64::MAX (casts to negative i64)
                if job.key.1 == "event_id" && *i < 0 {
                    // Negative i64 means original u64 > i64::MAX, reconstruct it
                    (*i as u64).to_string()
                } else {
                    i.to_string()
                }
            }
            ScalarValue::Timestamp(ts) => ts.to_string(),
            ScalarValue::Float64(f) => f.to_string(),
            ScalarValue::Boolean(b) => b.to_string(),
            ScalarValue::Null => String::new(),
            ScalarValue::Binary(bytes) => BASE64_STANDARD.encode(bytes),
        };
        values.push(s);
    }

    pub fn finish(self) -> BTreeMap<(ColumnKey, u32), (Vec<u8>, Vec<u32>, Vec<String>)> {
        let mut out: BTreeMap<(ColumnKey, u32), (Vec<u8>, Vec<u32>, Vec<String>)> = BTreeMap::new();
        for (key_zone, values) in self.groups {
            let (key, _zone) = &key_zone;
            let phys = self
                .types_by_key
                .get(key)
                .cloned()
                .unwrap_or(PhysicalType::VarBytes);
            let row_count = values.len() as u32;
            match phys {
                PhysicalType::I64 => {
                    // Build null bitmap (if any) and fixed-width payload
                    let mut nulls: Vec<u8> = vec![0u8; ((row_count as usize) + 7) / 8];
                    let mut payload: Vec<u8> = Vec::with_capacity((row_count as usize) * 8);
                    let mut any_nulls = false;
                    for (i, s) in values.iter().enumerate() {
                        if let Ok(n) = s.parse::<i64>() {
                            payload.extend_from_slice(&n.to_le_bytes());
                        } else {
                            any_nulls = true;
                            nulls[i / 8] |= 1 << (i % 8);
                            payload.extend_from_slice(&0i64.to_le_bytes());
                        }
                    }
                    // Align payload start to 8 bytes to enable zero-copy typed slices
                    let mut aux_len = if any_nulls { nulls.len() } else { 0 };
                    let pad = (8 - ((ColumnBlockHeader::LEN + aux_len) % 8)) % 8;
                    aux_len += pad;
                    let mut buf: Vec<u8> =
                        Vec::with_capacity(ColumnBlockHeader::LEN + aux_len + payload.len());
                    let header = ColumnBlockHeader::new(
                        PhysicalType::I64,
                        any_nulls,
                        row_count,
                        aux_len as u32,
                    );
                    header.write_to(&mut buf);
                    if any_nulls {
                        buf.extend_from_slice(&nulls);
                    }
                    if pad > 0 {
                        buf.extend(std::iter::repeat(0u8).take(pad));
                    }
                    buf.extend_from_slice(&payload);
                    // lengths here represent per-row byte lengths for VarBytes; for typed blocks we can store zeros
                    let lengths = vec![0u32; row_count as usize];
                    out.insert(key_zone, (buf, lengths, values));
                }
                PhysicalType::U64 => {
                    let mut nulls: Vec<u8> = vec![0u8; ((row_count as usize) + 7) / 8];
                    let mut payload: Vec<u8> = Vec::with_capacity((row_count as usize) * 8);
                    let mut any_nulls = false;
                    for (i, s) in values.iter().enumerate() {
                        if let Ok(n) = s.parse::<u64>() {
                            payload.extend_from_slice(&n.to_le_bytes());
                        } else {
                            any_nulls = true;
                            nulls[i / 8] |= 1 << (i % 8);
                            payload.extend_from_slice(&0u64.to_le_bytes());
                        }
                    }
                    let mut aux_len = if any_nulls { nulls.len() } else { 0 };
                    let pad = (8 - ((ColumnBlockHeader::LEN + aux_len) % 8)) % 8;
                    aux_len += pad;
                    let mut buf: Vec<u8> =
                        Vec::with_capacity(ColumnBlockHeader::LEN + aux_len + payload.len());
                    let header = ColumnBlockHeader::new(
                        PhysicalType::U64,
                        any_nulls,
                        row_count,
                        aux_len as u32,
                    );
                    header.write_to(&mut buf);
                    if any_nulls {
                        buf.extend_from_slice(&nulls);
                    }
                    if pad > 0 {
                        buf.extend(std::iter::repeat(0u8).take(pad));
                    }
                    buf.extend_from_slice(&payload);
                    let lengths = vec![0u32; row_count as usize];
                    out.insert(key_zone, (buf, lengths, values));
                }
                PhysicalType::F64 => {
                    let mut nulls: Vec<u8> = vec![0u8; ((row_count as usize) + 7) / 8];
                    let mut payload: Vec<u8> = Vec::with_capacity((row_count as usize) * 8);
                    let mut any_nulls = false;
                    for (i, s) in values.iter().enumerate() {
                        if let Ok(f) = s.parse::<f64>() {
                            payload.extend_from_slice(&f.to_le_bytes());
                        } else {
                            any_nulls = true;
                            nulls[i / 8] |= 1 << (i % 8);
                            payload.extend_from_slice(&0f64.to_le_bytes());
                        }
                    }
                    let mut aux_len = if any_nulls { nulls.len() } else { 0 };
                    let pad = (8 - ((ColumnBlockHeader::LEN + aux_len) % 8)) % 8;
                    aux_len += pad;
                    let mut buf: Vec<u8> =
                        Vec::with_capacity(ColumnBlockHeader::LEN + aux_len + payload.len());
                    let header = ColumnBlockHeader::new(
                        PhysicalType::F64,
                        any_nulls,
                        row_count,
                        aux_len as u32,
                    );
                    header.write_to(&mut buf);
                    if any_nulls {
                        buf.extend_from_slice(&nulls);
                    }
                    if pad > 0 {
                        buf.extend(std::iter::repeat(0u8).take(pad));
                    }
                    buf.extend_from_slice(&payload);
                    let lengths = vec![0u32; row_count as usize];
                    out.insert(key_zone, (buf, lengths, values));
                }
                PhysicalType::Bool => {
                    // Values bitset as payload; nulls bitset in aux if any
                    let bits_len = ((row_count as usize) + 7) / 8;
                    let mut values_bits: Vec<u8> = vec![0u8; bits_len];
                    let mut nulls: Vec<u8> = vec![0u8; bits_len];
                    let mut any_nulls = false;
                    for (i, s) in values.iter().enumerate() {
                        match s {
                            v if v.eq_ignore_ascii_case("true") => {
                                values_bits[i / 8] |= 1 << (i % 8);
                            }
                            v if v.eq_ignore_ascii_case("false") => {
                                // leave bit 0
                            }
                            _ => {
                                any_nulls = true;
                                nulls[i / 8] |= 1 << (i % 8);
                            }
                        }
                    }
                    let aux_len = if any_nulls { bits_len } else { 0 };
                    let mut buf: Vec<u8> =
                        Vec::with_capacity(ColumnBlockHeader::LEN + aux_len + values_bits.len());
                    let header = ColumnBlockHeader::new(
                        PhysicalType::Bool,
                        any_nulls,
                        row_count,
                        aux_len as u32,
                    );
                    header.write_to(&mut buf);
                    if any_nulls {
                        buf.extend_from_slice(&nulls);
                    }
                    buf.extend_from_slice(&values_bits);
                    let lengths = vec![0u32; row_count as usize];
                    out.insert(key_zone, (buf, lengths, values));
                }
                _ => {
                    // VarBytes (default)
                    let mut lengths: Vec<u32> = Vec::with_capacity(values.len());
                    let mut payload: Vec<u8> = Vec::new();
                    for s in &values {
                        let b = s.as_bytes();
                        lengths.push(b.len() as u32);
                        payload.extend_from_slice(b);
                    }
                    let aux_len = (row_count as usize) * 4;
                    let mut buf: Vec<u8> =
                        Vec::with_capacity(ColumnBlockHeader::LEN + aux_len + payload.len());
                    let header = ColumnBlockHeader::new(
                        PhysicalType::VarBytes,
                        false,
                        row_count,
                        aux_len as u32,
                    );
                    header.write_to(&mut buf);
                    for len in &lengths {
                        buf.extend_from_slice(&len.to_le_bytes());
                    }
                    buf.extend_from_slice(&payload);
                    out.insert(key_zone, (buf, lengths, values));
                }
            }
        }
        out
    }
}
