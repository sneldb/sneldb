use serde_json::Value;

pub struct ValueExtractor;

impl ValueExtractor {
    pub fn extract_u64(value: &Value) -> Option<u64> {
        value
            .as_u64()
            .or_else(|| {
                value
                    .as_i64()
                    .and_then(|i| if i >= 0 { Some(i as u64) } else { None })
            })
            .or_else(|| value.as_str().and_then(|s| s.parse::<u64>().ok()))
    }

    pub fn extract_i64(value: &Value) -> Option<i64> {
        value
            .as_i64()
            .or_else(|| value.as_u64().map(|u| u as i64))
            .or_else(|| value.as_str().and_then(|s| s.parse::<i64>().ok()))
    }

    pub fn extract_f64(value: &Value) -> Option<f64> {
        value
            .as_f64()
            .or_else(|| value.as_str().and_then(|s| s.parse::<f64>().ok()))
    }
}

pub struct ByteEncoder;

impl ByteEncoder {
    pub fn encode_bytes(bytes: &[u8], buffer: &mut Vec<u8>) {
        let len = bytes.len() as u32;
        buffer.extend_from_slice(&len.to_le_bytes());
        buffer.extend_from_slice(bytes);
    }

    pub fn decode_bytes<'a>(
        data: &'a [u8],
    ) -> Result<(&'a [u8], &'a [u8]), crate::engine::materialize::MaterializationError> {
        if data.len() < 4 {
            return Err(crate::engine::materialize::MaterializationError::Corrupt(
                "Insufficient bytes for length prefix".into(),
            ));
        }
        let (len_bytes, rest) = data.split_at(4);
        let len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
        if rest.len() < len {
            return Err(crate::engine::materialize::MaterializationError::Corrupt(
                "Insufficient bytes for value payload".into(),
            ));
        }
        let (bytes, remaining) = rest.split_at(len);
        Ok((bytes, remaining))
    }
}
