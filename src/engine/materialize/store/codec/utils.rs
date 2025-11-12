use crate::engine::materialize::MaterializationError;
use crate::engine::types::ScalarValue;

pub struct ValueExtractor;

impl ValueExtractor {
    pub fn extract_u64(value: &ScalarValue) -> Option<u64> {
        value.as_u64()
    }

    pub fn extract_i64(value: &ScalarValue) -> Option<i64> {
        value.as_i64()
    }

    pub fn extract_f64(value: &ScalarValue) -> Option<f64> {
        value.as_f64()
    }
}

pub struct ByteEncoder;

impl ByteEncoder {
    pub fn encode_bytes(bytes: &[u8], buffer: &mut Vec<u8>) {
        let len = bytes.len() as u32;
        buffer.extend_from_slice(&len.to_le_bytes());
        buffer.extend_from_slice(bytes);
    }

    pub fn decode_bytes<'a>(data: &'a [u8]) -> Result<(&'a [u8], &'a [u8]), MaterializationError> {
        if data.len() < 4 {
            return Err(MaterializationError::Corrupt(
                "Insufficient bytes for length prefix".into(),
            ));
        }
        let (len_bytes, rest) = data.split_at(4);
        let len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
        if rest.len() < len {
            return Err(MaterializationError::Corrupt(
                "Insufficient bytes for value payload".into(),
            ));
        }
        let (bytes, remaining) = rest.split_at(len);
        Ok((bytes, remaining))
    }
}
