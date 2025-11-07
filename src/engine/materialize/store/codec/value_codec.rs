use crate::engine::materialize::MaterializationError;
use crate::engine::types::ScalarValue;
use serde_json::{Number, Value};
use sonic_rs;

use super::utils::{ByteEncoder, ValueExtractor};

pub struct ValueCodec;

impl ValueCodec {
    pub fn encode_value(
        value: &ScalarValue,
        logical_type: &str,
        buffer: &mut Vec<u8>,
    ) -> Result<(), MaterializationError> {
        match logical_type {
            "Timestamp" | "Integer" => {
                if let Some(num) = ValueExtractor::extract_i64(value) {
                    buffer.extend_from_slice(&num.to_le_bytes());
                } else if let Some(num) = ValueExtractor::extract_u64(value) {
                    buffer.extend_from_slice(&(num as i64).to_le_bytes());
                } else {
                    Self::encode_json_value(&value.to_json(), buffer)?;
                }
            }
            "Float" => {
                if let Some(f) = ValueExtractor::extract_f64(value) {
                    buffer.extend_from_slice(&f.to_le_bytes());
                } else {
                    Self::encode_json_value(&value.to_json(), buffer)?;
                }
            }
            "Boolean" => {
                let byte = if let Some(b) = value.as_bool() {
                    if b { 1u8 } else { 0u8 }
                } else if let Some(s) = value.as_str() {
                    if s.eq_ignore_ascii_case("true") {
                        1u8
                    } else if s.eq_ignore_ascii_case("false") {
                        0u8
                    } else {
                        Self::encode_json_value(&value.to_json(), buffer)?;
                        return Ok(());
                    }
                } else {
                    Self::encode_json_value(&value.to_json(), buffer)?;
                    return Ok(());
                };
                buffer.push(byte);
            }
            "String" => {
                let s = value
                    .as_str()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| value.to_json().to_string());
                ByteEncoder::encode_bytes(s.as_bytes(), buffer);
            }
            _ => {
                Self::encode_json_value(&value.to_json(), buffer)?;
            }
        }
        Ok(())
    }

    pub fn decode_value_fast<'a>(
        data: &'a [u8],
        logical_type: &str,
    ) -> Result<(Value, &'a [u8]), MaterializationError> {
        match logical_type {
            "Timestamp" | "Integer" => {
                if data.len() < 8 {
                    return Err(MaterializationError::Corrupt(format!(
                        "Insufficient bytes for integer field (have {})",
                        data.len()
                    )));
                }
                // We already checked length, so split_at is safe
                let (head, tail) = data.split_at(8);
                let num = i64::from_le_bytes(head.try_into().unwrap());
                Ok((Value::Number(Number::from(num)), tail))
            }
            "Float" => {
                if data.len() < 8 {
                    return Err(MaterializationError::Corrupt(format!(
                        "Insufficient bytes for float field (have {})",
                        data.len()
                    )));
                }
                let (head, tail) = data.split_at(8);
                let num = f64::from_le_bytes(head.try_into().unwrap());
                let value = Number::from_f64(num)
                    .map(Value::Number)
                    .unwrap_or(Value::Null);
                Ok((value, tail))
            }
            "Boolean" => {
                if data.is_empty() {
                    return Err(MaterializationError::Corrupt(
                        "Insufficient bytes for boolean field".into(),
                    ));
                }
                let (head, tail) = data.split_at(1);
                Ok((Value::Bool(head[0] != 0), tail))
            }
            "String" => Self::decode_string_fast(data),
            _ => Self::decode_json_fast(data),
        }
    }

    fn encode_json_value(value: &Value, buffer: &mut Vec<u8>) -> Result<(), MaterializationError> {
        let serialized = serde_json::to_vec(value)?;
        let len = serialized.len() as u32;
        buffer.extend_from_slice(&len.to_le_bytes());
        buffer.extend_from_slice(&serialized);
        Ok(())
    }

    #[inline(always)]
    fn decode_json_fast<'a>(data: &'a [u8]) -> Result<(Value, &'a [u8]), MaterializationError> {
        if data.len() < 4 {
            return Err(MaterializationError::Corrupt(
                "Insufficient bytes for JSON length".into(),
            ));
        }
        let (len_bytes, rest) = data.split_at(4);
        let len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
        if rest.len() < len {
            return Err(MaterializationError::Corrupt(format!(
                "Insufficient bytes for JSON payload (expected {}, got {})",
                len,
                rest.len()
            )));
        }
        let (json_bytes, tail) = rest.split_at(len);

        // Use sonic-rs for faster JSON parsing (faster than serde_json)
        let value: Value = sonic_rs::from_slice(json_bytes)
            .map_err(|e| MaterializationError::Corrupt(format!("JSON parse error: {e}")))?;
        Ok((value, tail))
    }

    #[inline(always)]
    fn decode_string_fast<'a>(data: &'a [u8]) -> Result<(Value, &'a [u8]), MaterializationError> {
        let (bytes, rest) = ByteEncoder::decode_bytes(data)?;
        // Use from_utf8_unchecked for hot path - we control the encoding
        // But we need to validate for safety, so keep the check but optimize
        let s = std::str::from_utf8(bytes)
            .map_err(|e| MaterializationError::Corrupt(format!("UTF-8 decode error: {e}")))?;
        // Avoid unnecessary allocation if we can use the slice directly
        // But Value::String requires owned String, so we need to allocate
        Ok((Value::String(s.to_string()), rest))
    }
}
