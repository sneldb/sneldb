use serde_json::Value;

/// Zero-allocation encoding API: writes into the provided scratch buffer when needed
/// and returns a slice view of the encoded bytes.
///
/// - Strings: returns a borrowed view of the original string bytes
/// - Numbers and bools: writes into `scratch` and returns a subslice
pub fn encode_value_into<'a>(value: &'a Value, scratch: &'a mut [u8; 8]) -> Option<&'a [u8]> {
    match value {
        Value::String(s) => Some(s.as_bytes()),
        Value::Number(n) => encode_number_into(n, scratch),
        Value::Bool(b) => {
            scratch[0] = if *b { 1u8 } else { 0u8 };
            Some(&scratch[..1])
        }
        _ => None,
    }
}

/// Backwards-compatible API (allocating). Prefer `encode_value_into`.
pub fn encode_value(value: &Value) -> Option<Vec<u8>> {
    let mut scratch = [0u8; 8];
    encode_value_into(value, &mut scratch).map(|s| s.to_vec())
}

fn encode_number_into<'a>(n: &serde_json::Number, scratch: &'a mut [u8; 8]) -> Option<&'a [u8]> {
    if let Some(i) = n.as_i64() {
        return Some(encode_i64_into(i, scratch));
    }
    if let Some(u) = n.as_u64() {
        return Some(encode_u64_into(u, scratch));
    }
    if let Some(f) = n.as_f64() {
        // Normalize integral floats to integer encoding for consistency with stored values
        if f.is_finite() {
            let t = f.trunc();
            if (f - t).abs() == 0.0 {
                // exact integer -> encode using i64 lane when representable to match schema int behavior
                if t >= (i64::MIN as f64) && t <= (i64::MAX as f64) {
                    return Some(encode_i64_into(t as i64, scratch));
                }
                // otherwise fallback to u64 if positive, else to f64 mapping
                if t >= 0.0 {
                    let u = t as u64;
                    return Some(encode_u64_into(u, scratch));
                }
            }
        }
        return Some(encode_f64_into(f, scratch));
    }
    None
}

pub fn encode_i64(i: i64) -> Vec<u8> {
    let mut scratch = [0u8; 8];
    encode_i64_into(i, &mut scratch).to_vec()
}

pub fn encode_u64(u: u64) -> Vec<u8> {
    let mut scratch = [0u8; 8];
    encode_u64_into(u, &mut scratch).to_vec()
}

pub fn encode_f64(f: f64) -> Vec<u8> {
    let mut scratch = [0u8; 8];
    encode_f64_into(f, &mut scratch).to_vec()
}

pub fn encode_i64_into<'a>(i: i64, scratch: &'a mut [u8; 8]) -> &'a [u8] {
    let ux = (i as u64) ^ 0x8000_0000_0000_0000u64;
    scratch.copy_from_slice(&ux.to_be_bytes());
    &scratch[..]
}

pub fn encode_u64_into<'a>(u: u64, scratch: &'a mut [u8; 8]) -> &'a [u8] {
    scratch.copy_from_slice(&u.to_be_bytes());
    &scratch[..]
}

pub fn encode_f64_into<'a>(f: f64, scratch: &'a mut [u8; 8]) -> &'a [u8] {
    let bits = f.to_bits();
    let lex = if (bits & (1u64 << 63)) != 0 {
        !bits
    } else {
        bits ^ (1u64 << 63)
    };
    scratch.copy_from_slice(&lex.to_be_bytes());
    &scratch[..]
}
