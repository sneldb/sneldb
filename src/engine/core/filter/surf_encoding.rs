use crate::engine::types::ScalarValue;

pub fn encode_value(value: &ScalarValue) -> Option<Vec<u8>> {
    match value {
        ScalarValue::Utf8(s) => Some(s.as_bytes().to_vec()),
        ScalarValue::Int64(i) => Some(encode_i64(*i)),
        ScalarValue::Timestamp(ts) => Some(encode_i64(*ts)),
        ScalarValue::Float64(f) => {
            // Normalize integral floats to integer encoding for consistency with stored values
            if f.is_finite() {
                let t = f.trunc();
                if (f - t).abs() == 0.0 {
                    // exact integer -> encode using i64 lane when representable to match schema int behavior
                    if t >= (i64::MIN as f64) && t <= (i64::MAX as f64) {
                        return Some(encode_i64(t as i64));
                    }
                    // otherwise fallback to u64 if positive, else to f64 mapping
                    if t >= 0.0 {
                        let u = t as u64;
                        return Some(encode_u64(u));
                    }
                }
            }
            Some(encode_f64(*f))
        }
        ScalarValue::Boolean(b) => Some(if *b { vec![1u8] } else { vec![0u8] }),
        // JSON numbers are now Utf8 strings - try to parse
        ScalarValue::Utf8(s) => {
            if let Ok(i) = s.parse::<i64>() {
                Some(encode_i64(i))
            } else if let Ok(u) = s.parse::<u64>() {
                Some(encode_u64(u))
            } else if let Ok(f) = s.parse::<f64>() {
                // Normalize integral floats to integer encoding for consistency with stored values
                if f.is_finite() {
                    let t = f.trunc();
                    if (f - t).abs() == 0.0 {
                        // exact integer -> encode using i64 lane when representable to match schema int behavior
                        if t >= (i64::MIN as f64) && t <= (i64::MAX as f64) {
                            return Some(encode_i64(t as i64));
                        }
                        // otherwise fallback to u64 if positive, else to f64 mapping
                        if t >= 0.0 {
                            let u = t as u64;
                            return Some(encode_u64(u));
                        }
                    }
                }
                Some(encode_f64(f))
            } else {
                None
            }
        }
        _ => None,
    }
}

pub fn encode_i64(i: i64) -> Vec<u8> {
    let ux = (i as u64) ^ 0x8000_0000_0000_0000u64;
    ux.to_be_bytes().to_vec()
}

pub fn encode_u64(u: u64) -> Vec<u8> {
    u.to_be_bytes().to_vec()
}

pub fn encode_f64(f: f64) -> Vec<u8> {
    let bits = f.to_bits();
    let lex = if (bits & (1u64 << 63)) != 0 {
        !bits
    } else {
        bits ^ (1u64 << 63)
    };
    lex.to_be_bytes().to_vec()
}
