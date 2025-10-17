use std::sync::{Arc, OnceLock};

use crate::engine::core::column::format::PhysicalType;
use crate::engine::core::read::cache::DecompressedBlock;
use std::simd::Simd;
use std::simd::prelude::*;

/// Zero-copy view over values stored inside a decompressed column block.
#[derive(Clone, Debug)]
pub struct ColumnValues {
    pub block: Arc<DecompressedBlock>,
    pub ranges: Vec<(usize, usize)>, // (start, len)
    // Lazily constructed cache of parsed i64 values; None for non-integers.
    numeric_cache: Arc<OnceLock<Arc<Vec<Option<i64>>>>>,
    // Cached result indicating whether all entries are valid UTF-8
    utf8_validated: Arc<OnceLock<bool>>,
    // Optional typed i64 view: (payload_start, row_count, optional (nulls_start, nulls_len))
    typed_i64: Option<(usize, usize, Option<(usize, usize)>)>,
    // Optional typed u64 view
    typed_u64: Option<(usize, usize, Option<(usize, usize)>)>,
    // Optional typed f64 view
    typed_f64: Option<(usize, usize, Option<(usize, usize)>)>,
    // Optional typed bool view: (payload_start for bitset, row_count, optional nulls bitset)
    typed_bool: Option<(usize, usize, Option<(usize, usize)>)>,
}

impl ColumnValues {
    pub fn empty() -> Self {
        Self::new(
            Arc::new(DecompressedBlock::from_bytes(Vec::new())),
            Vec::new(),
        )
    }
    pub fn new(block: Arc<DecompressedBlock>, ranges: Vec<(usize, usize)>) -> Self {
        Self {
            block,
            ranges,
            numeric_cache: Arc::new(OnceLock::new()),
            utf8_validated: Arc::new(OnceLock::new()),
            typed_i64: None,
            typed_u64: None,
            typed_f64: None,
            typed_bool: None,
        }
    }

    pub fn new_typed_i64(
        block: Arc<DecompressedBlock>,
        payload_start: usize,
        row_count: usize,
        nulls: Option<(usize, usize)>,
    ) -> Self {
        Self {
            block,
            ranges: Vec::new(),
            numeric_cache: Arc::new(OnceLock::new()),
            utf8_validated: Arc::new(OnceLock::new()),
            typed_i64: Some((payload_start, row_count, nulls)),
            typed_u64: None,
            typed_f64: None,
            typed_bool: None,
        }
    }

    pub fn new_typed_u64(
        block: Arc<DecompressedBlock>,
        payload_start: usize,
        row_count: usize,
        nulls: Option<(usize, usize)>,
    ) -> Self {
        Self {
            block,
            ranges: Vec::new(),
            numeric_cache: Arc::new(OnceLock::new()),
            utf8_validated: Arc::new(OnceLock::new()),
            typed_i64: None,
            typed_u64: Some((payload_start, row_count, nulls)),
            typed_f64: None,
            typed_bool: None,
        }
    }

    pub fn new_typed_f64(
        block: Arc<DecompressedBlock>,
        payload_start: usize,
        row_count: usize,
        nulls: Option<(usize, usize)>,
    ) -> Self {
        Self {
            block,
            ranges: Vec::new(),
            numeric_cache: Arc::new(OnceLock::new()),
            utf8_validated: Arc::new(OnceLock::new()),
            typed_i64: None,
            typed_u64: None,
            typed_f64: Some((payload_start, row_count, nulls)),
            typed_bool: None,
        }
    }

    pub fn new_typed_bool(
        block: Arc<DecompressedBlock>,
        payload_start: usize,
        row_count: usize,
        nulls: Option<(usize, usize)>,
    ) -> Self {
        Self {
            block,
            ranges: Vec::new(),
            numeric_cache: Arc::new(OnceLock::new()),
            utf8_validated: Arc::new(OnceLock::new()),
            typed_i64: None,
            typed_u64: None,
            typed_f64: None,
            typed_bool: Some((payload_start, row_count, nulls)),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        if let Some((_, rc, _)) = self.typed_i64 {
            return rc;
        }
        if let Some((_, rc, _)) = self.typed_u64 {
            return rc;
        }
        if let Some((_, rc, _)) = self.typed_f64 {
            return rc;
        }
        if let Some((_, rc, _)) = self.typed_bool {
            return rc;
        }
        self.ranges.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    /// Returns the physical type of this column if it's typed
    #[inline]
    pub fn physical_type(&self) -> Option<PhysicalType> {
        if self.typed_i64.is_some() {
            Some(PhysicalType::I64)
        } else if self.typed_u64.is_some() {
            Some(PhysicalType::U64)
        } else if self.typed_f64.is_some() {
            Some(PhysicalType::F64)
        } else if self.typed_bool.is_some() {
            Some(PhysicalType::Bool)
        } else {
            None // VarBytes or untyped
        }
    }

    /// Check if this column has a known type (faster access path)
    #[inline]
    pub fn is_typed(&self) -> bool {
        self.typed_i64.is_some()
            || self.typed_u64.is_some()
            || self.typed_f64.is_some()
            || self.typed_bool.is_some()
    }

    #[inline]
    pub fn get_str_at(&self, index: usize) -> Option<&str> {
        if self.typed_i64.is_some() {
            // Typed numeric column has no string view
            return None;
        }
        let (start, len) = *self.ranges.get(index)?;
        let bytes = &self.block.bytes[start..start + len];
        // Values are UTF-8 encoded when written; if invalid, return None.
        std::str::from_utf8(bytes).ok()
    }

    /// Validates the entire column contains valid UTF-8; caches the result.
    pub fn validate_utf8(&self) -> bool {
        *self.utf8_validated.get_or_init(|| {
            // SIMD ASCII fast-path: many values are ASCII only; validate quickly
            const LANES: usize = 32;
            for (start, len) in &self.ranges {
                let bytes = &self.block.bytes[*start..*start + *len];
                let mut i = 0;
                let blen = bytes.len();
                // Check ASCII with SIMD (highest bit must be 0)
                while i + LANES <= blen {
                    let v = Simd::<u8, LANES>::from_array(
                        bytes[i..i + LANES]
                            .try_into()
                            .expect("slice to array of LANES"),
                    );
                    let m = v & Simd::splat(0x80);
                    let nz = m.simd_ne(Simd::splat(0));
                    if nz.to_bitmask() != 0 {
                        // Non-ASCII present; fall back to full utf8 check
                        if std::str::from_utf8(bytes).is_err() {
                            return false;
                        } else {
                            break; // this string ok
                        }
                    }
                    i += LANES;
                }
                // Scalar tail (already ASCII-only chunked); tail also ASCII-only check
                while i < blen {
                    if bytes[i] & 0x80 != 0 {
                        if std::str::from_utf8(bytes).is_err() {
                            return false;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            true
        })
    }

    /// Unsafe fast-path: assumes the column has been validated via `validate_utf8`.
    #[inline]
    pub fn get_str_at_unchecked(&self, index: usize) -> Option<&str> {
        let (start, len) = *self.ranges.get(index)?;
        let bytes = &self.block.bytes[start..start + len];
        Some(unsafe { std::str::from_utf8_unchecked(bytes) })
    }

    #[inline]
    pub fn get_i64_at(&self, index: usize) -> Option<i64> {
        if let Some((payload_start, row_count, nulls)) = self.typed_i64 {
            if index >= row_count {
                return None;
            }
            if let Some((ns, _nl)) = nulls {
                let nb = &self.block.bytes[ns..];
                let bit = nb[index / 8] & (1 << (index % 8));
                if bit != 0 {
                    return None;
                }
            }
            let base = payload_start + index * 8;
            let bytes = &self.block.bytes[base..base + 8];
            // SAFETY: writer guarantees 8-byte aligned payload_start for numeric types
            return Some(i64::from_le_bytes(bytes.try_into().ok()?));
        }
        let cache = self.numeric_cache.get_or_init(|| {
            let mut parsed: Vec<Option<i64>> = Vec::with_capacity(self.ranges.len());
            for i in 0..self.ranges.len() {
                let v = self.get_str_at(i).and_then(|s| fast_parse_i64(s));
                parsed.push(v);
            }
            Arc::new(parsed)
        });
        cache.get(index)?.clone()
    }

    #[inline]
    pub fn get_u64_at(&self, index: usize) -> Option<u64> {
        if let Some((payload_start, row_count, nulls)) = self.typed_u64 {
            if index >= row_count {
                return None;
            }
            if let Some((ns, _nl)) = nulls {
                let nb = &self.block.bytes[ns..];
                let bit = nb[index / 8] & (1 << (index % 8));
                if bit != 0 {
                    return None;
                }
            }
            let base = payload_start + index * 8;
            let bytes = &self.block.bytes[base..base + 8];
            return Some(u64::from_le_bytes(bytes.try_into().ok()?));
        }
        None
    }

    #[inline]
    pub fn get_f64_at(&self, index: usize) -> Option<f64> {
        if let Some((payload_start, row_count, nulls)) = self.typed_f64 {
            if index >= row_count {
                return None;
            }
            if let Some((ns, _nl)) = nulls {
                let nb = &self.block.bytes[ns..];
                let bit = nb[index / 8] & (1 << (index % 8));
                if bit != 0 {
                    return None;
                }
            }
            let base = payload_start + index * 8;
            let bytes = &self.block.bytes[base..base + 8];
            return Some(f64::from_le_bytes(bytes.try_into().ok()?));
        }
        None
    }

    #[inline]
    pub fn get_bool_at(&self, index: usize) -> Option<bool> {
        if let Some((payload_start, row_count, nulls)) = self.typed_bool {
            if index >= row_count {
                return None;
            }
            if let Some((ns, _nl)) = nulls {
                let nb = &self.block.bytes[ns..];
                let bit = nb[index / 8] & (1 << (index % 8));
                if bit != 0 {
                    return None;
                }
            }
            let vb = &self.block.bytes[payload_start..];
            let bit = vb[index / 8] & (1 << (index % 8));
            return Some(bit != 0);
        }
        None
    }

    /// Pre-builds the numeric cache for this column if any numeric access is expected.
    /// This avoids first-touch contention and amortizes parsing cost outside the hot loop.
    pub fn warm_numeric_cache(&self) {
        let _ = self.numeric_cache.get_or_init(|| {
            let mut parsed: Vec<Option<i64>> = Vec::with_capacity(self.ranges.len());
            for i in 0..self.ranges.len() {
                let v = self.get_str_at(i).and_then(|s| fast_parse_i64(s));
                parsed.push(v);
            }
            Arc::new(parsed)
        });
    }
}

#[inline]
fn fast_parse_i64(s: &str) -> Option<i64> {
    // Trim spaces quickly
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return None;
    }
    let mut neg = false;
    let mut start = 0;
    if bytes[0] == b'-' {
        neg = true;
        start = 1;
        if start == bytes.len() {
            return None;
        }
    }

    // SIMD digit precheck: ensure all remaining bytes are '0'..'9'
    const LANES: usize = 32;
    let mut i = start;
    while i + LANES <= bytes.len() {
        let v = Simd::<u8, LANES>::from_array(
            bytes[i..i + LANES]
                .try_into()
                .expect("slice to array of LANES"),
        );
        let ge0 = v.simd_ge(Simd::splat(b'0'));
        let le9 = v.simd_le(Simd::splat(b'9'));
        let ok = ge0 & le9;
        let bits = ok.to_bitmask() as u128;
        if bits != ((1u128 << LANES) - 1) {
            // Fallback to standard parse if mixed or non-digit encountered
            return s.parse::<i64>().ok();
        }
        i += LANES;
    }
    while i < bytes.len() {
        let c = bytes[i];
        if c < b'0' || c > b'9' {
            return s.parse::<i64>().ok();
        }
        i += 1;
    }

    // All digits: convert
    let mut value: i128 = 0; // wider to avoid overflow during accumulation
    for &c in &bytes[start..] {
        value = value * 10 + (c - b'0') as i128;
        if value > i64::MAX as i128 + if neg { 1 } else { 0 } {
            // Overflow: fallback to standard parse (will fail or clamp appropriately)
            return s.parse::<i64>().ok();
        }
    }
    // Apply sign in i128 to avoid i64::MIN negation overflow
    let signed: i128 = if neg { -value } else { value };
    if signed < i64::MIN as i128 || signed > i64::MAX as i128 {
        return s.parse::<i64>().ok();
    }
    Some(signed as i64)
}

impl PartialEq for ColumnValues {
    fn eq(&self, other: &Self) -> bool {
        // Consider equality based on ranges only (content layout). The backing
        // block identity is ignored for equality semantics in tests/containers.
        self.ranges == other.ranges
    }
}

impl Eq for ColumnValues {}
