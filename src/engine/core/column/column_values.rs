use std::sync::{Arc, OnceLock};

use crate::engine::core::read::cache::DecompressedBlock;

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
            for (start, len) in &self.ranges {
                let bytes = &self.block.bytes[*start..*start + *len];
                if std::str::from_utf8(bytes).is_err() {
                    return false;
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
                let v = self.get_str_at(i).and_then(|s| s.parse::<i64>().ok());
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
                let v = self.get_str_at(i).and_then(|s| s.parse::<i64>().ok());
                parsed.push(v);
            }
            Arc::new(parsed)
        });
    }
}

impl PartialEq for ColumnValues {
    fn eq(&self, other: &Self) -> bool {
        // Consider equality based on ranges only (content layout). The backing
        // block identity is ignored for equality semantics in tests/containers.
        self.ranges == other.ranges
    }
}

impl Eq for ColumnValues {}
