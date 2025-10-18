use std::hash::{Hash, Hasher};
use std::path::Path;

/// Parse a segment identifier string into a u64.
/// Numeric-only scheme: accepts only plain numeric strings; otherwise falls back
/// to a stable 64-bit hash of the entire string (for tests using non-numeric ids).
pub fn parse_segment_id_u64(segment_id: &str) -> u64 {
    if let Ok(n) = segment_id.parse::<u64>() {
        return n;
    }
    // Stable 64-bit hash fallback (SipHasher via DefaultHasher)
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    segment_id.hash(&mut hasher);
    hasher.finish()
}

/// Compact shard id from Option<usize> into u16 (0 when None).
pub fn compact_shard_id(shard_id: Option<usize>) -> u16 {
    shard_id.map(|s| s as u16).unwrap_or(0)
}

/// Extract shard id from a base_dir (e.g., .../shard-3/42) if present.
pub fn parse_shard_id_from_base(base_dir: &Path) -> Option<usize> {
    base_dir
        .file_name()
        .and_then(|os| os.to_str())
        .and_then(|name| name.strip_prefix("shard-"))
        .and_then(|id| id.parse::<usize>().ok())
}
