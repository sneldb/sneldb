use rustc_hash::FxHasher;
use std::hash::{Hash, Hasher};

/// Deterministic 64-bit hash for persisted filter keys.
/// Uses fixed keys to guarantee stability across processes and runs.
pub fn stable_hash64<T: Hash>(value: &T) -> u64 {
    // FxHasher is deterministic across runs and fast. Changing this requires migration.
    let mut hasher = FxHasher::default();
    value.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::stable_hash64;

    #[test]
    fn stable_hash64_is_deterministic() {
        let a = stable_hash64(&"apple");
        let b = stable_hash64(&"apple");
        assert_eq!(a, b);
        // Different input hashes differently
        assert_ne!(a, stable_hash64(&"banana"));
    }
}
