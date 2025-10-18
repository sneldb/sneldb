use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};

/// Span of IDs per compaction level. L0: [0, LEVEL_SPAN), L1: [LEVEL_SPAN, 2*LEVEL_SPAN), ...
pub const LEVEL_SPAN: u32 = 10_000;
/// Zero-padding width for segment directory names
pub const SEGMENT_ID_PAD: usize = 5;

/// Canonical numeric segment identifier.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SegmentId {
    pub id: u32,
}

impl SegmentId {
    #[inline]
    pub fn new(id: u32) -> Self {
        Self { id }
    }

    /// Level derived from ID range.
    #[inline]
    pub fn level(&self) -> u32 {
        self.id / LEVEL_SPAN
    }

    /// Zero-padded directory name for on-disk layout.
    #[inline]
    pub fn dir_name(&self) -> String {
        format!("{:0width$}", self.id, width = SEGMENT_ID_PAD)
    }

    /// Join a base directory with this segment's directory name.
    #[inline]
    pub fn join_dir(&self, base: &Path) -> PathBuf {
        base.join(self.dir_name())
    }

    /// Parse a zero-padded numeric directory name into SegmentId.
    #[inline]
    pub fn from_str(s: &str) -> Option<Self> {
        s.parse::<u32>().ok().map(Self::new)
    }
}

impl From<u32> for SegmentId {
    #[inline]
    fn from(value: u32) -> Self {
        Self::new(value)
    }
}

impl Display for SegmentId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.dir_name())
    }
}
