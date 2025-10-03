use super::ident_intern::{intern_field, intern_uid};
use super::seg_id::{compact_shard_id, parse_segment_id_u64};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ZoneSurfCacheKey {
    pub shard_id: u16,
    pub segment_id: u64,
    pub uid_id: u32,
    pub field_id: u32,
}

impl ZoneSurfCacheKey {
    #[inline]
    pub fn from_context(
        shard_id_opt: Option<usize>,
        segment_id_str: &str,
        uid: &str,
        field: &str,
    ) -> Self {
        Self {
            shard_id: compact_shard_id(shard_id_opt),
            segment_id: parse_segment_id_u64(segment_id_str),
            uid_id: intern_uid(uid),
            field_id: intern_field(field),
        }
    }
}
