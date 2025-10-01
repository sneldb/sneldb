#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ZoneSurfCacheKey {
    pub segment_id: String,
    pub uid: String,
    pub field: String,
}

impl ZoneSurfCacheKey {
    pub fn new(
        segment_id: impl Into<String>,
        uid: impl Into<String>,
        field: impl Into<String>,
    ) -> Self {
        Self {
            segment_id: segment_id.into(),
            uid: uid.into(),
            field: field.into(),
        }
    }
}

