#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ZoneIndexKey {
    pub segment_id: String,
    pub uid: String,
}

impl ZoneIndexKey {
    pub fn new(segment_id: impl Into<String>, uid: impl Into<String>) -> Self {
        Self {
            segment_id: segment_id.into(),
            uid: uid.into(),
        }
    }
}
