use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RetentionPolicy {
    pub max_rows: Option<u64>,
    pub max_age_seconds: Option<u64>,
}

impl RetentionPolicy {
    pub fn keep_all() -> Self {
        Self {
            max_rows: None,
            max_age_seconds: None,
        }
    }

    pub fn is_noop(&self) -> bool {
        self.max_rows.is_none() && self.max_age_seconds.is_none()
    }
}
