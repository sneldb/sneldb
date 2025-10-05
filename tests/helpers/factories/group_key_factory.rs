use crate::engine::core::read::aggregate::partial::GroupKey;

pub struct GroupKeyFactory {
    bucket: Option<u64>,
    groups: Vec<String>,
}

impl GroupKeyFactory {
    pub fn new() -> Self {
        Self {
            bucket: None,
            groups: Vec::new(),
        }
    }

    pub fn with_bucket(mut self, bucket: u64) -> Self {
        self.bucket = Some(bucket);
        self
    }

    pub fn without_bucket(mut self) -> Self {
        self.bucket = None;
        self
    }

    pub fn with_groups(mut self, groups: &[&str]) -> Self {
        self.groups = groups.iter().map(|s| s.to_string()).collect();
        self
    }

    pub fn add_group(mut self, group: &str) -> Self {
        self.groups.push(group.to_string());
        self
    }

    pub fn us_in_month(bucket: u64) -> GroupKey {
        Self::new()
            .with_bucket(bucket)
            .with_groups(&["US"])
            .create()
    }

    pub fn create(self) -> GroupKey {
        GroupKey {
            bucket: self.bucket,
            groups: self.groups,
        }
    }
}
