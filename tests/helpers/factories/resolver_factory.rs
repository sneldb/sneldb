use crate::engine::core::UidResolver;
use std::collections::HashMap;

pub struct ResolverFactory {
    map: HashMap<String, String>,
}

impl ResolverFactory {
    pub fn new() -> Self {
        let mut map = HashMap::new();
        map.insert("test_event".into(), "uid-test".into());
        Self { map }
    }

    pub fn with(mut self, event_type: &str, uid: &str) -> Self {
        self.map.insert(event_type.to_string(), uid.to_string());
        self
    }

    pub fn create(self) -> UidResolver {
        UidResolver::from_map(self.map)
    }
}
