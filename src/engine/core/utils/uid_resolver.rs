use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::engine::core::ZonePlan;
use crate::engine::errors::StoreError;
use crate::engine::schema::SchemaRegistry;

#[derive(Debug, Clone)]
pub struct UidResolver {
    map: HashMap<String, String>,
}

impl UidResolver {
    pub async fn from_events(
        zone_plans: &[ZonePlan],
        registry: &Arc<RwLock<SchemaRegistry>>,
    ) -> Result<Self, StoreError> {
        let mut map = HashMap::new();
        let reg = registry.read().await;

        for zone in zone_plans {
            for event in &zone.events {
                let et = &event.event_type;
                if !map.contains_key(et) {
                    if let Some(uid) = reg.get_uid(et) {
                        map.insert(et.clone(), uid.clone());
                    } else {
                        return Err(StoreError::FlushFailed(format!(
                            "UID not found for event_type: {}",
                            et
                        )));
                    }
                }
            }
        }

        Ok(Self { map })
    }

    pub fn get(&self, event_type: &str) -> Option<&String> {
        self.map.get(event_type)
    }

    pub fn from_map(map: HashMap<String, String>) -> Self {
        Self { map }
    }
}
