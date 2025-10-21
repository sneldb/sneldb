use crate::engine::core::filter::filter_plan::FilterPlan;
use crate::engine::core::read::catalog::{IndexKind, IndexRegistry};
use crate::engine::core::read::index_strategy::IndexStrategy;
use crate::engine::schema::registry::SchemaRegistry;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct IndexPlanner<'a> {
    pub registry: &'a Arc<RwLock<SchemaRegistry>>,
    pub index_registry: &'a IndexRegistry,
    pub event_type_uid: Option<String>,
}

impl<'a> IndexPlanner<'a> {
    pub fn new(
        registry: &'a Arc<RwLock<SchemaRegistry>>,
        index_registry: &'a IndexRegistry,
        event_type_uid: Option<String>,
    ) -> Self {
        Self {
            registry,
            index_registry,
            event_type_uid,
        }
    }

    pub async fn choose(&self, plan: &FilterPlan, segment_id: &str) -> IndexStrategy {
        let field = plan.column.clone();
        // If no catalog for this segment, avoid choosing any index to prevent fs probing
        if !self.index_registry.has_catalog(segment_id) {
            return IndexStrategy::FullScan;
        }
        let kinds = self.index_registry.available_for(segment_id, &field);
        // Temporal
        let is_temporal = {
            if field == "timestamp" {
                true
            } else {
                if let Some(uid) = &self.event_type_uid {
                    self.registry
                        .read()
                        .await
                        .get_schema_by_uid(uid)
                        .and_then(|s| s.field_type(&field).cloned())
                        .map(|ft| match ft {
                            crate::engine::schema::FieldType::Timestamp
                            | crate::engine::schema::FieldType::Date => true,
                            crate::engine::schema::FieldType::Optional(inner) => matches!(
                                *inner,
                                crate::engine::schema::FieldType::Timestamp
                                    | crate::engine::schema::FieldType::Date
                            ),
                            _ => false,
                        })
                        .unwrap_or(false)
                } else {
                    false
                }
            }
        };
        if is_temporal {
            if matches!(plan.operation, Some(crate::command::types::CompareOp::Eq)) {
                return IndexStrategy::TemporalEq { field };
            } else {
                return IndexStrategy::TemporalRange { field };
            }
        }

        // Enum
        let is_enum = if let Some(uid) = &self.event_type_uid {
            self.registry
                .read()
                .await
                .is_enum_field_by_uid(uid, &plan.column)
        } else {
            false
        };
        if is_enum && kinds.contains(IndexKind::ENUM_BITMAP) {
            return IndexStrategy::EnumBitmap { field };
        }

        // Range
        if let Some(op) = &plan.operation {
            use crate::command::types::CompareOp::*;
            if matches!(op, Gt | Gte | Lt | Lte) && kinds.contains(IndexKind::ZONE_SURF) {
                return IndexStrategy::ZoneSuRF { field };
            }
        }

        // Equality
        if kinds.contains(IndexKind::ZONE_XOR_INDEX) {
            return IndexStrategy::ZoneXorIndex { field };
        }
        if kinds.contains(IndexKind::XOR_FIELD_FILTER) {
            return IndexStrategy::XorPresence { field };
        }

        IndexStrategy::FullScan
    }
}
