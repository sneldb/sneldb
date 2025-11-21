use crate::command::types::CompareOp;
use crate::engine::core::filter::filter_group::FilterGroup;
use crate::engine::core::read::catalog::{IndexKind, IndexRegistry};
use crate::engine::core::read::event_scope::EventScope;
use crate::engine::core::read::index_strategy::IndexStrategy;
use crate::engine::schema::{FieldType, registry::SchemaRegistry};
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct IndexPlanner<'a> {
    pub registry: &'a Arc<RwLock<SchemaRegistry>>,
    pub index_registry: &'a IndexRegistry,
    pub event_scope: &'a EventScope,
}

impl<'a> IndexPlanner<'a> {
    pub fn new(
        registry: &'a Arc<RwLock<SchemaRegistry>>,
        index_registry: &'a IndexRegistry,
        event_scope: &'a EventScope,
    ) -> Self {
        Self {
            registry,
            index_registry,
            event_scope,
        }
    }

    pub async fn choose(&self, plan: &FilterGroup, segment_id: &str) -> IndexStrategy {
        // Only single filters can have index strategies
        let (column, operation, index_strategy) = match plan {
            FilterGroup::Filter {
                column,
                operation,
                index_strategy,
                ..
            } => (column.clone(), operation.clone(), index_strategy.clone()),
            _ => return IndexStrategy::FullScan, // Logical groups use FullScan
        };

        // If the filter already has an index strategy set (e.g., FullScan for OR conditions),
        // respect that choice instead of choosing a new one.
        if let Some(ref strategy) = index_strategy {
            return strategy.clone();
        }
        let field = column;
        if self.event_scope.is_wildcard() {
            return IndexStrategy::FullScan;
        }
        let Some(uid_ref) = self.event_scope.primary_uid() else {
            return IndexStrategy::FullScan;
        };
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
                self.registry
                    .read()
                    .await
                    .get_schema_by_uid(uid_ref)
                    .and_then(|s| s.field_type(&field).cloned())
                    .map(|ft| match ft {
                        FieldType::Timestamp | FieldType::Date => true,
                        FieldType::Optional(inner) => {
                            matches!(*inner, FieldType::Timestamp | FieldType::Date)
                        }
                        _ => false,
                    })
                    .unwrap_or(false)
            }
        };
        if is_temporal {
            // IN operations require checking multiple values, which temporal range indexes can't efficiently handle.
            // Use FullScan and let the condition evaluator filter events.
            if matches!(operation, Some(CompareOp::In)) {
                return IndexStrategy::FullScan;
            }
            if matches!(operation, Some(CompareOp::Eq)) {
                return IndexStrategy::TemporalEq { field };
            } else {
                return IndexStrategy::TemporalRange { field };
            }
        }

        // Enum
        let is_enum = self
            .registry
            .read()
            .await
            .is_enum_field_by_uid(uid_ref, &field);
        if is_enum && kinds.contains(IndexKind::ENUM_BITMAP) {
            return IndexStrategy::EnumBitmap { field };
        }

        // Range
        if let Some(op) = &operation {
            use CompareOp::*;
            if matches!(op, Gt | Gte | Lt | Lte) && kinds.contains(IndexKind::ZONE_SURF) {
                return IndexStrategy::ZoneSuRF { field };
            }
            // IN operations require checking multiple values, which XOR filters can't efficiently handle.
            // Use FullScan and let the condition evaluator filter events.
            if matches!(op, In) {
                return IndexStrategy::FullScan;
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
