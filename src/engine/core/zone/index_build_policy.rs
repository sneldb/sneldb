use crate::engine::core::read::catalog::IndexKind;
use crate::engine::schema::types::FieldType;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldCategory {
    Temporal,
    Enum,
    Context,
    EventType,
    Primitive,
}

#[derive(Debug, Clone)]
pub struct IndexBuildPolicy {
    pub enable_rlte_for_primitives: bool,
}

impl Default for IndexBuildPolicy {
    fn default() -> Self {
        Self {
            enable_rlte_for_primitives: true,
        }
    }
}

impl IndexBuildPolicy {
    pub fn categorize(field_name: &str, ty: &FieldType) -> FieldCategory {
        if field_name == "timestamp" {
            return FieldCategory::Temporal;
        }
        match ty {
            FieldType::Timestamp | FieldType::Date => FieldCategory::Temporal,
            FieldType::Optional(inner)
                if matches!(**inner, FieldType::Timestamp | FieldType::Date) =>
            {
                FieldCategory::Temporal
            }
            FieldType::Enum(_) => FieldCategory::Enum,
            _ => match field_name {
                "context_id" => FieldCategory::Context,
                "event_type" => FieldCategory::EventType,
                _ => FieldCategory::Primitive,
            },
        }
    }

    pub fn kinds_for_category(&self, cat: FieldCategory) -> IndexKind {
        match cat {
            FieldCategory::Temporal => {
                IndexKind::TS_CALENDAR
                    | IndexKind::TS_ZTI
                    | IndexKind::FIELD_CALENDAR
                    | IndexKind::FIELD_ZTI
            }
            FieldCategory::Enum => IndexKind::ENUM_BITMAP,
            FieldCategory::Context | FieldCategory::EventType => {
                IndexKind::XOR_FIELD_FILTER | IndexKind::ZONE_XOR_INDEX
            }
            FieldCategory::Primitive => {
                let base =
                    IndexKind::ZONE_SURF | IndexKind::ZONE_XOR_INDEX | IndexKind::XOR_FIELD_FILTER;
                if self.enable_rlte_for_primitives {
                    base | IndexKind::RLTE
                } else {
                    base
                }
            }
        }
    }
}
