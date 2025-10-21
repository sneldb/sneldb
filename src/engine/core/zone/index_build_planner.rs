use super::index_build_policy::IndexBuildPolicy;
use crate::engine::core::read::catalog::{IndexKind, SegmentIndexCatalog};
use crate::engine::schema::registry::MiniSchema;
use crate::engine::schema::types::FieldType;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct BuildPlan {
    pub per_field: HashMap<String, IndexKind>,
    pub global: IndexKind,
}

impl BuildPlan {
    pub fn new() -> Self {
        Self {
            per_field: HashMap::new(),
            global: IndexKind::from_bits_truncate(0),
        }
    }
}

pub struct IndexBuildPlanner<'a> {
    pub uid: &'a str,
    pub segment_id: &'a str,
    pub schema: &'a MiniSchema,
    pub policy: IndexBuildPolicy,
}

impl<'a> IndexBuildPlanner<'a> {
    pub fn new(
        uid: &'a str,
        segment_id: &'a str,
        schema: &'a MiniSchema,
        policy: IndexBuildPolicy,
    ) -> Self {
        Self {
            uid,
            segment_id,
            schema,
            policy,
        }
    }

    pub fn plan(&self) -> BuildPlan {
        let mut plan = BuildPlan::new();

        // Core fields always present
        let core_fields: [(&str, FieldType); 3] = [
            ("event_type", FieldType::String),
            ("context_id", FieldType::String),
            ("timestamp", FieldType::Timestamp),
        ];
        for (name, ty) in core_fields {
            let cat = IndexBuildPolicy::categorize(name, &ty);
            let kinds = self.policy.kinds_for_category(cat);
            plan.per_field.insert(name.to_string(), kinds);
        }

        for (name, ty) in &self.schema.fields {
            let cat = IndexBuildPolicy::categorize(name, ty);
            let kinds = self.policy.kinds_for_category(cat);
            plan.per_field.insert(name.clone(), kinds);
        }

        // Global kinds
        if self.policy.enable_rlte_for_primitives {
            plan.global |= IndexKind::RLTE;
        }
        plan.global |= IndexKind::ZONE_INDEX;

        plan
    }

    pub fn to_catalog(&self, plan: &BuildPlan) -> SegmentIndexCatalog {
        let mut catalog =
            SegmentIndexCatalog::new(self.uid.to_string(), self.segment_id.to_string());
        for (field, kinds) in &plan.per_field {
            catalog.set_field_kind(field, *kinds);
        }
        catalog.global_kinds = plan.global;
        catalog
    }
}
