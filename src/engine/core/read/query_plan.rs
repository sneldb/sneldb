use crate::command::types::{Command, CompareOp, Expr, OrderSpec};
use crate::engine::core::InflightSegments;
use crate::engine::core::filter::filter_group::FilterGroup;
use crate::engine::core::filter::filter_group_builder::FilterGroupBuilder;
use crate::engine::core::read::aggregate::plan::AggregatePlan;
use crate::engine::core::read::cache::GlobalIndexCatalogCache;
use crate::engine::core::read::catalog::IndexRegistry;
use crate::engine::core::read::event_scope::EventScope;
use crate::engine::core::read::index_planner::IndexPlanner;
use crate::engine::core::read::projection::ProjectionPlanner;
use crate::engine::schema::registry::SchemaRegistry;
use crate::engine::types::ScalarValue;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info};

/// The main query plan structure that holds all query information
#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub command: Command,
    pub metadata: HashMap<String, String>,
    pub filter_groups: Vec<FilterGroup>, // Individual filters extracted from filter_group tree
    pub filter_group: Option<FilterGroup>, // Logical tree for WHERE clause
    pub registry: Arc<RwLock<SchemaRegistry>>,
    pub segment_base_dir: PathBuf,
    pub segment_ids: Arc<std::sync::RwLock<Vec<String>>>,
    pub aggregate_plan: Option<AggregatePlan>,
    pub index_registry: IndexRegistry,
    event_scope: EventScope,
    inflight_segments: Option<InflightSegments>,
}

impl QueryPlan {
    /// Creates a new query plan from a Query command and schema registry
    pub async fn new(
        command: Command,
        registry: &Arc<RwLock<SchemaRegistry>>,
        segment_base_dir: &Path,
        segment_ids: &Arc<std::sync::RwLock<Vec<String>>>,
        inflight_segments: Option<InflightSegments>,
    ) -> Option<Self> {
        match &command {
            Command::Query {
                where_clause,
                event_type,
                ..
            } => {
                let event_scope = {
                    let guard = registry.read().await;
                    EventScope::from_command(event_type, &guard)
                };
                let event_type_uid = match &event_scope {
                    EventScope::Specific { uid, .. } => uid.clone(),
                    EventScope::Wildcard { .. } => None,
                };
                // Build FilterGroup from WHERE clause to preserve logical structure
                let filter_group = where_clause
                    .as_ref()
                    .and_then(|expr| FilterGroupBuilder::build(expr, &event_type_uid));

                // Extract individual FilterGroups from FilterGroup tree or build all filters
                let mut filter_groups = if let Some(ref group) = filter_group {
                    group.extract_individual_filters()
                } else {
                    FilterGroupBuilder::build_all(&command, registry).await
                };

                if tracing::enabled!(tracing::Level::INFO) {
                    info!(
                        target: "sneldb::query_plan",
                        filters = filter_groups.len(),
                        has_filter_group = filter_group.is_some(),
                        "Built filter groups for query"
                    );
                }
                let aggregate_plan = AggregatePlan::from_command(&command);
                if aggregate_plan.is_some() {
                    if let Command::Query {
                        time_field, since, ..
                    } = &command
                    {
                        let tf = time_field.as_deref().unwrap_or("timestamp");
                        // Remove implicit 'since' time filter for aggregations
                        filter_groups.retain(|fg| match fg {
                            FilterGroup::Filter {
                                column,
                                operation,
                                value,
                                ..
                            } => {
                                if column != tf {
                                    return true;
                                }
                                match (operation, value) {
                                    (Some(CompareOp::Gte), Some(ScalarValue::Utf8(s)))
                                        if since.as_deref() == Some(s.as_str()) =>
                                    {
                                        false
                                    }
                                    _ => true,
                                }
                            }
                            _ => true,
                        });
                    }
                }
                let mut plan = Self {
                    command,
                    metadata: HashMap::new(),
                    filter_groups,
                    filter_group,
                    registry: Arc::clone(registry),
                    segment_base_dir: segment_base_dir.to_path_buf(),
                    segment_ids: Arc::clone(segment_ids),
                    aggregate_plan,
                    index_registry: IndexRegistry::new(),
                    event_scope,
                    inflight_segments,
                };
                // Preload catalogs for discovered segments (best-effort)
                if let Some(uid) = plan.event_type_uid().await {
                    let segs = plan
                        .segment_ids
                        .read()
                        .unwrap_or_else(|p| p.into_inner())
                        .clone();
                    let _ = GlobalIndexCatalogCache::instance();
                    plan.index_registry
                        .load_for_segments(&plan.segment_base_dir, &segs, &uid);
                    // Assign index strategies using a segment that has a loaded catalog as representative
                    // Prefer a segment that has a catalog entry
                    let rep_seg = segs
                        .iter()
                        .find(|s| plan.index_registry.available_global(s).bits() != 0)
                        .or_else(|| segs.first());
                    if let Some(rep) = rep_seg {
                        let scope_clone = plan.event_scope.clone();
                        let planner =
                            IndexPlanner::new(&plan.registry, &plan.index_registry, &scope_clone);
                        let mut filters = std::mem::take(&mut plan.filter_groups);
                        for fg in &mut filters {
                            let strat = planner.choose(fg, rep).await;
                            if let Some(strategy_mut) = fg.index_strategy_mut() {
                                *strategy_mut = Some(strat);
                            }
                        }
                        if let Some(ref mut filter_group) = plan.filter_group {
                            filter_group.sync_index_strategies_from(&filters);
                        }
                        plan.filter_groups = filters;
                    }
                }
                Some(plan)
            }
            _ => {
                error!(target: "sneldb::query_plan", "Expected a Query command, got something else");
                None
            }
        }
    }

    pub fn set_metadata(&mut self, key: String, value: String) {
        self.metadata.insert(key, value);
    }

    pub fn event_type(&self) -> &str {
        if let Command::Query { event_type, .. } = &self.command {
            event_type
        } else {
            error!(target: "sneldb::query_plan", "event_type() called on non-Query command");
            ""
        }
    }

    pub fn context_id(&self) -> Option<&str> {
        if let Command::Query { context_id, .. } = &self.command {
            context_id.as_deref()
        } else {
            error!(target: "sneldb::query_plan", "context_id() called on non-Query command");
            None
        }
    }

    pub fn limit(&self) -> Option<usize> {
        if let Command::Query { limit, .. } = &self.command {
            limit.map(|v| v as usize)
        } else {
            None
        }
    }

    pub fn offset(&self) -> Option<usize> {
        if let Command::Query { offset, .. } = &self.command {
            offset.map(|v| v as usize)
        } else {
            None
        }
    }

    pub fn order_by(&self) -> Option<&OrderSpec> {
        if let Command::Query { order_by, .. } = &self.command {
            order_by.as_ref()
        } else {
            None
        }
    }

    /// Returns the OrderSpec if ordering should happen at the shard level (before aggregation).
    /// Returns None if ordering should be deferred to the merger (after aggregation).
    ///
    /// For aggregate queries, ordering must happen after aggregation because:
    /// 1. Metric columns don't exist until after aggregation
    /// 2. Partial aggregates from multiple shards must be merged first
    /// 3. Final ordering happens on merged results in AggregateStreamMerger
    ///
    /// For non-aggregate queries, ordering can happen at the shard level.
    pub fn order_by_for_shard_level(&self) -> Option<&OrderSpec> {
        // If there's an aggregate plan, ordering must happen after aggregation
        if self.aggregate_plan.is_some() {
            return None;
        }
        // For non-aggregate queries, ordering can happen at shard level
        self.order_by()
    }

    pub fn context_id_plan(&self) -> Option<&FilterGroup> {
        self.filter_groups.iter().find(|plan| plan.is_context_id())
    }

    pub fn event_type_plan(&self) -> Option<&FilterGroup> {
        self.filter_groups.iter().find(|plan| plan.is_event_type())
    }

    pub fn where_clause(&self) -> Option<&Expr> {
        if let Command::Query { where_clause, .. } = &self.command {
            where_clause.as_ref()
        } else {
            error!(target: "sneldb::query_plan", "where_clause() called on non-Query command");
            None
        }
    }

    /// Delegates to the ProjectionPlanner to compute required columns.
    pub async fn columns_to_load(&self) -> Vec<String> {
        ProjectionPlanner::new(self).columns_to_load().await
    }

    pub async fn event_type_uid(&self) -> Option<String> {
        match &self.event_scope {
            EventScope::Specific { uid: Some(uid), .. } => Some(uid.clone()),
            _ => None,
        }
    }

    pub async fn build(command: &Command, registry: Arc<RwLock<SchemaRegistry>>) -> Self {
        let event_scope = match command {
            Command::Query { event_type, .. } => {
                let guard = registry.read().await;
                EventScope::from_command(event_type, &guard)
            }
            _ => EventScope::Specific {
                event_type: String::new(),
                uid: None,
            },
        };

        // Build FilterGroup from WHERE clause if present
        let filter_group = if let Command::Query { where_clause, .. } = command {
            let event_type_uid = match &event_scope {
                EventScope::Specific { uid, .. } => uid.clone(),
                EventScope::Wildcard { .. } => None,
            };
            where_clause
                .as_ref()
                .and_then(|expr| FilterGroupBuilder::build(expr, &event_type_uid))
        } else {
            None
        };

        // Extract individual FilterGroups from FilterGroup tree or build all filters
        let filter_groups = if let Some(ref group) = filter_group {
            group.extract_individual_filters()
        } else {
            FilterGroupBuilder::build_all(command, &registry).await
        };

        let aggregate_plan = AggregatePlan::from_command(command);
        if tracing::enabled!(tracing::Level::INFO) {
            info!(
                target: "sneldb::query_plan",
                "Building inline query plan with {} filters",
                filter_groups.len()
            );
        }
        Self {
            command: command.clone(),
            registry,
            filter_groups,
            filter_group,
            metadata: HashMap::new(),
            segment_base_dir: PathBuf::new(),
            segment_ids: Arc::new(std::sync::RwLock::new(Vec::new())),
            aggregate_plan,
            index_registry: IndexRegistry::new(),
            event_scope,
            inflight_segments: None,
        }
    }

    pub fn event_scope(&self) -> &EventScope {
        &self.event_scope
    }

    pub fn set_inflight_segments(&mut self, tracker: Option<InflightSegments>) {
        self.inflight_segments = tracker;
    }

    pub fn inflight_segments(&self) -> Option<&InflightSegments> {
        self.inflight_segments.as_ref()
    }

    pub fn is_segment_inflight(&self, segment_id: &str) -> bool {
        self.inflight_segments
            .as_ref()
            .map(|tracker| tracker.contains(segment_id))
            .unwrap_or(false)
    }

    /// Returns true if the given segment is expected to contain data for the provided uid.
    pub fn segment_maybe_contains_uid(&self, segment_id: &str, uid: &str) -> bool {
        if self.index_registry.has_catalog(segment_id) {
            return true;
        }

        if self.is_segment_inflight(segment_id) {
            return true;
        }

        let zones_path = self
            .segment_base_dir
            .join(segment_id)
            .join(format!("{}.zones", uid));
        fs::metadata(zones_path).is_ok()
    }
}
