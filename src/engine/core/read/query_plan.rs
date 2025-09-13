use crate::command::types::{Command, Expr};
use crate::engine::core::FilterPlan;
use crate::engine::schema::registry::SchemaRegistry;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

/// The main query plan structure that holds all query information
#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub command: Command,
    pub metadata: HashMap<String, String>,
    pub filter_plans: Vec<FilterPlan>,
    pub registry: Arc<RwLock<SchemaRegistry>>,
    pub segment_base_dir: PathBuf,
    pub segment_ids: Arc<std::sync::RwLock<Vec<String>>>,
}

impl QueryPlan {
    /// Creates a new query plan from a Query command and schema registry
    pub async fn new(
        command: Command,
        registry: &Arc<RwLock<SchemaRegistry>>,
        segment_base_dir: &Path,
        segment_ids: &Arc<std::sync::RwLock<Vec<String>>>,
    ) -> Option<Self> {
        match &command {
            Command::Query { .. } => {
                let filter_plans = FilterPlan::build_all(&command, registry).await;
                info!(
                    target: "sneldb::query_plan",
                    filters = ?filter_plans,
                    "Built filter plans for query"
                );
                Some(Self {
                    command,
                    metadata: HashMap::new(),
                    filter_plans,
                    registry: Arc::clone(registry),
                    segment_base_dir: segment_base_dir.to_path_buf(),
                    segment_ids: Arc::clone(segment_ids),
                })
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

    pub fn context_id_plan(&self) -> Option<&FilterPlan> {
        self.filter_plans.iter().find(|plan| plan.is_context_id())
    }

    pub fn event_type_plan(&self) -> Option<&FilterPlan> {
        self.filter_plans.iter().find(|plan| plan.is_event_type())
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
        crate::engine::core::read::projection::ProjectionPlanner::new(self)
            .columns_to_load()
            .await
    }

    pub async fn event_type_uid(&self) -> Option<String> {
        let guard = self.registry.read().await;
        let uid = guard.get_uid(self.event_type());
        if uid.is_none() {
            warn!(
                target: "sneldb::query_plan",
                event_type = %self.event_type(),
                "UID not found in schema registry"
            );
        }
        uid
    }

    pub async fn build(command: &Command, registry: Arc<RwLock<SchemaRegistry>>) -> Self {
        let filter_plans = FilterPlan::build_all(command, &registry).await;
        info!(
            target: "sneldb::query_plan",
            "Building inline query plan with {} filters",
            filter_plans.len()
        );
        Self {
            command: command.clone(),
            registry,
            filter_plans,
            metadata: HashMap::new(),
            segment_base_dir: PathBuf::new(),
            segment_ids: Arc::new(std::sync::RwLock::new(Vec::new())),
        }
    }
}
