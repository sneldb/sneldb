use crate::command::types::Command;
use crate::engine::core::{InflightSegments, QueryPlan};
use crate::engine::schema::registry::SchemaRegistry;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock as StdRwLock};
use tokio::sync::RwLock;

/// Factory for building `QueryPlan` instances in tests.
#[derive(Clone)]
pub struct QueryPlanFactory {
    command: Option<Command>,
    registry: Option<Arc<RwLock<SchemaRegistry>>>,
    segment_base_dir: PathBuf,
    segment_ids: Arc<StdRwLock<Vec<String>>>,
    inflight_segments: Option<InflightSegments>,
}

impl QueryPlanFactory {
    pub fn new() -> Self {
        Self {
            command: None,
            registry: None,
            segment_base_dir: PathBuf::from("/tmp/SNELDB_query_test"),
            segment_ids: Arc::new(StdRwLock::new(vec!["seg1".into()])),
            inflight_segments: None,
        }
    }

    pub fn with_command(mut self, command: Command) -> Self {
        self.command = Some(command);
        self
    }

    pub fn with_registry(mut self, registry: Arc<RwLock<SchemaRegistry>>) -> Self {
        self.registry = Some(registry);
        self
    }

    pub fn with_segment_base_dir<P: AsRef<Path>>(mut self, dir: P) -> Self {
        self.segment_base_dir = dir.as_ref().to_path_buf();
        self
    }

    pub fn with_segment_ids(mut self, ids: Vec<String>) -> Self {
        self.segment_ids = Arc::new(StdRwLock::new(ids));
        self
    }

    pub fn with_inflight_segments(mut self, inflight: InflightSegments) -> Self {
        self.inflight_segments = Some(inflight);
        self
    }

    pub async fn create(self) -> QueryPlan {
        let command = self.command.expect("QueryPlanFactory: command is required");
        let registry = self
            .registry
            .expect("QueryPlanFactory: registry is required");

        QueryPlan::new(
            command,
            &registry,
            &self.segment_base_dir,
            &self.segment_ids,
            self.inflight_segments.clone(),
        )
        .await
        .expect("QueryPlan::new should return Some(QueryPlan)")
    }

    pub async fn build(self) -> QueryPlan {
        let command = self.command.expect("QueryPlanFactory: command is required");
        let registry = self
            .registry
            .expect("QueryPlanFactory: registry is required");

        QueryPlan::build(&command, registry).await
    }
}
