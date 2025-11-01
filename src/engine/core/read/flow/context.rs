use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::{BatchPool, FlowMetrics};

/// Lightweight metadata captured when constructing a flow, used for observability
/// and debugging of streaming pipelines.
#[derive(Debug, Clone, Default)]
pub struct FlowTelemetry {
    pub pipeline_name: Option<String>,
    pub operator_count: Option<usize>,
}

/// Runtime configuration shared by all operators participating in a streaming
/// flow. Carries batch sizing, shared buffers, metrics collectors, and optional
/// spill locations.
#[derive(Debug, Clone)]
pub struct FlowContext {
    batch_size: usize,
    pool: BatchPool,
    metrics: Arc<FlowMetrics>,
    spill_dir: Option<PathBuf>,
    telemetry: FlowTelemetry,
}

impl FlowContext {
    pub fn new<P: AsRef<Path>>(
        batch_size: usize,
        pool: BatchPool,
        metrics: Arc<FlowMetrics>,
        spill_dir: Option<P>,
        telemetry: FlowTelemetry,
    ) -> Self {
        let spill_dir = spill_dir.map(|p| p.as_ref().to_path_buf());

        debug_assert_eq!(
            batch_size,
            pool.batch_size(),
            "flow context batch size should match pool"
        );

        Self {
            batch_size,
            pool,
            metrics,
            spill_dir,
            telemetry,
        }
    }

    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    pub fn pool(&self) -> &BatchPool {
        &self.pool
    }

    pub fn metrics(&self) -> &Arc<FlowMetrics> {
        &self.metrics
    }

    pub fn spill_dir(&self) -> Option<&Path> {
        self.spill_dir.as_deref()
    }

    pub fn telemetry(&self) -> &FlowTelemetry {
        &self.telemetry
    }
}
