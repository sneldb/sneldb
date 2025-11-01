use std::sync::Arc;

use super::{BatchPool, FlowContext, FlowMetrics, FlowTelemetry};

#[test]
fn context_exposes_configuration() {
    let metrics = FlowMetrics::new();
    let pool = BatchPool::new(16).expect("pool builds");
    let telemetry = FlowTelemetry {
        pipeline_name: Some("shard-1".into()),
        operator_count: Some(5),
    };

    let tmp_dir = tempfile::tempdir().expect("tmp dir");
    let spill_path = tmp_dir.path().join("spill");

    let ctx = FlowContext::new(
        16,
        pool.clone(),
        Arc::clone(&metrics),
        Some(&spill_path),
        telemetry.clone(),
    );

    assert_eq!(ctx.batch_size(), 16);
    assert_eq!(ctx.pool().batch_size(), 16);
    assert_eq!(ctx.telemetry().pipeline_name, telemetry.pipeline_name);
    assert_eq!(ctx.telemetry().operator_count, telemetry.operator_count);
    assert_eq!(ctx.metrics().total_sent_batches(), 0);
    assert_eq!(ctx.spill_dir().unwrap(), spill_path.as_path());
}
