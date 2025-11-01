use super::FlowMetrics;

#[test]
fn metrics_track_send_receive_and_backpressure() {
    let metrics = FlowMetrics::new();

    assert_eq!(metrics.total_sent_batches(), 0);
    assert_eq!(metrics.pending_batches(), 0);

    metrics.on_send_success(10);
    assert_eq!(metrics.total_sent_batches(), 1);
    assert_eq!(metrics.total_sent_rows(), 10);
    assert_eq!(metrics.pending_batches(), 1);
    assert_eq!(metrics.peak_pending_batches(), 1);

    metrics.record_backpressure();
    assert_eq!(metrics.backpressure_events(), 1);

    metrics.on_receive(7);
    assert_eq!(metrics.total_received_batches(), 1);
    assert_eq!(metrics.total_received_rows(), 7);
    assert_eq!(metrics.pending_batches(), 0);
}
