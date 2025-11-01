use super::operator::FlowOperatorError;

#[test]
fn operator_error_reports_display() {
    let custom = FlowOperatorError::operator("projection failed");
    assert!(matches!(custom, FlowOperatorError::Operator(_)));
    assert_eq!(format!("{}", custom), "operator error: projection failed");

    let closed = FlowOperatorError::ChannelClosed;
    assert_eq!(format!("{}", closed), "downstream channel closed");
}
