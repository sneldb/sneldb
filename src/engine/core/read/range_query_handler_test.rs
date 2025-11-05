use crate::command::types::CompareOp;
use crate::engine::core::RangeQueryHandler;
use crate::engine::types::ScalarValue;
use crate::test_helpers::factories::FieldXorFilterFactory;
use serde_json::json;

#[test]
fn handles_range_queries_and_returns_expected_zones() {
    let filter = FieldXorFilterFactory::new().build();
    let segment_id = "00001".to_string();
    let handler = RangeQueryHandler::new(filter, segment_id.clone());

    // Perform GT query
    let gt_result = handler.handle_range_query(&ScalarValue::from(json!(123)), &CompareOp::Gt);
    assert!(gt_result.is_some());
    let gt_zones = gt_result.unwrap();
    assert!(!gt_zones.is_empty());
    for zone in &gt_zones {
        assert_eq!(zone.segment_id, segment_id);
    }

    // Perform LTE query
    let lte_result = handler.handle_range_query(&ScalarValue::from(json!(50)), &CompareOp::Lte);
    assert!(lte_result.is_some());
    let lte_zones = lte_result.unwrap();
    assert!(!lte_zones.is_empty());
    for zone in &lte_zones {
        assert_eq!(zone.segment_id, segment_id);
    }

    // Ensure unsupported EQ returns None
    let eq_result = handler.handle_range_query(&ScalarValue::from(json!(42)), &CompareOp::Eq);
    assert!(eq_result.is_none());
}
