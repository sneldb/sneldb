use crate::command::types::CompareOp;
use crate::engine::types::ScalarValue;

pub struct PruneArgs<'a> {
    pub segment_id: &'a str,
    pub uid: &'a str,
    pub column: &'a str,
    pub value: Option<&'a ScalarValue>,
    pub op: Option<&'a CompareOp>,
}
