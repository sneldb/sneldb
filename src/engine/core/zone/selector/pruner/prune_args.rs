use crate::command::types::CompareOp;

pub struct PruneArgs<'a> {
    pub segment_id: &'a str,
    pub uid: &'a str,
    pub column: &'a str,
    pub value: Option<&'a serde_json::Value>,
    pub op: Option<&'a CompareOp>,
}
