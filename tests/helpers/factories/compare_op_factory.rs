use crate::command::types::CompareOp;

pub struct CompareOpFactory {
    op: CompareOp,
}

impl CompareOpFactory {
    pub fn new() -> Self {
        Self {
            op: CompareOp::Eq, // default
        }
    }

    pub fn with_op(mut self, op: CompareOp) -> Self {
        self.op = op;
        self
    }

    pub fn create(self) -> CompareOp {
        self.op
    }

    pub fn all() -> Vec<CompareOp> {
        vec![
            CompareOp::Eq,
            CompareOp::Neq,
            CompareOp::Gt,
            CompareOp::Gte,
            CompareOp::Lt,
            CompareOp::Lte,
        ]
    }
}
