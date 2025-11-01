use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;

use super::{BatchError, BatchReceiver, BatchSender, FlowContext};

#[async_trait]
pub trait FlowSource: Send {
    async fn run(self, output: BatchSender, ctx: Arc<FlowContext>)
    -> Result<(), FlowOperatorError>;
}

#[async_trait]
pub trait FlowOperator: Send {
    async fn run(
        self,
        input: BatchReceiver,
        output: BatchSender,
        ctx: Arc<FlowContext>,
    ) -> Result<(), FlowOperatorError>;
}

#[derive(Debug)]
pub enum FlowOperatorError {
    ChannelClosed,
    Operator(String),
    Batch(String),
}

impl FlowOperatorError {
    pub fn operator<S: Into<String>>(message: S) -> Self {
        Self::Operator(message.into())
    }
}

impl fmt::Display for FlowOperatorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FlowOperatorError::ChannelClosed => write!(f, "downstream channel closed"),
            FlowOperatorError::Operator(message) => write!(f, "operator error: {}", message),
            FlowOperatorError::Batch(message) => write!(f, "batch error: {}", message),
        }
    }
}

impl std::error::Error for FlowOperatorError {}

impl From<BatchError> for FlowOperatorError {
    fn from(value: BatchError) -> Self {
        FlowOperatorError::Batch(value.to_string())
    }
}
