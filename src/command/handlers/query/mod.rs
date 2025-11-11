mod context;
mod dispatch;
mod handler;
pub mod merge;
mod orchestrator;
mod planner;
mod response;
mod streaming;

#[cfg(test)]
mod context_test;
#[cfg(test)]
mod orchestrator_test;
#[cfg(test)]
mod response_test;

pub use handler::QueryCommandHandler;
pub use orchestrator::QueryExecutionPipeline;

// Re-export streaming types for use by comparison handler
pub use streaming::{QueryResponseWriter, QueryStreamingConfig};

#[cfg(test)]
pub use streaming::set_streaming_enabled;
