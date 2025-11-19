mod context;
mod dispatch;
mod handler;
pub mod merge;
mod orchestrator;
mod planner;
mod streaming;

#[cfg(test)]
mod context_test;

pub use handler::QueryCommandHandler;
pub use orchestrator::QueryExecutionPipeline;

// Re-export streaming types for use by comparison handler
pub use streaming::QueryResponseWriter;
