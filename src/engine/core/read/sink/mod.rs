mod aggregate;
mod event_sink;
mod result_sink;

pub use aggregate::AggregateSink;
pub use event_sink::EventSink;
pub use result_sink::ResultSink;

#[cfg(test)]
mod event_sink_test;
