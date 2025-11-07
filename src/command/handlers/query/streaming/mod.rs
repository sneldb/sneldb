mod response_writer;

#[cfg(test)]
mod response_writer_test;

pub use response_writer::QueryResponseWriter;

pub struct QueryStreamingConfig;

impl QueryStreamingConfig {
    pub fn enabled() -> bool {
        crate::shared::config::CONFIG
            .query
            .as_ref()
            .and_then(|cfg| cfg.streaming_enabled)
            .unwrap_or(false)
    }
}
