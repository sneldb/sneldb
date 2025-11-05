use crate::shared::response::types::Response;
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamingFormat {
    Json,
    Arrow,
}

/// A trait that defines how to serialize a `Response` for a given transport.
pub trait Renderer: Send + Sync {
    fn render(&self, response: &Response) -> Vec<u8>;

    fn streaming_format(&self) -> StreamingFormat;

    /// Encode the schema frame for a streaming query response into the provided buffer.
    fn stream_schema_json(&self, _columns: &[(String, String)], _out: &mut Vec<u8>) {
        unreachable!("stream_schema_json called on renderer without JSON support")
    }

    /// Encode a single row frame for a streaming query response into the provided buffer.
    fn stream_row_json(&self, _columns: &[&str], _values: &[&Value], _out: &mut Vec<u8>) {
        unreachable!("stream_row_json called on renderer without JSON support")
    }

    /// Encode a batch of rows for a streaming query response into the provided buffer.
    /// Each element in `batch` is a row represented as a slice of column values.
    /// This is more efficient than calling `stream_row` multiple times.
    fn stream_batch_json(&self, _columns: &[&str], _batch: &[Vec<&Value>], _out: &mut Vec<u8>) {
        unreachable!("stream_batch_json called on renderer without JSON support")
    }

    /// Encode the terminal frame for a streaming query response into the provided buffer.
    fn stream_end_json(&self, _row_count: usize, _out: &mut Vec<u8>) {
        unreachable!("stream_end_json called on renderer without JSON support")
    }
}
