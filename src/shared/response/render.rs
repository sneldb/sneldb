use crate::engine::core::read::flow::ColumnBatch;
use crate::engine::types::ScalarValue;
use crate::shared::response::types::Response;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamingFormat {
    Json,
    Arrow,
}

/// A trait that defines how to serialize a `Response` for a given transport.
/// All rendering works directly with ScalarValue - no intermediate types.
pub trait Renderer: Send + Sync {
    /// Render a complete Response to bytes.
    fn render(&self, response: &Response) -> Vec<u8>;

    /// Get the streaming format this renderer produces.
    fn streaming_format(&self) -> StreamingFormat;

    /// Encode the schema frame for a streaming query response into the provided buffer.
    fn stream_schema(&self, _columns: &[(String, String)], _out: &mut Vec<u8>) {
        unreachable!("stream_schema called on renderer without support")
    }

    /// Encode a single row frame for a streaming query response into the provided buffer.
    fn stream_row(&self, _columns: &[&str], _values: &[ScalarValue], _out: &mut Vec<u8>) {
        unreachable!("stream_row called on renderer without support")
    }

    /// Encode a batch of rows for a streaming query response into the provided buffer.
    /// Each element in `batch` is a row represented as a slice of ScalarValues.
    /// This is more efficient than calling `stream_row` multiple times.
    fn stream_batch(&self, _columns: &[&str], _batch: &[Vec<ScalarValue>], _out: &mut Vec<u8>) {
        unreachable!("stream_batch called on renderer without support")
    }

    /// Encode a ColumnBatch directly (most efficient for Arrow format).
    fn stream_column_batch(&self, _batch: &ColumnBatch, _out: &mut Vec<u8>) {
        unreachable!("stream_column_batch called on renderer without support")
    }

    /// Encode the terminal frame for a streaming query response into the provided buffer.
    fn stream_end(&self, _row_count: usize, _out: &mut Vec<u8>) {
        unreachable!("stream_end called on renderer without support")
    }
}
