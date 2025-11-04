use crate::shared::response::types::Response;
use serde_json::Value;

/// A trait that defines how to serialize a `Response` for a given transport.
pub trait Renderer: Send + Sync {
    fn render(&self, response: &Response) -> Vec<u8>;

    /// Encode the schema frame for a streaming query response into the provided buffer.
    fn stream_schema(&self, columns: &[(String, String)], out: &mut Vec<u8>);

    /// Encode a single row frame for a streaming query response into the provided buffer.
    fn stream_row(&self, columns: &[&str], values: &[&Value], out: &mut Vec<u8>);

    /// Encode a batch of rows for a streaming query response into the provided buffer.
    /// Each element in `batch` is a row represented as a slice of column values.
    /// This is more efficient than calling `stream_row` multiple times.
    fn stream_batch(&self, columns: &[&str], batch: &[Vec<&Value>], out: &mut Vec<u8>);

    /// Encode the terminal frame for a streaming query response into the provided buffer.
    fn stream_end(&self, row_count: usize, out: &mut Vec<u8>);
}
