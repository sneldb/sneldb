use crate::shared::response::types::Response;

/// A trait that defines how to serialize a `Response` for a given transport.
pub trait Renderer: Send + Sync {
    fn render(&self, response: &Response) -> Vec<u8>;
}
