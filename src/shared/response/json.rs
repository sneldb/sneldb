use crate::shared::response::render::Renderer;
use crate::shared::response::types::{Response, ResponseBody};
use serde_json::{Value, json};

pub struct JsonRenderer;

impl Renderer for JsonRenderer {
    fn render(&self, response: &Response) -> Vec<u8> {
        let results = match &response.body {
            ResponseBody::Lines(lines) => lines.iter().map(|s| json!(s)).collect::<Vec<Value>>(),
            ResponseBody::JsonArray(values) => values.clone(),
        };

        let payload = json!({
            "count": response.count,
            "status": response.status.code(),
            "message": response.message,
            "results": results
        });

        let mut buf = serde_json::to_vec(&payload).unwrap_or_else(|_| {
            b"{\"status\":500,\"message\":\"Failed to serialize JSON\"}".to_vec()
        });

        buf.push(b'\n');
        buf
    }
}
