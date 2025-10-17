use crate::shared::response::render::Renderer;
use crate::shared::response::types::{Response, ResponseBody};
use serde::Serialize;
use serde_json::{Value, json};

pub struct JsonRenderer;

#[derive(Serialize)]
struct JsonResponse<'a> {
    count: usize,
    status: u16,
    message: &'a str,
    results: &'a [Value],
}

impl Renderer for JsonRenderer {
    fn render(&self, response: &Response) -> Vec<u8> {
        // Pre-allocate estimated size
        let estimated = estimate_json_size(response);
        let mut buf = Vec::with_capacity(estimated);

        // Build results array
        let results = match &response.body {
            ResponseBody::Lines(lines) => lines.iter().map(|s| json!(s)).collect::<Vec<Value>>(),
            ResponseBody::JsonArray(values) => values.clone(),
            ResponseBody::Table { columns, rows } => {
                let cols = columns
                    .iter()
                    .map(|(n, t)| json!({ "name": n, "type": t }))
                    .collect::<Vec<Value>>();
                let row_vals = rows
                    .iter()
                    .map(|r| Value::Array(r.clone()))
                    .collect::<Vec<Value>>();
                vec![json!({ "columns": cols, "rows": row_vals })]
            }
        };

        let payload = JsonResponse {
            count: response.count,
            status: response.status.code(),
            message: &response.message,
            results: &results,
        };

        // Use sonic-rs for serialization
        if sonic_rs::to_writer(&mut buf, &payload).is_err() {
            buf = b"{\"status\":500,\"message\":\"Failed to serialize JSON\"}".to_vec();
        }

        buf.push(b'\n');
        buf
    }
}

fn estimate_json_size(response: &Response) -> usize {
    // Base overhead for {"count":X,"status":XXX,"message":"...","results":[]}
    let mut size = 100;

    match &response.body {
        ResponseBody::Lines(lines) => {
            // Each line: "string" + comma
            size += lines.len() * 50;
        }
        ResponseBody::JsonArray(values) => {
            // Rough estimate: 100 bytes per value
            size += values.len() * 100;
        }
        ResponseBody::Table { columns, rows } => {
            // columns: [{"name":"...","type":"..."}]
            size += columns.len() * 50;
            // rows: [[val1,val2,...]]
            size += rows.len() * columns.len() * 30;
        }
    }

    size
}
