use crate::shared::response::render::Renderer;
use crate::shared::response::types::{Response, ResponseBody};
use serde_json::to_string;

pub struct UnixRenderer;

impl Renderer for UnixRenderer {
    fn render(&self, response: &Response) -> Vec<u8> {
        let mut output = Vec::new();

        // Header line: 200 OK
        output.extend_from_slice(
            format!("{} {}\n", response.status.code(), response.message).as_bytes(),
        );

        match &response.body {
            ResponseBody::Lines(lines) => {
                for line in lines {
                    output.extend_from_slice(line.as_bytes());
                    output.push(b'\n');
                }
            }

            ResponseBody::JsonArray(items) => {
                for (i, item) in items.iter().enumerate() {
                    match to_string(item) {
                        Ok(json_str) => {
                            output.extend_from_slice(format!("[{}] {}\n", i, json_str).as_bytes());
                        }
                        Err(_) => {
                            output
                                .extend_from_slice(format!("[{}] <invalid json>\n", i).as_bytes());
                        }
                    }
                }
            }
        }

        output
    }
}
