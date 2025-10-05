use crate::shared::response::render::Renderer;
use crate::shared::response::types::{Response, ResponseBody};
use serde_json::{to_string, to_writer};

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

            ResponseBody::Table { columns, rows } => {
                // Stream a single JSON object: {"columns":[...],"rows":[...]}
                output.extend_from_slice(b"{");
                // columns
                output.extend_from_slice(b"\"columns\":");
                output.push(b'[');
                for (i, (name, ty)) in columns.iter().enumerate() {
                    if i > 0 {
                        output.push(b',');
                    }
                    output.push(b'{');
                    output.extend_from_slice(b"\"name\":");
                    to_writer(&mut output, name).ok();
                    output.push(b',');
                    output.extend_from_slice(b"\"type\":");
                    to_writer(&mut output, ty).ok();
                    output.push(b'}');
                }
                output.push(b']');

                output.push(b',');
                // rows
                output.extend_from_slice(b"\"rows\":");
                output.push(b'[');
                for (ri, row) in rows.iter().enumerate() {
                    if ri > 0 {
                        output.push(b',');
                    }
                    output.push(b'[');
                    for (ci, cell) in row.iter().enumerate() {
                        if ci > 0 {
                            output.push(b',');
                        }
                        to_writer(&mut output, cell).ok();
                    }
                    output.push(b']');
                }
                output.push(b']');
                output.push(b'}');
                output.push(b'\n');
            }
        }

        output
    }
}
