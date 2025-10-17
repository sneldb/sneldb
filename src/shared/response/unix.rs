use crate::shared::response::render::Renderer;
use crate::shared::response::types::{Response, ResponseBody};

pub struct UnixRenderer;

impl Renderer for UnixRenderer {
    fn render(&self, response: &Response) -> Vec<u8> {
        // Pre-allocate buffer based on estimated response size
        let estimated_size = estimate_response_size(response);
        let mut output = Vec::with_capacity(estimated_size);

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
                    output.extend_from_slice(format!("[{}] ", i).as_bytes());
                    match sonic_rs::to_writer(&mut output, item) {
                        Ok(_) => {
                            output.push(b'\n');
                        }
                        Err(_) => {
                            output.extend_from_slice(b"<invalid json>\n");
                        }
                    }
                }
            }

            ResponseBody::Table { columns, rows } => {
                // Use sonic-rs to serialize the entire table at once for better performance
                #[derive(serde::Serialize)]
                struct TableResponse<'a> {
                    columns: &'a [(String, String)],
                    rows: &'a [Vec<serde_json::Value>],
                }

                let table = TableResponse { columns, rows };
                if let Ok(_) = sonic_rs::to_writer(&mut output, &table) {
                    output.push(b'\n');
                }
            }
        }

        output
    }
}

/// Estimate the size of the response for buffer pre-allocation
fn estimate_response_size(response: &Response) -> usize {
    // Base size for status line
    let mut size = 50;

    match &response.body {
        ResponseBody::Lines(lines) => {
            // Each line + newline
            size += lines.iter().map(|l| l.len() + 1).sum::<usize>();
        }
        ResponseBody::JsonArray(items) => {
            // Rough estimate: ~200 bytes per JSON event (typical)
            // Plus index prefix "[N] "
            size += items.len() * 220;
        }
        ResponseBody::Table { columns, rows } => {
            // Column headers + row data
            // Rough estimate: ~50 bytes per cell
            size += columns.len() * 50; // Column headers
            size += rows.len() * columns.len() * 50; // Cell data
        }
    }

    size
}
