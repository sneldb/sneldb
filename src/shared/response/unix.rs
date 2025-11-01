use crate::shared::response::render::Renderer;
use crate::shared::response::types::{Response, ResponseBody};
use serde::Serialize;
use serde::ser::{SerializeMap, SerializeSeq, Serializer as SerdeSerializer};
use serde_json::Serializer as JsonSerializer;
use serde_json::Value;

// Frame structure that avoids Value cloning
#[derive(Serialize)]
struct UnixRowFrameNoClone<'a> {
    #[serde(rename = "type")]
    frame_type: &'static str,
    values: RowValues<'a>,
}

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

    fn stream_schema(&self, columns: &[(String, String)], out: &mut Vec<u8>) {
        out.clear();
        let mut serializer = JsonSerializer::new(&mut *out);
        let mut map =
            SerdeSerializer::serialize_map(&mut serializer, Some(2)).expect("serialize schema map");
        map.serialize_entry("type", "schema")
            .expect("serialize schema type");
        map.serialize_entry("columns", &ColumnList(columns))
            .expect("serialize schema columns");
        SerializeMap::end(map).expect("finish schema map");
        out.push(b'\n');
    }

    fn stream_row(&self, columns: &[&str], values: &[&Value], out: &mut Vec<u8>) {
        out.clear();

        // Use RowValues which serializes Values by reference without cloning
        let row_values = RowValues { columns, values };

        let frame = UnixRowFrameNoClone {
            frame_type: "row",
            values: row_values,
        };

        if sonic_rs::to_writer(&mut *out, &frame).is_err() {
            out.clear();
            out.extend_from_slice(b"{\"type\":\"row\",\"values\":{}}\n");
            return;
        }

        out.push(b'\n');
    }

    fn stream_end(&self, row_count: usize, out: &mut Vec<u8>) {
        out.clear();
        let mut serializer = JsonSerializer::new(&mut *out);
        let mut map =
            SerdeSerializer::serialize_map(&mut serializer, Some(2)).expect("serialize end map");
        map.serialize_entry("type", "end")
            .expect("serialize end type");
        map.serialize_entry("row_count", &row_count)
            .expect("serialize end row_count");
        SerializeMap::end(map).expect("finish end map");
        out.push(b'\n');
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

#[derive(Serialize)]
struct ColumnRef<'a> {
    name: &'a str,
    logical_type: &'a str,
}

struct ColumnList<'a>(&'a [(String, String)]);

impl<'a> Serialize for ColumnList<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for (name, logical_type) in self.0 {
            seq.serialize_element(&ColumnRef {
                name: name.as_str(),
                logical_type: logical_type.as_str(),
            })?;
        }
        seq.end()
    }
}

struct RowValues<'a> {
    columns: &'a [&'a str], // Changed from &[String] to &[&str]
    values: &'a [&'a Value],
}

impl<'a> Serialize for RowValues<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.columns.len()))?;
        for (name, value) in self.columns.iter().zip(self.values.iter()) {
            map.serialize_entry(*name, *value)?;
        }
        map.end()
    }
}
