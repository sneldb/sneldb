use crate::engine::core::read::flow::ColumnBatch;
use crate::engine::types::ScalarValue;
use crate::shared::response::render::{Renderer, StreamingFormat};
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

            ResponseBody::ScalarArray(items) => {
                for (i, item) in items.iter().enumerate() {
                    output.extend_from_slice(format!("[{}] ", i).as_bytes());
                    let json_value = item.to_json();
                    match sonic_rs::to_writer(&mut output, &json_value) {
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
                // Convert ScalarValues to JSON Values at render time
                let json_rows: Vec<Vec<Value>> = rows
                    .iter()
                    .map(|row| row.iter().map(|v| v.to_json()).collect())
                    .collect();

                // Use sonic-rs to serialize the entire table at once for better performance
                #[derive(serde::Serialize)]
                struct TableResponse<'a> {
                    columns: &'a [(String, String)],
                    rows: &'a [Vec<serde_json::Value>],
                }

                let table = TableResponse {
                    columns,
                    rows: &json_rows,
                };
                if let Ok(_) = sonic_rs::to_writer(&mut output, &table) {
                    output.push(b'\n');
                }
            }
        }

        output
    }

    fn streaming_format(&self) -> StreamingFormat {
        StreamingFormat::Json
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

    fn stream_row(&self, columns: &[&str], values: &[ScalarValue], out: &mut Vec<u8>) {
        out.clear();

        // Convert ScalarValues to JSON Values at render time
        let json_values: Vec<Value> = values.iter().map(|v| v.to_json()).collect();
        let json_refs: Vec<&Value> = json_values.iter().collect();
        let row_values = RowValues {
            columns,
            values: &json_refs,
        };

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

    fn stream_batch(&self, _columns: &[&str], batch: &[Vec<ScalarValue>], out: &mut Vec<u8>) {
        out.clear();

        // Convert ScalarValues to JSON Values at render time
        let json_batch: Vec<Vec<Value>> = batch
            .iter()
            .map(|row| row.iter().map(|v| v.to_json()).collect())
            .collect();

        // Serialize batch as array of arrays (same format as JsonRenderer)
        #[derive(serde::Serialize)]
        struct UnixBatchFrame {
            #[serde(rename = "type")]
            frame_type: &'static str,
            rows: Vec<Vec<Value>>,
        }

        let batch_frame = UnixBatchFrame {
            frame_type: "batch",
            rows: json_batch,
        };

        if sonic_rs::to_writer(&mut *out, &batch_frame).is_err() {
            out.clear();
            out.extend_from_slice(b"{\"type\":\"batch\",\"rows\":[]}\n");
            return;
        }

        out.push(b'\n');
    }

    fn stream_column_batch(&self, _batch: &ColumnBatch, _out: &mut Vec<u8>) {
        // For Unix, we need to convert ColumnBatch to rows of ScalarValues
        // This is handled by the caller using stream_batch instead
        unreachable!("stream_column_batch should not be called for Unix renderer")
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
        ResponseBody::ScalarArray(items) => {
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
