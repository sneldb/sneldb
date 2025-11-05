use crate::shared::response::render::{Renderer, StreamingFormat};
use crate::shared::response::types::{Response, ResponseBody};
use serde::Serialize;
use serde_json::Value;
use serde_json::json;

pub struct JsonRenderer;

#[derive(Serialize)]
struct JsonResponse<'a> {
    count: usize,
    status: u16,
    message: &'a str,
    results: &'a [Value],
}

#[derive(Serialize)]
struct SchemaFrame<'a> {
    #[serde(rename = "type")]
    frame_type: &'static str,
    columns: &'a [ColumnRef<'a>],
}

// Frame structure that avoids Value cloning by serializing values by reference
#[derive(Serialize)]
struct RowFrameNoClone<'a> {
    #[serde(rename = "type")]
    frame_type: &'static str,
    values: RowValuesNoClone<'a>,
}

// Serializes Values by reference without cloning
struct RowValuesNoClone<'a> {
    columns: &'a [&'a str],
    values: &'a [&'a Value],
}

impl<'a> Serialize for RowValuesNoClone<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.columns.len()))?;
        for (name, value) in self.columns.iter().zip(self.values.iter()) {
            map.serialize_key(name)?;
            map.serialize_value(*value)?;
        }
        map.end()
    }
}

#[derive(Serialize)]
struct EndFrame {
    #[serde(rename = "type")]
    frame_type: &'static str,
    row_count: usize,
}

#[derive(Serialize)]
struct BatchFrame<'a> {
    #[serde(rename = "type")]
    frame_type: &'static str,
    rows: BatchRows<'a>,
}

// Serializes batch rows as array of arrays
struct BatchRows<'a> {
    rows: &'a [Vec<&'a Value>],
}

impl<'a> Serialize for BatchRows<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq;
        let mut seq = serializer.serialize_seq(Some(self.rows.len()))?;
        for row in self.rows {
            seq.serialize_element(row)?;
        }
        seq.end()
    }
}

#[derive(Serialize)]
struct ColumnRef<'a> {
    name: &'a str,
    logical_type: &'a str,
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

    fn streaming_format(&self) -> StreamingFormat {
        StreamingFormat::Json
    }

    fn stream_schema_json(&self, columns: &[(String, String)], out: &mut Vec<u8>) {
        out.clear();

        // Convert columns to ColumnRef slices
        let column_refs: Vec<ColumnRef> = columns
            .iter()
            .map(|(name, logical_type)| ColumnRef {
                name: name.as_str(),
                logical_type: logical_type.as_str(),
            })
            .collect();

        let frame = SchemaFrame {
            frame_type: "schema",
            columns: &column_refs,
        };

        // Serialize directly into out
        if sonic_rs::to_writer(&mut *out, &frame).is_err() {
            out.clear();
            out.extend_from_slice(b"{\"type\":\"schema\",\"columns\":[]}\n");
            return;
        }

        out.push(b'\n');
    }

    fn stream_row_json(&self, columns: &[&str], values: &[&Value], out: &mut Vec<u8>) {
        out.clear();

        // Use RowValuesNoClone which serializes Values by reference without cloning
        let row_values = RowValuesNoClone { columns, values };

        let frame = RowFrameNoClone {
            frame_type: "row",
            values: row_values,
        };

        // Serialize directly into out - sonic-rs will serialize RowValuesNoClone
        // which serializes each Value by reference, avoiding cloning
        if sonic_rs::to_writer(&mut *out, &frame).is_err() {
            out.clear();
            out.extend_from_slice(b"{\"type\":\"row\",\"values\":{}}\n");
            return;
        }

        out.push(b'\n');
    }

    fn stream_batch_json(&self, _columns: &[&str], batch: &[Vec<&Value>], out: &mut Vec<u8>) {
        out.clear();

        // Serialize batch as array of arrays (more efficient than per-row objects)
        let batch_frame = BatchFrame {
            frame_type: "batch",
            rows: BatchRows { rows: batch },
        };

        if sonic_rs::to_writer(&mut *out, &batch_frame).is_err() {
            out.clear();
            out.extend_from_slice(b"{\"type\":\"batch\",\"rows\":[]}\n");
            return;
        }

        out.push(b'\n');
    }

    fn stream_end_json(&self, row_count: usize, out: &mut Vec<u8>) {
        out.clear();

        let frame = EndFrame {
            frame_type: "end",
            row_count,
        };

        // Serialize directly into out
        if sonic_rs::to_writer(&mut *out, &frame).is_err() {
            out.clear();
            out.extend_from_slice(b"{\"type\":\"end\",\"row_count\":0}\n");
            return;
        }

        out.push(b'\n');
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
