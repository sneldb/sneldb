use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_schema::Schema;
use clap::Parser;
use std::io::{self, Cursor, Read};
use std::sync::Arc;

#[derive(Parser)]
#[command(name = "sneldb-cli")]
#[command(about = "Query SnelDB and display results as a table", long_about = None)]
struct Args {
    /// The SnelDB query to execute
    #[arg(short, long)]
    query: Option<String>,

    /// SnelDB HTTP server URL
    #[arg(short, long, default_value = "http://127.0.0.1:8085")]
    url: String,

    /// Authentication token
    #[arg(short, long, default_value = "mysecrettoken")]
    token: String,

    /// Read query from stdin instead of command line
    #[arg(long)]
    stdin: bool,

    /// Maximum number of rows to display (0 = unlimited)
    #[arg(short, long, default_value = "0")]
    limit: usize,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Get query from args, stdin, or prompt
    let query = if args.stdin {
        let mut input = String::new();
        io::stdin().read_to_string(&mut input)?;
        input.trim().to_string()
    } else if let Some(q) = args.query {
        q
    } else {
        eprintln!("Error: No query provided. Use --query <query> or --stdin");
        std::process::exit(1);
    };

    if query.is_empty() {
        eprintln!("Error: Query cannot be empty");
        std::process::exit(1);
    }

    // Execute query via HTTP
    let (response_data, execution_time_ms) = execute_query(&args.url, &args.token, &query)?;

    // Parse and display Arrow data
    display_arrow_data(&response_data, args.limit)?;

    // Display execution time if available
    if let Some(time_ms) = execution_time_ms {
        eprintln!("\nServer execution time: {:.3} ms", time_ms);
    }

    Ok(())
}

fn execute_query(url: &str, token: &str, query: &str) -> anyhow::Result<(Vec<u8>, Option<f64>)> {
    use http_body_util::{BodyExt, Full};
    use hyper::{Method, Request};
    use hyper_util::client::legacy::Client;
    use hyper_util::client::legacy::connect::HttpConnector;
    use hyper_util::rt::TokioExecutor;

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let client: Client<HttpConnector, Full<bytes::Bytes>> =
            Client::builder(TokioExecutor::new()).build_http();

        let uri = format!("{}/command", url)
            .parse::<hyper::Uri>()
            .map_err(|e| anyhow::anyhow!("Invalid URL: {}", e))?;

        let body = http_body_util::Full::new(bytes::Bytes::from(query.to_string()));
        let req = Request::builder()
            .method(Method::POST)
            .uri(uri)
            .header("Authorization", format!("Bearer {}", token))
            .header("Content-Type", "text/plain")
            .body(body)
            .map_err(|e| anyhow::anyhow!("Failed to build request: {}", e))?;

        let res = client.request(req).await?;
        let status = res.status();

        if !status.is_success() {
            let body_bytes = res.collect().await?.to_bytes();
            let body_str = String::from_utf8_lossy(&body_bytes);
            anyhow::bail!("HTTP error {}: {}", status, body_str);
        }

        // Extract execution time from header (before collecting body)
        // HTTP headers are case-insensitive, but hyper normalizes them
        let execution_time_ms = res
            .headers()
            .iter()
            .find(|(name, _)| name.as_str().eq_ignore_ascii_case("x-execution-time-ms"))
            .and_then(|(_, value)| value.to_str().ok())
            .and_then(|s| s.parse::<f64>().ok());

        let body_bytes = res.collect().await?.to_bytes();
        Ok((body_bytes.to_vec(), execution_time_ms))
    })
}

fn display_arrow_data(data: &[u8], row_limit: usize) -> anyhow::Result<()> {
    // Check if response is JSON (error response) instead of Arrow
    if let Ok(json_str) = std::str::from_utf8(data) {
        let trimmed = json_str.trim();

        if trimmed.starts_with('{') || trimmed.starts_with('[') {
            // Check if this is streaming JSON (newline-delimited JSON) format
            // Streaming JSON has multiple JSON objects, one per line
            if trimmed.contains('\n') {
                // Check if it looks like streaming format (has "type" field in first line)
                let first_line = trimmed.lines().next().unwrap_or("");
                if first_line.contains("\"type\":\"schema\"")
                    || first_line.contains("\"type\":\"batch\"")
                {
                    return display_streaming_json(trimmed, row_limit);
                }
                // If it has newlines and is large, likely streaming format
                if data.len() > 10000 {
                    return display_streaming_json(trimmed, row_limit);
                }
            }

            // Try to parse as single JSON object
            match serde_json::from_str::<serde_json::Value>(trimmed) {
                Ok(v) => {
                    println!("{}", serde_json::to_string_pretty(&v)?);
                    return Ok(());
                }
                Err(e) => {
                    // If large response, likely streaming format
                    if data.len() > 10000 {
                        return display_streaming_json(trimmed, row_limit);
                    }
                    return Err(e.into());
                }
            }
        }
    }
    // Try to parse as Arrow IPC stream
    let cursor = Cursor::new(data);
    let mut reader = StreamReader::try_new(cursor, None).map_err(|e| {
        anyhow::anyhow!(
            "Failed to parse Arrow stream: {}. Is the server configured for Arrow output?",
            e
        )
    })?;

    let schema = reader.schema();
    print_schema(&schema);

    let mut all_batches = Vec::new();
    let mut total_rows = 0;

    while let Some(batch_result) = reader.next() {
        let batch = batch_result?;
        total_rows += batch.num_rows();
        all_batches.push(batch);
    }

    if all_batches.is_empty() {
        println!("\nNo data returned.");
        return Ok(());
    }

    println!(
        "\nTotal rows: {} (showing first {})",
        total_rows,
        if row_limit > 0 { row_limit } else { total_rows }
    );
    println!();

    // Combine all batches into one table
    display_table(&all_batches, &schema, row_limit)
}

fn display_streaming_json(json_str: &str, row_limit: usize) -> anyhow::Result<()> {
    let mut schema: Option<serde_json::Value> = None;
    let mut column_names: Vec<String> = Vec::new();
    let mut all_rows: Vec<Vec<String>> = Vec::new();
    let mut row_count = 0;

    // Parse each line as a separate JSON object (NDJSON format)
    for (_line_num, line) in json_str.lines().enumerate() {
        let trimmed_line = line.trim();
        if trimmed_line.is_empty() {
            continue;
        }

        match serde_json::from_str::<serde_json::Value>(trimmed_line) {
            Ok(json) => {
                if let Some(obj) = json.as_object() {
                    if let Some(type_val) = obj.get("type") {
                        if let Some(type_str) = type_val.as_str() {
                            match type_str {
                                "schema" => {
                                    schema = Some(json.clone());
                                    if let Some(columns) =
                                        obj.get("columns").and_then(|c| c.as_array())
                                    {
                                        column_names = columns
                                            .iter()
                                            .filter_map(|col| {
                                                col.as_object()
                                                    .and_then(|o| o.get("name"))
                                                    .and_then(|n| n.as_str())
                                                    .map(|s| s.to_string())
                                            })
                                            .collect();
                                    }
                                }
                                "batch" => {
                                    if let Some(rows) = obj.get("rows").and_then(|r| r.as_array()) {
                                        for row in rows {
                                            if row_limit > 0 && row_count >= row_limit {
                                                break;
                                            }
                                            if let Some(row_array) = row.as_array() {
                                                let row_values: Vec<String> = row_array
                                                    .iter()
                                                    .map(|v| match v {
                                                        serde_json::Value::Null => {
                                                            "NULL".to_string()
                                                        }
                                                        serde_json::Value::String(s) => s.clone(),
                                                        serde_json::Value::Number(n) => {
                                                            n.to_string()
                                                        }
                                                        serde_json::Value::Bool(b) => b.to_string(),
                                                        _ => v.to_string(),
                                                    })
                                                    .collect();
                                                all_rows.push(row_values);
                                                row_count += 1;
                                            }
                                        }
                                    }
                                }
                                "end" => {
                                    break;
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            Err(_) => {
                // Skip invalid JSON lines
            }
        }
    }

    if let Some(schema_json) = schema {
        println!("Schema:");
        println!("{}", serde_json::to_string_pretty(&schema_json)?);
        println!();
    }

    if all_rows.is_empty() {
        println!("No data rows found.");
        return Ok(());
    }

    println!(
        "Total rows: {} (showing first {})",
        row_count,
        if row_limit > 0 { row_limit } else { row_count }
    );
    println!();

    // Display table
    if column_names.is_empty() {
        // If no column names, use generic names
        column_names = (0..all_rows.first().map(|r| r.len()).unwrap_or(0))
            .map(|i| format!("col_{}", i))
            .collect();
    }

    display_simple_table(&column_names, &all_rows);
    Ok(())
}

fn print_schema(schema: &Arc<Schema>) {
    println!("Schema:");
    println!("{}", "─".repeat(60));
    for (i, field) in schema.fields().iter().enumerate() {
        println!("  {}: {} ({})", i, field.name(), field.data_type());
    }
}

fn display_table(
    batches: &[RecordBatch],
    schema: &Arc<Schema>,
    row_limit: usize,
) -> anyhow::Result<()> {
    // Collect all data
    let mut all_rows: Vec<Vec<String>> = Vec::new();
    let mut row_count = 0;

    for batch in batches {
        let num_rows = batch.num_rows();
        let num_cols = batch.num_columns();

        for row_idx in 0..num_rows {
            if row_limit > 0 && row_count >= row_limit {
                break;
            }

            let mut row = Vec::with_capacity(num_cols);
            for col_idx in 0..num_cols {
                let array = batch.column(col_idx);
                let value_str = format_cell_value(array, row_idx)?;
                row.push(value_str);
            }
            all_rows.push(row);
            row_count += 1;
        }

        if row_limit > 0 && row_count >= row_limit {
            break;
        }
    }

    if all_rows.is_empty() {
        println!("No rows to display.");
        return Ok(());
    }

    // Get column names
    let column_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

    // Display using a simple table format
    display_simple_table(&column_names, &all_rows);

    Ok(())
}

fn display_simple_table(columns: &[String], rows: &[Vec<String>]) {
    // Calculate column widths
    let mut col_widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();

    for row in rows {
        for (col_idx, value) in row.iter().enumerate() {
            if col_idx < col_widths.len() {
                let display_len = value.chars().count();
                if display_len > col_widths[col_idx] {
                    col_widths[col_idx] = display_len.min(50); // Max width 50
                }
            }
        }
    }

    // Print header
    print!("┌");
    for (i, width) in col_widths.iter().enumerate() {
        if i > 0 {
            print!("┬");
        }
        print!("{}", "─".repeat(*width + 2));
    }
    println!("┐");

    print!("│");
    for (i, (col_name, width)) in columns.iter().zip(col_widths.iter()).enumerate() {
        if i > 0 {
            print!("│");
        }
        print!(" {:<width$} ", col_name, width = *width);
    }
    println!("│");

    print!("├");
    for (i, width) in col_widths.iter().enumerate() {
        if i > 0 {
            print!("┼");
        }
        print!("{}", "─".repeat(*width + 2));
    }
    println!("┤");

    // Print rows
    for row in rows {
        print!("│");
        for (col_idx, width) in col_widths.iter().enumerate() {
            if col_idx > 0 {
                print!("│");
            }
            let value = if col_idx < row.len() {
                let val = &row[col_idx];
                if val.chars().count() > *width {
                    format!("{}...", &val.chars().take(*width - 3).collect::<String>())
                } else {
                    val.clone()
                }
            } else {
                String::new()
            };
            print!(" {:<width$} ", value, width = *width);
        }
        println!("│");
    }

    // Print footer
    print!("└");
    for (i, width) in col_widths.iter().enumerate() {
        if i > 0 {
            print!("┴");
        }
        print!("{}", "─".repeat(*width + 2));
    }
    println!("┘");
}

fn format_cell_value(array: &arrow_array::ArrayRef, row_idx: usize) -> anyhow::Result<String> {
    use arrow_array::*;

    match array.data_type() {
        arrow_schema::DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            Ok(if arr.is_null(row_idx) {
                "NULL".to_string()
            } else {
                arr.value(row_idx).to_string()
            })
        }
        arrow_schema::DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            Ok(if arr.is_null(row_idx) {
                "NULL".to_string()
            } else {
                arr.value(row_idx).to_string()
            })
        }
        arrow_schema::DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(if arr.is_null(row_idx) {
                "NULL".to_string()
            } else {
                arr.value(row_idx).to_string()
            })
        }
        arrow_schema::DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(if arr.is_null(row_idx) {
                "NULL".to_string()
            } else {
                arr.value(row_idx).to_string()
            })
        }
        arrow_schema::DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            Ok(if arr.is_null(row_idx) {
                "NULL".to_string()
            } else {
                arr.value(row_idx).to_string()
            })
        }
        arrow_schema::DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            Ok(if arr.is_null(row_idx) {
                "NULL".to_string()
            } else {
                arr.value(row_idx).to_string()
            })
        }
        arrow_schema::DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            Ok(if arr.is_null(row_idx) {
                "NULL".to_string()
            } else {
                arr.value(row_idx).to_string()
            })
        }
        arrow_schema::DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            Ok(if arr.is_null(row_idx) {
                "NULL".to_string()
            } else {
                arr.value(row_idx).to_string()
            })
        }
        arrow_schema::DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            Ok(if arr.is_null(row_idx) {
                "NULL".to_string()
            } else {
                arr.value(row_idx).to_string()
            })
        }
        arrow_schema::DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            Ok(if arr.is_null(row_idx) {
                "NULL".to_string()
            } else {
                arr.value(row_idx).to_string()
            })
        }
        arrow_schema::DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(if arr.is_null(row_idx) {
                "NULL".to_string()
            } else {
                arr.value(row_idx).to_string()
            })
        }
        arrow_schema::DataType::Utf8 | arrow_schema::DataType::LargeUtf8 => {
            if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
                Ok(if arr.is_null(row_idx) {
                    "NULL".to_string()
                } else {
                    arr.value(row_idx).to_string()
                })
            } else if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
                Ok(if arr.is_null(row_idx) {
                    "NULL".to_string()
                } else {
                    arr.value(row_idx).to_string()
                })
            } else {
                Ok("?".to_string())
            }
        }
        arrow_schema::DataType::Timestamp(_, _) => {
            if let Some(arr) = array.as_any().downcast_ref::<TimestampSecondArray>() {
                Ok(if arr.is_null(row_idx) {
                    "NULL".to_string()
                } else {
                    arr.value(row_idx).to_string()
                })
            } else if let Some(arr) = array.as_any().downcast_ref::<TimestampMillisecondArray>() {
                Ok(if arr.is_null(row_idx) {
                    "NULL".to_string()
                } else {
                    arr.value(row_idx).to_string()
                })
            } else if let Some(arr) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                Ok(if arr.is_null(row_idx) {
                    "NULL".to_string()
                } else {
                    arr.value(row_idx).to_string()
                })
            } else if let Some(arr) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
                Ok(if arr.is_null(row_idx) {
                    "NULL".to_string()
                } else {
                    arr.value(row_idx).to_string()
                })
            } else {
                Ok("?".to_string())
            }
        }
        _ => {
            // Fallback: try to get string representation
            Ok(format!("{:?}", array))
        }
    }
}
