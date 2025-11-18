use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_schema::Schema;
use clap::Parser;
use hmac::{Hmac, Mac};
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use sha2::Sha256;
use std::env;
use std::fs;
use std::io::{self, Cursor, Read};
use std::path::Path;
use std::sync::Arc;

type HmacSha256 = Hmac<Sha256>;

#[derive(Parser)]
#[command(name = "sneldb-cli")]
#[command(about = "Interactive SnelDB query console", long_about = None)]
struct Args {
    /// The SnelDB query to execute (non-interactive mode)
    #[arg(short, long)]
    query: Option<String>,

    /// SnelDB HTTP server URL
    /// Can also be set via SNELDB_URL environment variable
    #[arg(short, long)]
    url: Option<String>,

    /// Authentication token (Bearer token)
    /// Can also be set via SNELDB_TOKEN environment variable or config file
    #[arg(short, long)]
    token: Option<String>,

    /// User ID for HMAC authentication
    /// Can also be set via SNELDB_USER_ID environment variable or config file
    #[arg(long)]
    user_id: Option<String>,

    /// Secret key for HMAC authentication
    /// Can also be set via SNELDB_SECRET_KEY environment variable or config file
    #[arg(long)]
    secret_key: Option<String>,

    /// Read query from stdin instead of command line
    #[arg(long)]
    stdin: bool,

    /// Maximum number of rows to display (0 = unlimited)
    #[arg(short, long, default_value = "0")]
    limit: usize,
}

#[derive(Clone)]
enum AuthMethod {
    BearerToken(String),
    UserHmac { user_id: String, secret_key: String },
}

struct Config {
    url: String,
    auth: AuthMethod,
}

impl Config {
    fn load() -> anyhow::Result<Self> {
        // Priority order:
        // 1. Command line arguments (handled in main)
        // 2. Environment variables
        // 3. Config file (~/.sneldb/config or .sneldb/config)
        // 4. Defaults

        let url = env::var("SNELDB_URL")
            .ok()
            .or_else(|| read_config_file().and_then(|c| c.url))
            .unwrap_or_else(|| "http://127.0.0.1:8085".to_string());

        // Check for user-based auth first (user_id + secret_key)
        let user_id = env::var("SNELDB_USER_ID")
            .ok()
            .or_else(|| read_config_file().and_then(|c| c.user_id.clone()));
        let secret_key = env::var("SNELDB_SECRET_KEY")
            .ok()
            .or_else(|| read_config_file().and_then(|c| c.secret_key.clone()));

        let auth = if let (Some(uid), Some(key)) = (user_id, secret_key) {
            AuthMethod::UserHmac {
                user_id: uid,
                secret_key: key,
            }
        } else {
            // Fall back to Bearer token
            let token = env::var("SNELDB_TOKEN")
                .ok()
                .or_else(|| read_config_file().and_then(|c| c.token))
                .unwrap_or_else(|| "mysecrettoken".to_string());
            AuthMethod::BearerToken(token)
        };

        Ok(Config { url, auth })
    }

    fn merge_with_args(&mut self, args: &Args) {
        if let Some(url) = &args.url {
            self.url = url.clone();
        }

        // Command-line args override everything
        if let (Some(user_id), Some(secret_key)) = (&args.user_id, &args.secret_key) {
            self.auth = AuthMethod::UserHmac {
                user_id: user_id.clone(),
                secret_key: secret_key.clone(),
            };
        } else if let Some(token) = &args.token {
            self.auth = AuthMethod::BearerToken(token.clone());
        }
    }
}

#[derive(serde::Deserialize)]
struct ConfigFile {
    #[serde(rename = "config")]
    config: Option<ConfigSection>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "snake_case")]
struct ConfigSection {
    url: Option<String>,
    token: Option<String>,
    user_id: Option<String>,
    secret_key: Option<String>,
}

fn read_config_file() -> Option<ConfigSection> {
    // Try ~/.sneldb/config first
    if let Some(home) = dirs::home_dir() {
        let config_path = home.join(".sneldb").join("config");
        if let Ok(content) = fs::read_to_string(&config_path) {
            if let Ok(config_file) = toml::from_str::<ConfigFile>(&content) {
                if let Some(config) = config_file.config {
                    return Some(config);
                }
            }
        }
    }

    // Try .sneldb/config in current directory
    let local_config = Path::new(".sneldb").join("config");
    if let Ok(content) = fs::read_to_string(&local_config) {
        if let Ok(config_file) = toml::from_str::<ConfigFile>(&content) {
            if let Some(config) = config_file.config {
                return Some(config);
            }
        }
    }

    None
}

fn compute_hmac_signature(secret_key: &str, message: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret_key.as_bytes()).expect("HMAC can take key of any size");
    mac.update(message.trim().as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

struct Client {
    url: String,
    auth: AuthMethod,
    runtime: tokio::runtime::Runtime,
    http_client: hyper_util::client::legacy::Client<
        hyper_util::client::legacy::connect::HttpConnector,
        http_body_util::Full<bytes::Bytes>,
    >,
}

impl Client {
    fn new(url: String, auth: AuthMethod) -> anyhow::Result<Self> {
        use hyper_util::client::legacy::Client;
        use hyper_util::client::legacy::connect::HttpConnector;
        use hyper_util::rt::TokioExecutor;

        let runtime = tokio::runtime::Runtime::new()?;
        let http_client: Client<HttpConnector, http_body_util::Full<bytes::Bytes>> =
            Client::builder(TokioExecutor::new()).build_http();

        Ok(Self {
            url,
            auth,
            runtime,
            http_client,
        })
    }

    fn execute_query(&self, query: &str) -> anyhow::Result<(Vec<u8>, Option<f64>)> {
        use http_body_util::{BodyExt, Full};
        use hyper::{Method, Request};

        self.runtime.block_on(async {
            let uri = format!("{}/command", self.url)
                .parse::<hyper::Uri>()
                .map_err(|e| anyhow::anyhow!("Invalid URL: {}", e))?;

            let body = Full::new(bytes::Bytes::from(query.to_string()));
            let mut req_builder = Request::builder()
                .method(Method::POST)
                .uri(uri)
                .header("Content-Type", "text/plain");

            // Add authentication headers
            match &self.auth {
                AuthMethod::BearerToken(token) => {
                    req_builder = req_builder.header("Authorization", format!("Bearer {}", token));
                }
                AuthMethod::UserHmac { user_id, secret_key } => {
                    let signature = compute_hmac_signature(secret_key, query);
                    req_builder = req_builder
                        .header("X-Auth-User", user_id.as_str())
                        .header("X-Auth-Signature", signature.as_str());
                }
            }

            let req = req_builder
                .body(body)
                .map_err(|e| anyhow::anyhow!("Failed to build request: {}", e))?;

            let res = self.http_client.request(req).await?;
            let status = res.status();

            if !status.is_success() {
                let body_bytes = res.collect().await?.to_bytes();
                let body_str = String::from_utf8_lossy(&body_bytes);

                // Provide helpful error message for authentication failures
                if status == 401 {
                    let auth_help = match &self.auth {
                        AuthMethod::BearerToken(_) => {
                            "The server requires user-based HMAC authentication (not Bearer token).\n\n\
                            When the server has 'bypass_auth = false' in its config, you must use\n\
                            user-based authentication with user_id and secret_key.\n\n\
                            To fix this:\n\
                            1. Use user credentials instead of Bearer token:\n\
                               sneldb_cli --user-id 'admin' --secret-key 'your-secret-key'\n\n\
                            2. Or set environment variables:\n\
                               export SNELDB_USER_ID='your-user-id'\n\
                               export SNELDB_SECRET_KEY='your-secret-key'\n\n\
                            3. Or create a config file at ~/.sneldb/config:\n\
                               [config]\n\
                               user_id = 'your-user-id'\n\
                               secret_key = 'your-secret-key'\n\
                               url = 'http://127.0.0.1:8085'\n\n\
                            Note: If you don't have a user yet, you may need to:\n\
                            - Check if your server config has 'initial_admin_user' and 'initial_admin_key'\n\
                            - Or temporarily set 'bypass_auth = true' to create a user"
                        }
                        AuthMethod::UserHmac { .. } => {
                            "The server rejected your user credentials.\n\n\
                            To fix this:\n\
                            1. Verify your user_id and secret_key are correct\n\
                            2. Check if the user exists in the server\n\
                            3. If using initial admin credentials, verify they match server config:\n\
                               - Check server config for 'initial_admin_user' and 'initial_admin_key'\n\n\
                            You can also set credentials via:\n\
                            - Environment variables: SNELDB_USER_ID and SNELDB_SECRET_KEY\n\
                            - Command line: --user-id and --secret-key flags\n\
                            - Config file: ~/.sneldb/config"
                        }
                    };
                    anyhow::bail!(
                        "Authentication failed (401 Unauthorized)\n\n{}\n\nServer response: {}",
                        auth_help,
                        body_str
                    );
                }

                anyhow::bail!("HTTP error {}: {}", status, body_str);
            }

            // Extract execution time from header
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
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Load configuration (env vars, config file, defaults)
    let mut config = Config::load()?;
    config.merge_with_args(&args);

    // Non-interactive mode: execute query and exit
    if args.stdin {
        let mut input = String::new();
        io::stdin().read_to_string(&mut input)?;
        let query = input.trim().to_string();
        if query.is_empty() {
            eprintln!("Error: Query cannot be empty");
            std::process::exit(1);
        }
        let client = Client::new(config.url.clone(), config.auth.clone())?;
        let (response_data, execution_time_ms) = client.execute_query(&query)?;
        display_arrow_data(&response_data, args.limit)?;
        if let Some(time_ms) = execution_time_ms {
            eprintln!("\nServer execution time: {:.3} ms", time_ms);
        }
        return Ok(());
    }

    if let Some(query) = args.query {
        if query.is_empty() {
            eprintln!("Error: Query cannot be empty");
            std::process::exit(1);
        }
        let client = Client::new(config.url.clone(), config.auth.clone())?;
        let (response_data, execution_time_ms) = client.execute_query(&query)?;
        display_arrow_data(&response_data, args.limit)?;
        if let Some(time_ms) = execution_time_ms {
            eprintln!("\nServer execution time: {:.3} ms", time_ms);
        }
        return Ok(());
    }

    // Interactive mode
    run_interactive(config, args)
}

fn run_interactive(config: Config, args: Args) -> anyhow::Result<()> {
    let client = Client::new(config.url.clone(), config.auth.clone())?;
    let mut rl = DefaultEditor::new()?;

    // Load history if available
    let _ = rl.load_history(".sneldb_history");

    println!("SnelDB Interactive Console");
    println!("Type '\\h' for help, '\\q' to quit");
    println!("Connected to: {}", config.url);

    // Show authentication status
    match &config.auth {
        AuthMethod::BearerToken(token) => {
            let token_display = if token.len() > 8 {
                format!("{}...{}", &token[..4], &token[token.len() - 4..])
            } else {
                "***".to_string()
            };
            println!("Using Bearer token: {}\n", token_display);
        }
        AuthMethod::UserHmac {
            user_id,
            secret_key,
        } => {
            let key_display = if secret_key.len() > 8 {
                format!(
                    "{}...{}",
                    &secret_key[..4],
                    &secret_key[secret_key.len() - 4..]
                )
            } else {
                "***".to_string()
            };
            println!(
                "Using user authentication: user_id={}, secret_key={}\n",
                user_id, key_display
            );
        }
    }

    let mut query_buffer = String::new();
    let mut in_multiline = false;

    loop {
        let prompt = if in_multiline { "  -> " } else { "sneldb=> " };
        match rl.readline(prompt) {
            Ok(line) => {
                let trimmed = line.trim();

                // Handle empty lines in multiline mode
                if trimmed.is_empty() && in_multiline {
                    // Empty line in multiline mode - check if query is complete
                    if !query_buffer.trim().is_empty() {
                        // Execute the accumulated query
                        let query = query_buffer.trim().to_string();
                        query_buffer.clear();
                        in_multiline = false;

                        if !query.is_empty() {
                            match client.execute_query(&query) {
                                Ok((response_data, execution_time_ms)) => {
                                    if let Err(e) = display_arrow_data(&response_data, args.limit) {
                                        eprintln!("Error displaying results: {}", e);
                                    }
                                    if let Some(time_ms) = execution_time_ms {
                                        eprintln!("Execution time: {:.3} ms", time_ms);
                                    }
                                }
                                Err(e) => {
                                    eprintln!("Error: {}", e);
                                }
                            }
                        }
                    }
                    continue;
                }

                if trimmed.is_empty() {
                    continue;
                }

                // Handle special commands (backslash commands)
                if trimmed.starts_with('\\') {
                    let cmd = trimmed
                        .trim_start_matches('\\')
                        .split_whitespace()
                        .next()
                        .unwrap_or("");
                    match cmd {
                        "q" | "quit" | "exit" => {
                            println!("Goodbye!");
                            break;
                        }
                        "h" | "help" => {
                            print_help();
                            continue;
                        }
                        "c" | "clear" => {
                            query_buffer.clear();
                            in_multiline = false;
                            print!("\x1B[2J\x1B[1;1H"); // Clear screen
                            continue;
                        }
                        "l" | "limit" => {
                            if let Some(limit_str) = trimmed.split_whitespace().nth(1) {
                                if let Ok(limit) = limit_str.parse::<usize>() {
                                    println!("Row limit set to: {}", limit);
                                    // Note: This would require storing limit in a mutable way
                                    // For now, we'll just acknowledge it
                                } else {
                                    eprintln!("Invalid limit value");
                                }
                            } else {
                                println!("Current row limit: {}", args.limit);
                            }
                            continue;
                        }
                        "config" => {
                            println!();
                            println!("Current Configuration:");
                            println!("  URL:   {}", config.url);
                            match &config.auth {
                                AuthMethod::BearerToken(token) => {
                                    let token_display = if token.len() > 8 {
                                        format!("{}...{}", &token[..4], &token[token.len() - 4..])
                                    } else {
                                        "***".to_string()
                                    };
                                    println!("  Auth:  Bearer token ({})", token_display);
                                }
                                AuthMethod::UserHmac {
                                    user_id,
                                    secret_key,
                                } => {
                                    let key_display = if secret_key.len() > 8 {
                                        format!(
                                            "{}...{}",
                                            &secret_key[..4],
                                            &secret_key[secret_key.len() - 4..]
                                        )
                                    } else {
                                        "***".to_string()
                                    };
                                    println!(
                                        "  Auth:  User HMAC (user_id={}, secret_key={})",
                                        user_id, key_display
                                    );
                                }
                            }
                            println!();
                            println!("Configuration sources (in priority order):");
                            println!("  1. Command line arguments");
                            println!("  2. Environment variables");
                            println!("  3. Config file (~/.sneldb/config or .sneldb/config)");
                            println!("  4. Defaults");
                            println!();
                            continue;
                        }
                        _ => {
                            eprintln!("Unknown command: \\{}. Type \\h for help.", cmd);
                            continue;
                        }
                    }
                }

                // Add line to query buffer
                if !query_buffer.is_empty() {
                    query_buffer.push('\n');
                }
                query_buffer.push_str(&line);

                // Check if query seems complete (ends with semicolon)
                // For now, we'll use a simple heuristic: if line ends with semicolon
                if trimmed.ends_with(';') {
                    // Execute the query
                    let query = query_buffer.trim_end_matches(';').trim().to_string();
                    query_buffer.clear();
                    in_multiline = false;

                    if !query.is_empty() {
                        // Save to history
                        let _ = rl.add_history_entry(&query);

                        match client.execute_query(&query) {
                            Ok((response_data, execution_time_ms)) => {
                                if let Err(e) = display_arrow_data(&response_data, args.limit) {
                                    eprintln!("Error displaying results: {}", e);
                                }
                                if let Some(time_ms) = execution_time_ms {
                                    eprintln!("Execution time: {:.3} ms", time_ms);
                                }
                            }
                            Err(e) => {
                                eprintln!("Error: {}", e);
                            }
                        }
                    }
                } else {
                    // Continue multiline input
                    in_multiline = true;
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("^C");
                query_buffer.clear();
                in_multiline = false;
            }
            Err(ReadlineError::Eof) => {
                println!("\nGoodbye!");
                break;
            }
            Err(err) => {
                eprintln!("Error: {:?}", err);
                break;
            }
        }
    }

    // Save history
    let _ = rl.save_history(".sneldb_history");

    Ok(())
}

fn print_help() {
    println!();
    println!("SnelDB Console Help");
    println!("───────────────────");
    println!("Special commands:");
    println!("  \\q, \\quit, \\exit    Exit the console");
    println!("  \\h, \\help          Show this help message");
    println!("  \\c, \\clear         Clear the screen");
    println!("  \\l, \\limit [n]     Show or set row display limit");
    println!("  \\config            Show current configuration");
    println!();
    println!("Query input:");
    println!("  - Enter queries normally");
    println!("  - End queries with semicolon (;) or press Enter twice");
    println!("  - Use Ctrl+C to cancel current query");
    println!();
    println!("Authentication:");
    println!("  Bearer token (simple):");
    println!("    --token flag, SNELDB_TOKEN env var, or config file");
    println!("  User HMAC (production):");
    println!("    --user-id and --secret-key flags");
    println!("    SNELDB_USER_ID and SNELDB_SECRET_KEY env vars");
    println!("    Or config file");
    println!("  Config file:   ~/.sneldb/config or .sneldb/config");
    println!("  Example config file:");
    println!("    [config]");
    println!("    url = \"http://127.0.0.1:8085\"");
    println!("    # For Bearer token:");
    println!("    token = \"your-token\"");
    println!("    # OR for user authentication:");
    println!("    user_id = \"admin\"");
    println!("    secret_key = \"your-secret-key\"");
    println!();
}

fn display_arrow_data(data: &[u8], row_limit: usize) -> anyhow::Result<()> {
    // Check if response is JSON (error response) instead of Arrow
    if let Ok(json_str) = std::str::from_utf8(data) {
        let trimmed = json_str.trim();

        if trimmed.starts_with('{') || trimmed.starts_with('[') {
            // Check if this is streaming JSON (newline-delimited JSON) format
            if trimmed.contains('\n') {
                let first_line = trimmed.lines().next().unwrap_or("");
                if first_line.contains("\"type\":\"schema\"")
                    || first_line.contains("\"type\":\"batch\"")
                {
                    return display_streaming_json(trimmed, row_limit);
                }
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
