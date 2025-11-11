use anyhow::{Context, Result};
use hex;
use hmac::{Hmac, Mac};
use rand::Rng;
use rand::distributions::{Alphanumeric, DistString};
use serde_json::{Value, json};
use sha2::Sha256;
use snel_db::shared::config::CONFIG;
use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use sysinfo::System;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};

type HmacSha256 = Hmac<Sha256>;

#[tokio::main]
async fn main() -> Result<()> {
    let _ = snel_db::logging::init();

    // Parameters
    let total_events: usize = std::env::var("SNEL_STRESS_EVENTS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000_000);
    let concurrency: usize = std::env::var("SNEL_STRESS_CONCURRENCY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(256);
    let context_pool: usize = std::env::var("SNEL_STRESS_CONTEXTS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000);
    // Support multiple event types for sequence query testing
    // SNEL_STRESS_EVENT_TYPES (comma-separated) takes precedence over SNEL_STRESS_EVENT_TYPE
    let event_types: Vec<String> = std::env::var("SNEL_STRESS_EVENT_TYPES")
        .ok()
        .map(|s| {
            s.split(',')
                .map(|x| x.trim().to_string())
                .filter(|x| !x.is_empty())
                .collect()
        })
        .or_else(|| {
            std::env::var("SNEL_STRESS_EVENT_TYPE")
                .ok()
                .map(|s| vec![s])
        })
        .unwrap_or_else(|| vec!["stress_evt".to_string()]);

    // Link field for sequence queries (default: user_id)
    let link_field =
        std::env::var("SNEL_STRESS_LINK_FIELD").unwrap_or_else(|_| "user_id".to_string());

    // Sample context id for replay/query timings
    let sample_ctx =
        std::env::var("SNEL_STRESS_SAMPLE_CTX").unwrap_or_else(|_| "ctx-5000".to_string());
    // Optional wait timeout like `nc -w <secs>` for rw operations
    let wait_secs: Option<u64> = std::env::var("SNEL_STRESS_WAIT_SECS")
        .ok()
        .and_then(|s| s.parse().ok());
    let wait_dur: Option<Duration> = wait_secs.map(Duration::from_secs);

    // Time range for generated datetime field `created_at`
    // Defaults to last 30 days; can be overridden via env:
    // - SNEL_STRESS_CREATED_AT_DAYS or SNEL_STRESS_TS_DAYS: number of days in the past (default 30)
    // - SNEL_STRESS_CREATED_AT_START or SNEL_STRESS_TS_START: epoch seconds start (optional)
    // - SNEL_STRESS_CREATED_AT_END or SNEL_STRESS_TS_END: epoch seconds end (optional)
    let ts_days: i64 = std::env::var("SNEL_STRESS_CREATED_AT_DAYS")
        .ok()
        .or_else(|| std::env::var("SNEL_STRESS_TS_DAYS").ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);
    let now_secs_i64: i64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let default_start = now_secs_i64 - ts_days * 86_400;
    let ts_start: i64 = std::env::var("SNEL_STRESS_CREATED_AT_START")
        .ok()
        .or_else(|| std::env::var("SNEL_STRESS_TS_START").ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(default_start);
    let ts_end: i64 = std::env::var("SNEL_STRESS_CREATED_AT_END")
        .ok()
        .or_else(|| std::env::var("SNEL_STRESS_TS_END").ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(now_secs_i64);

    let addr = &CONFIG.server.tcp_addr;
    println!("Connecting to {}...", addr);

    // One control connection for admin commands and query
    let control = TcpStream::connect(addr)
        .await
        .with_context(|| format!("failed to connect to {}", addr))?;
    control.set_nodelay(true)?;
    let (control_reader_half, control_writer_half) = control.into_split();
    let mut control_reader = BufReader::new(control_reader_half);
    let mut control_writer = control_writer_half;

    // Create a user for authentication first (doesn't require auth)
    let user_id = "stress_user".to_string();
    println!("Creating user '{}'...", user_id);
    let create_user_cmd = format!("CREATE USER {}\n", user_id);
    let secret_key = match send_and_extract_secret_key(
        &mut control_reader,
        &mut control_writer,
        &create_user_cmd,
        wait_dur,
    )
    .await
    {
        Ok(key) => {
            println!("User created successfully. Secret key: {}", key);
            key
        }
        Err(e) => {
            eprintln!("Failed to create user: {}. Continuing without auth...", e);
            return Err(e);
        }
    };

    // Authenticate the control connection before defining schemas
    println!("Authenticating control connection...");
    let auth_signature = compute_hmac(&secret_key, &user_id);
    let auth_cmd = format!("AUTH {}:{}\n", user_id, auth_signature);
    match send_and_check_ok(
        &mut control_reader,
        &mut control_writer,
        &auth_cmd,
        wait_dur,
    )
    .await
    {
        Ok(_) => println!("Control connection authenticated"),
        Err(e) => {
            eprintln!("Failed to authenticate control connection: {}", e);
            return Err(e);
        }
    }

    // Define schemas for all event types (now authenticated)
    // All event types share the same schema with link_field for sequence queries
    println!(
        "Defining schemas for {} event type(s): {:?}",
        event_types.len(),
        event_types
    );
    for event_type in &event_types {
        let schema_cmd = format!(
            "DEFINE {} FIELDS {{ id: \"u64\", v: \"string\", flag: \"bool\", created_at: \"datetime\", {}: \"u64\", plan: [\"type01\", \"type02\", \"type03\", \"type04\", \"type05\", \"type06\", \"type07\", \"type08\", \"type09\", \"type10\", \"type11\", \"type12\", \"type13\", \"type14\", \"type15\", \"type16\", \"type17\", \"type18\", \"type19\", \"type20\"] }}\n",
            event_type, link_field
        );
        // Sign the DEFINE command
        let cmd_trimmed = schema_cmd.trim();
        let signature = compute_hmac(&secret_key, cmd_trimmed);
        let authenticated_cmd = format!("{}:{}\n", signature, cmd_trimmed);
        send_and_drain(
            &mut control_reader,
            &mut control_writer,
            &authenticated_cmd,
            wait_dur,
        )
        .await?;
    }

    // Pre-generate contexts
    let contexts: Vec<String> = (0..context_pool).map(|i| format!("ctx-{}", i)).collect();

    // Throughput tracking
    let start = Instant::now();
    let sent = Arc::new(AtomicUsize::new(0));

    // Sysinfo sampler
    let stop_flag = Arc::new(tokio::sync::Notify::new());
    let stop_flag_clone = stop_flag.clone();
    let sampler = tokio::spawn(async move {
        let mut sys = System::new_all();
        loop {
            tokio::select! {
                _ = stop_flag_clone.notified() => { break; }
                _ = sleep(Duration::from_secs(1)) => {
                    sys.refresh_all();
                    let total_mem = sys.total_memory();
                    let used_mem = sys.used_memory();
                    let la = System::load_average();
                    println!("[SYS] load_avg={:.2} used_mem={}MB/{}MB", la.one, used_mem / 1024 / 1024, total_mem / 1024 / 1024);
                }
            }
        }
    });

    // Progress reporter
    let sent_clone = sent.clone();
    let reporter = tokio::spawn(async move {
        let mut last = 0usize;
        let mut last_t = Instant::now();
        let mut zero_progress_count = 0u32;
        loop {
            sleep(Duration::from_secs(1)).await;
            let now = Instant::now();
            let cur = sent_clone.load(std::sync::atomic::Ordering::Relaxed);
            let dt = now.duration_since(last_t).as_secs_f64();
            let d = cur - last;
            println!("[PROG] total={} (+{}) {:.0} ev/s", cur, d, (d as f64) / dt);
            last = cur;
            last_t = now;

            // Exit if we've reached total events
            if cur >= total_events {
                break;
            }

            // Exit if no progress for 5 seconds (workers likely stopped)
            if d == 0 {
                zero_progress_count += 1;
                if zero_progress_count >= 5 {
                    break;
                }
            } else {
                zero_progress_count = 0;
            }
        }
    });

    // Writer pool with bounded concurrency and connection workers
    let connections = std::env::var("SNEL_STRESS_CONNECTIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(16);
    // Create per-worker channels
    let mut tx_vec = Vec::new();
    let mut rxs = Vec::new();
    for _ in 0..connections {
        let (txi, rxi) = mpsc::channel::<String>(concurrency);
        tx_vec.push(txi);
        rxs.push(rxi);
    }

    // Spawn workers with pipelined writes
    let mut worker_handles = Vec::new();
    for mut rx_local in rxs {
        let addr_clone = addr.clone();
        let sent_inner = sent.clone();
        let user_id_clone = user_id.clone();
        let secret_key_clone = secret_key.clone();
        let handle = tokio::spawn(async move {
            let stream = match TcpStream::connect(addr_clone.clone()).await {
                Ok(s) => s,
                Err(_) => return,
            };
            let _ = stream.set_nodelay(true);

            // Split stream into reader and writer
            let (reader_half, writer_half) = stream.into_split();
            let mut reader = BufReader::new(reader_half);
            let mut writer = writer_half;

            // Authenticate this connection
            let auth_signature = compute_hmac(&secret_key_clone, &user_id_clone);
            let auth_cmd = format!("AUTH {}:{}\n", user_id_clone, auth_signature);
            if let Err(_) = writer.write_all(auth_cmd.as_bytes()).await {
                return; // Failed to authenticate
            }
            if let Err(_) = writer.flush().await {
                return;
            }
            // Read OK response
            let mut response = String::new();
            if let Err(_) = read_line_with_timeout(&mut reader, &mut response, None).await {
                return; // Failed to read response
            }
            if !response.trim().starts_with("OK") {
                return; // Authentication failed
            }

            // Spawn background task to drain responses (abortable)
            let response_drainer = tokio::spawn(async move {
                let mut line = String::new();
                loop {
                    line.clear();
                    match reader.read_line(&mut line).await {
                        Ok(0) | Err(_) => break, // EOF or error
                        Ok(_) => {
                            // Check for backpressure messages and print them
                            let trimmed = line.trim();
                            if !trimmed.is_empty() {
                                let lower = trimmed.to_lowercase();
                                // Check for various backpressure/shutdown indicators
                                if lower.contains("under pressure")
                                    || lower.contains("service unavailable")
                                    || trimmed.starts_with("503")
                                    || trimmed.contains("ERROR:")
                                    || trimmed.contains("503 Service")
                                    || lower.contains("shutting down")
                                {
                                    eprintln!("[BACKPRESSURE] {}", trimmed);
                                }
                            }
                            continue; // Keep draining
                        }
                    }
                }
            });

            // Get abort handle to cancel drainer when done
            let drainer_abort = response_drainer.abort_handle();

            // Main writer loop - pipeline requests without waiting
            while let Some(cmd) = rx_local.recv().await {
                // Compute signature for the command (without newline)
                let cmd_trimmed = cmd.trim();
                let signature = compute_hmac(&secret_key_clone, cmd_trimmed);
                let authenticated_cmd = format!("{}:{}\n", signature, cmd_trimmed);

                match writer.write_all(authenticated_cmd.as_bytes()).await {
                    Ok(_) => {
                        sent_inner.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) if e.kind() == ErrorKind::BrokenPipe => {
                        // Try to reconnect once
                        if let Ok(fresh) = TcpStream::connect(addr_clone.clone()).await {
                            let _ = fresh.set_nodelay(true);
                            let (new_reader_half, mut new_writer_half) = fresh.into_split();
                            let mut new_reader = BufReader::new(new_reader_half);

                            // Re-authenticate
                            let auth_signature = compute_hmac(&secret_key_clone, &user_id_clone);
                            let auth_cmd = format!("AUTH {}:{}\n", user_id_clone, auth_signature);
                            if let Err(_) = new_writer_half.write_all(auth_cmd.as_bytes()).await {
                                break;
                            }
                            if let Err(_) = new_writer_half.flush().await {
                                break;
                            }
                            let mut response = String::new();
                            if let Err(_) =
                                read_line_with_timeout(&mut new_reader, &mut response, None).await
                            {
                                break;
                            }
                            if !response.trim().starts_with("OK") {
                                break;
                            }

                            writer = new_writer_half;
                            let signature = compute_hmac(&secret_key_clone, cmd_trimmed);
                            let authenticated_cmd = format!("{}:{}\n", signature, cmd_trimmed);
                            let _ = writer.write_all(authenticated_cmd.as_bytes()).await;
                            sent_inner.fetch_add(1, Ordering::Relaxed);
                        }
                        break; // Give up if reconnect fails
                    }
                    Err(_) => break, // Other errors, stop this worker
                }
            }

            // Abort the response drainer since we're done writing
            drainer_abort.abort();
            let _ = response_drainer.await; // Wait for cancellation (should be immediate)
        });
        worker_handles.push(handle);
    }

    // Produce jobs - distribute events across event types
    // For sequence queries, we need some user_ids to have multiple event types
    // Strategy: Assign user_id independently from event_type to ensure overlap
    let mut rng = rand::thread_rng();
    for i in 0..total_events {
        let ctx_id = &contexts[i % context_pool];
        // Select event type based on index (round-robin distribution)
        let event_type = &event_types[i % event_types.len()];
        // Generate user_id independently from event_type to ensure some user_ids have multiple event types
        // Use random selection from context_pool to create overlap between event types
        let user_id = rng.gen_range(0..context_pool);
        let evt = random_event_payload(i as u64, ts_start, ts_end, user_id as u64, &link_field);
        let cmd = format!("STORE {} FOR {} PAYLOAD {}\n", event_type, ctx_id, evt);
        let txi = &tx_vec[i % tx_vec.len()];
        let _ = txi.send(cmd).await; // backpressure via channel
    }
    drop(tx_vec); // close channels

    // Wait for workers to finish draining
    for h in worker_handles {
        let _ = h.await;
    }

    let elapsed = start.elapsed().as_secs_f64();
    println!(
        "Ingested {} events in {:.2}s ({:.0} ev/s)",
        total_events,
        elapsed,
        (total_events as f64) / elapsed
    );

    // Stop sampler and reporter
    stop_flag.notify_waiters();
    let _ = sampler.await;
    let _ = reporter.await;

    // Run REPLAY to sample latency (only if control connection is still alive)
    // Use first event type for replay
    if let Some(first_event_type) = event_types.first() {
        if let Ok(replay_result) = timeout(Duration::from_secs(2), async {
            let replay_cmd = format!("REPLAY {} FOR {}\n", first_event_type, sample_ctx);
            // Sign the REPLAY command
            let cmd_trimmed = replay_cmd.trim();
            let signature = compute_hmac(&secret_key, cmd_trimmed);
            let authenticated_cmd = format!("{}:{}\n", signature, cmd_trimmed);
            let t0 = Instant::now();
            send_and_collect_json_with_timeout(
                &mut control_reader,
                &mut control_writer,
                &authenticated_cmd,
                10,
                wait_dur,
            )
            .await?;
            println!(
                "Replay latency: {:.2} ms",
                t0.elapsed().as_secs_f64() * 1000.0
            );
            Ok::<(), anyhow::Error>(())
        })
        .await
        {
            let _ = replay_result;
        } else {
            eprintln!("Skipping replay (connection closed)");
        }
    }

    // Run QUERY (scoped) to sample latency over time using `created_at` (only if connection alive)
    // Use first event type for regular query
    if let Some(first_event_type) = event_types.first() {
        if let Ok(query_result) = timeout(Duration::from_secs(2), async {
            let since_secs = now_secs_i64 - 86_400; // last 24h
            let query_cmd = format!(
                "QUERY {} SINCE {} USING created_at WHERE id < 100\n",
                first_event_type, since_secs
            );
            // Sign the QUERY command
            let cmd_trimmed = query_cmd.trim();
            let signature = compute_hmac(&secret_key, cmd_trimmed);
            let authenticated_cmd = format!("{}:{}\n", signature, cmd_trimmed);
            let t1 = Instant::now();
            send_and_collect_json_with_timeout(
                &mut control_reader,
                &mut control_writer,
                &authenticated_cmd,
                10,
                wait_dur,
            )
            .await?;
            println!(
                "Query latency: {:.2} ms",
                t1.elapsed().as_secs_f64() * 1000.0
            );
            Ok::<(), anyhow::Error>(())
        })
        .await
        {
            let _ = query_result;
        } else {
            eprintln!("Skipping query (connection closed)");
        }
    }

    Ok(())
}

fn random_event_payload(
    seq: u64,
    ts_start: i64,
    ts_end: i64,
    user_id: u64,
    link_field: &str,
) -> String {
    let v = Alphanumeric.sample_string(&mut rand::thread_rng(), 12);
    let plan = format!("type{:02}", (seq % 20) + 1);
    let mut rng = rand::thread_rng();
    let low = ts_start.min(ts_end);
    let high = ts_start.max(ts_end);
    let ts = rng.gen_range(low..=high);
    let mut obj = json!({
        "id": seq,
        "v": v,
        "flag": (seq % 2 == 0),
        "created_at": ts,
        "plan": plan
    });
    // Add link_field for sequence queries
    if let Value::Object(ref mut map) = obj {
        map.insert(link_field.to_string(), json!(user_id));
    }
    obj.to_string()
}

async fn send_and_drain<R: AsyncRead + Unpin, W: AsyncWriteExt + Unpin>(
    reader: &mut BufReader<R>,
    writer: &mut W,
    cmd: &str,
    wait: Option<Duration>,
) -> Result<()> {
    write_all_with_timeout_stream(writer, cmd.as_bytes(), wait).await?;
    let mut header = String::new();
    read_line_with_timeout(reader, &mut header, wait).await?;
    // Drain any following body lines until next prompt absence; here we assume single-line body for ok-lines
    Ok(())
}

async fn send_and_collect_json_with_timeout<R: AsyncRead + Unpin, W: AsyncWriteExt + Unpin>(
    reader: &mut BufReader<R>,
    writer: &mut W,
    cmd: &str,
    max_lines: usize,
    wait: Option<Duration>,
) -> Result<Vec<Value>> {
    write_all_with_timeout_stream(writer, cmd.as_bytes(), wait).await?;
    let mut header = String::new();
    read_line_with_timeout(reader, &mut header, wait).await?; // e.g., "200 OK"
    let mut out = Vec::new();
    for _ in 0..max_lines {
        let mut line = String::new();
        let n = read_line_with_timeout(reader, &mut line, wait).await?;
        if n == 0 {
            break;
        }
        if let Some(idx) = line.find(' ') {
            let json_part = &line[idx + 1..];
            if let Ok(val) = serde_json::from_str::<Value>(json_part.trim()) {
                out.push(val);
            } else {
                break;
            }
        }
    }
    Ok(out)
}

async fn write_all_with_timeout_stream<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    buf: &[u8],
    wait: Option<Duration>,
) -> std::io::Result<()> {
    if let Some(dur) = wait {
        match tokio::time::timeout(dur, writer.write_all(buf)).await {
            Ok(res) => res,
            Err(_) => Err(std::io::Error::new(ErrorKind::TimedOut, "write timeout")),
        }
    } else {
        writer.write_all(buf).await
    }
}

async fn read_line_with_timeout<R: AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
    line: &mut String,
    wait: Option<Duration>,
) -> std::io::Result<usize> {
    if let Some(dur) = wait {
        match tokio::time::timeout(dur, reader.read_line(line)).await {
            Ok(res) => res,
            Err(_) => Err(std::io::Error::new(ErrorKind::TimedOut, "read timeout")),
        }
    } else {
        reader.read_line(line).await
    }
}

/// Compute HMAC-SHA256 signature
fn compute_hmac(secret_key: &str, message: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret_key.as_bytes()).expect("HMAC can take key of any size");
    mac.update(message.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Send CREATE USER command and extract the secret key from response
async fn send_and_extract_secret_key<R: AsyncRead + Unpin, W: AsyncWriteExt + Unpin>(
    reader: &mut BufReader<R>,
    writer: &mut W,
    cmd: &str,
    wait: Option<Duration>,
) -> Result<String> {
    write_all_with_timeout_stream(writer, cmd.as_bytes(), wait).await?;

    // Read status line (e.g., "200 OK")
    let mut status = String::new();
    read_line_with_timeout(reader, &mut status, wait).await?;

    // Read "User '...' created" line
    let mut user_line = String::new();
    read_line_with_timeout(reader, &mut user_line, wait).await?;

    // Read "Secret key: ..." line
    let mut key_line = String::new();
    read_line_with_timeout(reader, &mut key_line, wait).await?;

    // Extract secret key from "Secret key: <key>"
    if let Some(key_part) = key_line.strip_prefix("Secret key: ") {
        Ok(key_part.trim().to_string())
    } else {
        Err(anyhow::anyhow!(
            "Failed to parse secret key from response: {}",
            key_line
        ))
    }
}

/// Send command and check for OK response
async fn send_and_check_ok<R: AsyncRead + Unpin, W: AsyncWriteExt + Unpin>(
    reader: &mut BufReader<R>,
    writer: &mut W,
    cmd: &str,
    wait: Option<Duration>,
) -> Result<()> {
    write_all_with_timeout_stream(writer, cmd.as_bytes(), wait).await?;
    let mut response = String::new();
    read_line_with_timeout(reader, &mut response, wait).await?;
    if response.trim().starts_with("OK") {
        Ok(())
    } else {
        Err(anyhow::anyhow!("Expected OK, got: {}", response.trim()))
    }
}
