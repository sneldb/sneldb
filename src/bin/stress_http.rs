use anyhow::Result;
use bytes::Bytes;
use hex;
use hmac::{Hmac, Mac};
use http_body_util::BodyExt;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use rand::Rng;
use rand::distributions::{Alphanumeric, DistString};
use serde_json::{Value, json};
use sha2::Sha256;
use snel_db::shared::config::CONFIG;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use sysinfo::System;
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

    // Time range for generated datetime field `created_at`
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

    // Choose endpoint: /command (line format) or /json-command (JSON format)
    let use_json = std::env::var("SNEL_STRESS_USE_JSON")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(false);

    let base_url = format!("http://{}", CONFIG.server.http_addr);
    let command_endpoint = if use_json {
        "/json-command"
    } else {
        "/command"
    };
    println!("Connecting to {}{}...", base_url, command_endpoint);

    // Create HTTP client
    let client: Client<HttpConnector, http_body_util::Full<Bytes>> =
        Client::builder(TokioExecutor::new()).build_http();

    // Get or create user for authentication
    let (user_id, secret_key) = if let Ok(provided_secret) = std::env::var("SNEL_STRESS_SECRET_KEY")
    {
        // Use provided user and secret key
        let provided_user =
            std::env::var("SNEL_STRESS_USER_ID").unwrap_or_else(|_| "stress_user".to_string());
        println!("Using provided user '{}' with secret key", provided_user);
        (provided_user, provided_secret)
    } else {
        // Try to create user or use existing
        let user_id =
            std::env::var("SNEL_STRESS_USER_ID").unwrap_or_else(|_| "stress_user".to_string());
        println!("Creating user '{}'...", user_id);
        let create_user_cmd = format!("CREATE USER {}\n", user_id);
        match create_user_via_http(&client, &base_url, &create_user_cmd).await {
            Ok(key) => {
                println!("User created successfully. Secret key: {}", key);
                (user_id, key)
            }
            Err(e) => {
                // Check if user already exists
                let error_msg = e.to_string();
                if error_msg.contains("User already exists") {
                    eprintln!(
                        "User '{}' already exists. Please provide SNEL_STRESS_SECRET_KEY or use a different user with SNEL_STRESS_USER_ID",
                        user_id
                    );
                    return Err(anyhow::anyhow!(
                        "User '{}' already exists. Set SNEL_STRESS_SECRET_KEY to use existing user, or SNEL_STRESS_USER_ID to create a different user",
                        user_id
                    ));
                }
                eprintln!("Failed to create user: {}. Exiting...", e);
                return Err(e);
            }
        }
    };

    // Define schemas for all event types
    println!(
        "Defining schemas for {} event type(s): {:?}",
        event_types.len(),
        event_types
    );
    for event_type in &event_types {
        let schema_cmd = if use_json {
            let schema_json = json!({
                "type": "Define",
                "eventType": event_type,
                "schema": {
                    "fields": {
                        "id": "u64",
                        "v": "string",
                        "flag": "bool",
                        "created_at": "datetime",
                        &link_field: "u64",
                        "plan": ["type01", "type02", "type03", "type04", "type05", "type06", "type07", "type08", "type09", "type10", "type11", "type12", "type13", "type14", "type15", "type16", "type17", "type18", "type19", "type20"]
                    }
                }
            });
            schema_json.to_string()
        } else {
            format!(
                "DEFINE {} FIELDS {{ id: \"u64\", v: \"string\", flag: \"bool\", created_at: \"datetime\", {}: \"u64\", plan: [\"type01\", \"type02\", \"type03\", \"type04\", \"type05\", \"type06\", \"type07\", \"type08\", \"type09\", \"type10\", \"type11\", \"type12\", \"type13\", \"type14\", \"type15\", \"type16\", \"type17\", \"type18\", \"type19\", \"type20\"] }}\n",
                event_type, link_field
            )
        };

        // Sign the command
        let cmd_trimmed = schema_cmd.trim();
        let signature = compute_hmac(&secret_key, cmd_trimmed);
        match send_authenticated_request(
            &client,
            &base_url,
            command_endpoint,
            &user_id,
            &signature,
            &schema_cmd,
            use_json,
        )
        .await
        {
            Ok(_) => {}
            Err(e) => {
                eprintln!("Failed to define schema for {}: {}", event_type, e);
                return Err(e);
            }
        }
    }

    // Pre-generate contexts
    let contexts: Vec<String> = (0..context_pool).map(|i| format!("ctx-{}", i)).collect();

    // Throughput tracking
    let start = Instant::now();
    let sent = Arc::new(AtomicUsize::new(0));
    let errors = Arc::new(AtomicUsize::new(0));
    let backpressure = Arc::new(AtomicUsize::new(0));
    let rate_limited = Arc::new(AtomicUsize::new(0));

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
    let errors_clone = errors.clone();
    let backpressure_clone = backpressure.clone();
    let rate_limited_clone = rate_limited.clone();
    let reporter = tokio::spawn(async move {
        let mut last = 0usize;
        let mut last_t = Instant::now();
        let mut zero_progress_count = 0u32;
        loop {
            sleep(Duration::from_secs(1)).await;
            let now = Instant::now();
            let cur = sent_clone.load(Ordering::Relaxed);
            let errs = errors_clone.load(Ordering::Relaxed);
            let bp = backpressure_clone.load(Ordering::Relaxed);
            let rl = rate_limited_clone.load(Ordering::Relaxed);
            let dt = now.duration_since(last_t).as_secs_f64();
            let d = cur - last;
            println!(
                "[PROG] total={} (+{}) {:.0} req/s errors={} backpressure={} rate_limited={}",
                cur,
                d,
                (d as f64) / dt,
                errs,
                bp,
                rl
            );
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

    // Worker pool with bounded concurrency
    let connections = std::env::var("SNEL_STRESS_CONNECTIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(16);
    let mut tx_vec = Vec::new();
    let mut rxs = Vec::new();
    for _ in 0..connections {
        let (txi, rxi) = mpsc::channel::<String>(concurrency);
        tx_vec.push(txi);
        rxs.push(rxi);
    }

    // Spawn workers with pipelined requests (like TCP version)
    let mut worker_handles = Vec::new();
    for mut rx_local in rxs {
        let client_clone = client.clone();
        let base_url_clone = base_url.clone();
        let endpoint_clone = command_endpoint.to_string();
        let sent_inner = sent.clone();
        let errors_inner = errors.clone();
        let backpressure_inner = backpressure.clone();
        let rate_limited_inner = rate_limited.clone();
        let user_id_clone = user_id.clone();
        let secret_key_clone = secret_key.clone();
        let use_json_clone = use_json;

        let handle = tokio::spawn(async move {
            // Limit concurrent requests to prevent connection exhaustion
            // Use a semaphore to bound in-flight requests per worker
            // This prevents spawning unlimited tasks that could exhaust file descriptors
            let max_inflight = std::cmp::min(concurrency, 256); // Cap at 256 per worker
            let semaphore = Arc::new(tokio::sync::Semaphore::new(max_inflight));

            // Main send loop - fire requests without waiting (like TCP version)
            // Hyper client handles connection pooling automatically
            while let Some(cmd) = rx_local.recv().await {
                // Compute signature for the command
                let cmd_trimmed = cmd.trim();
                let signature = compute_hmac(&secret_key_clone, cmd_trimmed);

                // Clone for background task
                let client_for_req = client_clone.clone();
                let base_url_for_req = base_url_clone.clone();
                let endpoint_for_req = endpoint_clone.clone();
                let user_id_for_req = user_id_clone.clone();
                let signature_for_req = signature.clone();
                let cmd_for_req = cmd.clone();
                let bp = backpressure_inner.clone();
                let rl = rate_limited_inner.clone();
                let err = errors_inner.clone();
                let permit = semaphore.clone().acquire_owned().await.unwrap();

                // Count as sent immediately (fire and forget - like TCP)
                sent_inner.fetch_add(1, Ordering::Relaxed);

                // Spawn request in background - don't block send loop
                tokio::spawn(async move {
                    let result = send_authenticated_request(
                        &client_for_req,
                        &base_url_for_req,
                        &endpoint_for_req,
                        &user_id_for_req,
                        &signature_for_req,
                        &cmd_for_req,
                        use_json_clone,
                    )
                    .await;

                    // Process response asynchronously (non-blocking)
                    match result {
                        Ok(status) => {
                            if status == 503 {
                                bp.fetch_add(1, Ordering::Relaxed);
                            } else if status == 429 {
                                rl.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Err(_) => {
                            err.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    drop(permit); // Release semaphore permit
                });
            }
        });
        worker_handles.push(handle);
    }

    // Produce jobs - distribute events across event types
    let mut rng = rand::thread_rng();
    for i in 0..total_events {
        let ctx_id = &contexts[i % context_pool];
        let event_type = &event_types[i % event_types.len()];
        let user_id_val = rng.gen_range(0..context_pool);

        let cmd = if use_json {
            let payload = random_event_payload_json(
                i as u64,
                ts_start,
                ts_end,
                user_id_val as u64,
                &link_field,
            );
            json!({
                "type": "Store",
                "eventType": event_type,
                "contextId": ctx_id,
                "payload": payload
            })
            .to_string()
        } else {
            let evt =
                random_event_payload(i as u64, ts_start, ts_end, user_id_val as u64, &link_field);
            format!("STORE {} FOR {} PAYLOAD {}\n", event_type, ctx_id, evt)
        };

        let txi = &tx_vec[i % tx_vec.len()];
        let _ = txi.send(cmd).await;
    }
    drop(tx_vec); // close channels

    // Wait for workers to finish
    for h in worker_handles {
        let _ = h.await;
    }

    let elapsed = start.elapsed().as_secs_f64();
    let total_sent = sent.load(Ordering::Relaxed);
    let total_errors = errors.load(Ordering::Relaxed);
    let total_backpressure = backpressure.load(Ordering::Relaxed);
    let total_rate_limited = rate_limited.load(Ordering::Relaxed);

    println!(
        "Ingested {} events in {:.2}s ({:.0} req/s)",
        total_sent,
        elapsed,
        (total_sent as f64) / elapsed
    );
    println!(
        "Errors: {} | Backpressure (503): {} | Rate Limited (429): {}",
        total_errors, total_backpressure, total_rate_limited
    );

    // Stop sampler and reporter
    stop_flag.notify_waiters();
    let _ = sampler.await;
    let _ = reporter.await;

    // Run REPLAY to sample latency
    if let Some(first_event_type) = event_types.first() {
        if let Ok(replay_result) = timeout(Duration::from_secs(2), async {
            let replay_cmd = if use_json {
                json!({
                    "type": "Replay",
                    "eventType": first_event_type,
                    "contextId": sample_ctx
                })
                .to_string()
            } else {
                format!("REPLAY {} FOR {}\n", first_event_type, sample_ctx)
            };

            let cmd_trimmed = replay_cmd.trim();
            let signature = compute_hmac(&secret_key, cmd_trimmed);
            let t0 = Instant::now();
            match send_authenticated_request(
                &client,
                &base_url,
                command_endpoint,
                &user_id,
                &signature,
                &replay_cmd,
                use_json,
            )
            .await
            {
                Ok(_) => {
                    println!(
                        "Replay latency: {:.2} ms",
                        t0.elapsed().as_secs_f64() * 1000.0
                    );
                }
                Err(e) => {
                    eprintln!("Replay failed: {}", e);
                }
            }
            Ok::<(), anyhow::Error>(())
        })
        .await
        {
            let _ = replay_result;
        } else {
            eprintln!("Skipping replay (timeout)");
        }
    }

    // Run QUERY to sample latency
    if let Some(first_event_type) = event_types.first() {
        if let Ok(query_result) = timeout(Duration::from_secs(2), async {
            let since_secs = now_secs_i64 - 86_400; // last 24h
            let query_cmd = if use_json {
                json!({
                    "type": "Query",
                    "eventType": first_event_type,
                    "since": since_secs.to_string(),
                    "timeField": "created_at",
                    "where": {
                        "field": "id",
                        "op": "lt",
                        "value": 100
                    }
                })
                .to_string()
            } else {
                format!(
                    "QUERY {} SINCE {} USING created_at WHERE id < 100\n",
                    first_event_type, since_secs
                )
            };

            let cmd_trimmed = query_cmd.trim();
            let signature = compute_hmac(&secret_key, cmd_trimmed);
            let t1 = Instant::now();
            match send_authenticated_request(
                &client,
                &base_url,
                command_endpoint,
                &user_id,
                &signature,
                &query_cmd,
                use_json,
            )
            .await
            {
                Ok(_) => {
                    println!(
                        "Query latency: {:.2} ms",
                        t1.elapsed().as_secs_f64() * 1000.0
                    );
                }
                Err(e) => {
                    eprintln!("Query failed: {}", e);
                }
            }
            Ok::<(), anyhow::Error>(())
        })
        .await
        {
            let _ = query_result;
        } else {
            eprintln!("Skipping query (timeout)");
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
    if let Value::Object(ref mut map) = obj {
        map.insert(link_field.to_string(), json!(user_id));
    }
    obj.to_string()
}

fn random_event_payload_json(
    seq: u64,
    ts_start: i64,
    ts_end: i64,
    user_id: u64,
    link_field: &str,
) -> Value {
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
    if let Value::Object(ref mut map) = obj {
        map.insert(link_field.to_string(), json!(user_id));
    }
    obj
}

/// Compute HMAC-SHA256 signature
fn compute_hmac(secret_key: &str, message: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret_key.as_bytes()).expect("HMAC can take key of any size");
    mac.update(message.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Create user via HTTP and extract secret key
async fn create_user_via_http(
    client: &Client<HttpConnector, http_body_util::Full<Bytes>>,
    base_url: &str,
    cmd: &str,
) -> Result<String> {
    let uri: hyper::Uri = format!("{}/command", base_url).parse()?;
    let body = http_body_util::Full::new(Bytes::from(cmd.to_string()));
    let req = hyper::Request::builder()
        .method(hyper::Method::POST)
        .uri(uri)
        .header(hyper::header::CONTENT_TYPE, "text/plain")
        .header(
            hyper::header::AUTHORIZATION,
            format!("Bearer {}", CONFIG.server.auth_token),
        )
        .body(body)?;

    let res = client.request(req).await?;
    let status = res.status();
    let body_bytes = res.collect().await?.to_bytes();
    let body_str = String::from_utf8_lossy(&body_bytes);

    if !status.is_success() {
        return Err(anyhow::anyhow!(
            "Failed to create user: status={} body={}",
            status,
            body_str
        ));
    }

    // Parse response to extract secret key
    // HTTP responses are JSON format: {"count":1,"message":"OK","results":["User '...' created","Secret key: <key>"],"status":200}
    if let Ok(json) = serde_json::from_str::<serde_json::Value>(&body_str) {
        if let Some(results) = json.get("results").and_then(|r| r.as_array()) {
            for result in results {
                if let Some(result_str) = result.as_str() {
                    if let Some(key_part) = result_str.strip_prefix("Secret key: ") {
                        return Ok(key_part.trim().to_string());
                    }
                }
            }
        }
    }

    // Fallback: try line-based format (for text/plain responses)
    for line in body_str.lines() {
        if let Some(key_part) = line.strip_prefix("Secret key: ") {
            return Ok(key_part.trim().to_string());
        }
    }

    Err(anyhow::anyhow!(
        "Failed to parse secret key from response: {}",
        body_str
    ))
}

/// Send authenticated HTTP request with X-Auth-User and X-Auth-Signature headers
async fn send_authenticated_request(
    client: &Client<HttpConnector, http_body_util::Full<Bytes>>,
    base_url: &str,
    endpoint: &str,
    user_id: &str,
    signature: &str,
    body: &str,
    use_json: bool,
) -> Result<u16> {
    let uri: hyper::Uri = format!("{}{}", base_url, endpoint).parse()?;
    let content_type = if use_json {
        "application/json"
    } else {
        "text/plain"
    };

    let body_bytes = http_body_util::Full::new(Bytes::from(body.to_string()));
    let req = hyper::Request::builder()
        .method(hyper::Method::POST)
        .uri(uri)
        .header(hyper::header::CONTENT_TYPE, content_type)
        .header(
            hyper::header::AUTHORIZATION,
            format!("Bearer {}", CONFIG.server.auth_token),
        )
        .header("X-Auth-User", user_id)
        .header("X-Auth-Signature", signature)
        .body(body_bytes)?;

    let res = client.request(req).await?;
    let status = res.status().as_u16();

    // Drain response body (we don't need it for throughput testing)
    let _ = res.collect().await;

    Ok(status)
}
