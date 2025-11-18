use anyhow::{Context, Result};
use bytes::Bytes;
use hex;
use hmac::{Hmac, Mac};
use http_body_util::Full;
use hyper::{Method, Request};
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
use tokio::time::sleep;
use tracing::{debug, trace, warn};

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
    // Optional wait timeout for HTTP requests
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

    // Get admin user credentials from config (following new user management approach)
    let (user_id, secret_key) = match &CONFIG.auth {
        Some(auth_config) => {
            match (
                &auth_config.initial_admin_user,
                &auth_config.initial_admin_key,
            ) {
                (Some(admin_user), Some(admin_key)) => {
                    println!("Using admin user '{}' from config", admin_user);
                    (admin_user.clone(), admin_key.clone())
                }
                _ => {
                    anyhow::bail!(
                        "Admin user credentials not found in config. Please set initial_admin_user and initial_admin_key in [auth] section."
                    );
                }
            }
        }
        None => {
            anyhow::bail!(
                "Auth config not found. Please configure [auth] section with initial_admin_user and initial_admin_key."
            );
        }
    };

    let http_addr = &CONFIG.server.http_addr;
    let base_url = format!("http://{}/command", http_addr);
    let base_uri = base_url
        .parse::<hyper::Uri>()
        .with_context(|| format!("Invalid HTTP address: {}", http_addr))?;
    println!("Connecting to {}...", base_url);

    // Create HTTP client with aggressive connection pooling and reuse
    // Configure connector with connection limits and timeouts to prevent port exhaustion
    let mut connector = HttpConnector::new();
    connector.set_nodelay(true);
    connector.set_keepalive(Some(Duration::from_secs(30)));
    connector.set_connect_timeout(Some(Duration::from_secs(5)));

    // Increase max idle connections significantly to allow better connection reuse
    // Check if server has keep-alive enabled to adjust pool settings
    let server_keep_alive = std::env::var("SNELDB_HTTP_KEEP_ALIVE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(false);

    // Get connections value for pool sizing
    let connections = std::env::var("SNEL_STRESS_CONNECTIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(16);

    // With keep-alive, we can maintain a larger pool since connections are reused
    // Without keep-alive, we need to be more conservative
    // IMPORTANT: max_idle must not exceed server's connection limit (default 1024)
    // Otherwise connections accumulate and hit the server limit, causing failures
    let max_idle = if server_keep_alive {
        // With keep-alive: maintain connections matching concurrency + headroom
        // But cap at 512 to stay well below server limit (1024) and allow headroom
        // Connections are reused, so we only need enough for concurrent requests
        (concurrency * 2).max(connections * 2).min(512)
    } else {
        // Without keep-alive: more conservative since connections close immediately
        (concurrency * 2).max(128).min(1024)
    };

    // With keep-alive, connections stay open longer, but we need a reasonable timeout
    // Without keep-alive, connections close immediately, so shorter timeout is fine
    let idle_timeout = if server_keep_alive {
        Duration::from_secs(30) // 30 seconds - prevents indefinite accumulation
    } else {
        Duration::from_secs(30) // Shorter timeout since server closes connections
    };

    let http_client: Client<HttpConnector, Full<Bytes>> = Client::builder(TokioExecutor::new())
        .pool_max_idle_per_host(max_idle)
        .pool_idle_timeout(idle_timeout)
        .http1_allow_obsolete_multiline_headers_in_responses(true)
        .http1_title_case_headers(true)
        .build(connector);

    // Define schemas for all event types (using admin user from config)
    println!(
        "Defining schemas for {} event type(s): {:?}",
        event_types.len(),
        event_types
    );
    for event_type in &event_types {
        let schema_cmd = format!(
            "DEFINE {} FIELDS {{ id: \"u64\", v: \"string\", flag: \"bool\", created_at: \"datetime\", {}: \"u64\", plan: [\"type01\", \"type02\", \"type03\", \"type04\", \"type05\", \"type06\", \"type07\", \"type08\", \"type09\", \"type10\", \"type11\", \"type12\", \"type13\", \"type14\", \"type15\", \"type16\", \"type17\", \"type18\", \"type19\", \"type20\"] }}",
            event_type, link_field
        );
        // Sign the DEFINE command
        let cmd_trimmed = schema_cmd.trim();
        let signature = compute_hmac(&secret_key, cmd_trimmed);

        match send_http_command(
            &http_client,
            &base_url,
            &user_id,
            &signature,
            &schema_cmd,
            wait_dur,
        )
        .await
        {
            Ok(_) => println!("Schema defined for {}", event_type),
            Err(e) => {
                // Check if it's a "schema already defined" error - this is okay
                let err_str = e.to_string();
                if err_str.contains("already defined") {
                    println!("Schema for {} already exists, continuing...", event_type);
                } else {
                    warn!("Failed to define schema for {}: {}", event_type, e);
                    eprintln!("Failed to define schema for {}: {}", event_type, e);
                    return Err(e);
                }
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
    let skipped = Arc::new(AtomicUsize::new(0)); // Track skipped requests
    let error_401 = Arc::new(AtomicUsize::new(0)); // Track auth errors
    let error_500 = Arc::new(AtomicUsize::new(0)); // Track server errors
    let error_other = Arc::new(AtomicUsize::new(0)); // Track other errors

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
    let skipped_clone = skipped.clone();
    let error_401_clone = error_401.clone();
    let error_500_clone = error_500.clone();
    let error_other_clone = error_other.clone();
    let reporter = tokio::spawn(async move {
        let mut last = 0usize;
        let mut last_t = Instant::now();
        let mut zero_progress_count = 0u32;
        loop {
            sleep(Duration::from_secs(1)).await;
            let now = Instant::now();
            let cur = sent_clone.load(std::sync::atomic::Ordering::Relaxed);
            let errs = errors_clone.load(std::sync::atomic::Ordering::Relaxed);
            let bp = backpressure_clone.load(std::sync::atomic::Ordering::Relaxed);
            let sk = skipped_clone.load(std::sync::atomic::Ordering::Relaxed);
            let e401 = error_401_clone.load(std::sync::atomic::Ordering::Relaxed);
            let e500 = error_500_clone.load(std::sync::atomic::Ordering::Relaxed);
            let eother = error_other_clone.load(std::sync::atomic::Ordering::Relaxed);
            let dt = now.duration_since(last_t).as_secs_f64();
            let d = cur - last;

            if errs > 0 || bp > 0 || sk > 0 {
                println!(
                    "[PROG] total={} (+{}) {:.0} ev/s errors={} (401={} 500={} other={}) backpressure={} skipped={}",
                    cur,
                    d,
                    (d as f64) / dt,
                    errs,
                    e401,
                    e500,
                    eother,
                    bp,
                    sk
                );
            } else {
                println!("[PROG] total={} (+{}) {:.0} ev/s", cur, d, (d as f64) / dt);
            }
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
    // Note: connections variable is already defined above for pool sizing
    // Create per-worker channels
    let mut tx_vec = Vec::new();
    let mut rxs = Vec::new();
    for _ in 0..connections {
        let (txi, rxi) = tokio::sync::mpsc::channel::<String>(concurrency);
        tx_vec.push(txi);
        rxs.push(rxi);
    }

    // Use a channel-based worker pool to avoid skipping requests
    // Builders create requests and queue them, workers send them
    let http_client_arc = Arc::new(http_client);

    // Create a bounded channel for requests (prevents memory explosion)
    // With keep-alive, we can pipeline more requests since connections are reused
    // Without keep-alive, be more conservative
    let request_queue_size = if server_keep_alive {
        // With keep-alive: larger queue for better pipelining
        (concurrency * connections * 16).min(16384)
    } else {
        // Without keep-alive: smaller queue to prevent port exhaustion
        (concurrency * connections * 2).min(4096)
    };
    let (request_tx, request_rx) = tokio::sync::mpsc::channel::<(
        Request<Full<Bytes>>,
        Arc<Client<HttpConnector, Full<Bytes>>>,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
    )>(request_queue_size);

    // Spawn a fixed pool of HTTP request workers (one per connection)
    // Use a distributor pattern: single receiver distributes to workers via individual channels
    // This avoids mutex contention while still limiting concurrent requests
    let mut worker_channels = Vec::new();
    for _ in 0..connections {
        let (tx, rx) = tokio::sync::mpsc::channel::<(
            Request<Full<Bytes>>,
            Arc<Client<HttpConnector, Full<Bytes>>>,
            Arc<AtomicUsize>,
            Arc<AtomicUsize>,
            Arc<AtomicUsize>,
            Arc<AtomicUsize>,
            Arc<AtomicUsize>,
        )>(concurrency);
        worker_channels.push((tx, rx));
    }

    // Spawn distributor: pulls from main queue and distributes to workers round-robin
    let distributor_handle = {
        let mut request_rx_local = request_rx;
        let worker_txs: Vec<_> = worker_channels.iter().map(|(tx, _)| tx.clone()).collect();
        let mut worker_idx = 0;
        tokio::spawn(async move {
            while let Some(req_tuple) = request_rx_local.recv().await {
                // Round-robin distribution to workers
                let tx = &worker_txs[worker_idx % worker_txs.len()];
                if tx.send(req_tuple).await.is_err() {
                    break; // Worker channel closed
                }
                worker_idx += 1;
            }
            // Close all worker channels when main queue is done
            // Channels will close naturally when we drop the senders
            drop(worker_txs);
        })
    };

    // Connection attempt rate limiter - only needed without keep-alive
    // With keep-alive, connections are reused so we don't need aggressive rate limiting
    let connection_attempt_semaphore = if server_keep_alive {
        // With keep-alive: very permissive limit since connections are reused
        // Only limit to prevent initial connection burst
        Arc::new(tokio::sync::Semaphore::new((concurrency * 4).min(512)))
    } else {
        // Without keep-alive: more restrictive to prevent port exhaustion
        Arc::new(tokio::sync::Semaphore::new((concurrency * 2).min(256)))
    };

    // Spawn workers that pull from their own channels (no mutex contention!)
    let mut request_worker_handles = Vec::new();
    for (_, mut rx_local) in worker_channels {
        let connection_attempt_sem = connection_attempt_semaphore.clone();
        let server_keep_alive_worker = server_keep_alive;
        let handle = tokio::spawn(async move {
            // Track consecutive port exhaustion errors for exponential backoff
            // Only relevant without keep-alive
            let mut port_error_count = 0u32;

            while let Some((
                req,
                client,
                errors_worker,
                backpressure_worker,
                error_401_worker,
                error_500_worker,
                error_other_worker,
            )) = rx_local.recv().await
            {
                let request_start = Instant::now();

                // With keep-alive, connection attempts are rare (only initial connections)
                // Without keep-alive, we need to rate limit to prevent port exhaustion
                let _permit = if server_keep_alive_worker {
                    // With keep-alive: try_acquire to avoid blocking (connections are reused)
                    // If we can't get a permit immediately, proceed anyway (connection likely exists)
                    connection_attempt_sem.clone().try_acquire_owned().ok()
                } else {
                    // Without keep-alive: blocking acquire to rate limit connection attempts
                    Some(
                        connection_attempt_sem
                            .clone()
                            .acquire_owned()
                            .await
                            .unwrap(),
                    )
                };

                match client.request(req).await {
                    Ok(res) => {
                        // Reset port error count on success
                        port_error_count = 0;

                        let status = res.status();
                        let request_duration = request_start.elapsed();
                        trace!(
                            "HTTP response: status={}, duration={:?}",
                            status, request_duration
                        );

                        if status == hyper::StatusCode::SERVICE_UNAVAILABLE {
                            backpressure_worker.fetch_add(1, Ordering::Relaxed);
                            warn!("HTTP 503 Service Unavailable (backpressure)");
                        } else if !status.is_success() {
                            errors_worker.fetch_add(1, Ordering::Relaxed);

                            // Categorize errors
                            match status.as_u16() {
                                401 => {
                                    error_401_worker.fetch_add(1, Ordering::Relaxed);
                                    warn!("HTTP 401 Unauthorized");
                                }
                                500 => {
                                    error_500_worker.fetch_add(1, Ordering::Relaxed);
                                    warn!("HTTP 500 Internal Server Error");
                                }
                                _ => {
                                    error_other_worker.fetch_add(1, Ordering::Relaxed);
                                    warn!("HTTP error status: {}", status.as_u16());
                                }
                            }
                        }
                    }
                    Err(e) => {
                        let request_duration = request_start.elapsed();
                        errors_worker.fetch_add(1, Ordering::Relaxed);
                        error_other_worker.fetch_add(1, Ordering::Relaxed);

                        // Check if it's a port exhaustion error and add exponential backoff
                        let err_str = format!("{:?}", e);
                        let is_port_error = err_str.contains("Can't assign requested address")
                            || err_str.contains("AddrNotAvailable")
                            || err_str.contains("code: 49");

                        if is_port_error {
                            // Port exhaustion should be rare with keep-alive
                            // Only apply backoff if keep-alive is disabled
                            if !server_keep_alive_worker {
                                // Exponential backoff for port exhaustion errors
                                // Backoff increases with consecutive failures: 10ms, 20ms, 40ms, 80ms, max 200ms
                                port_error_count += 1;
                                let backoff_ms =
                                    (10u64 * (1u64 << port_error_count.min(4))).min(200);

                                warn!(
                                    "HTTP request failed (port exhaustion, consecutive={}): {:?}, duration={:?}, backing off {}ms",
                                    port_error_count, e, request_duration, backoff_ms
                                );
                                sleep(Duration::from_millis(backoff_ms)).await;
                            } else {
                                // With keep-alive, port exhaustion is unexpected - log but don't backoff
                                warn!(
                                    "HTTP request failed (port exhaustion with keep-alive?): {:?}, duration={:?}",
                                    e, request_duration
                                );
                            }
                        } else {
                            // Reset port error count on non-port errors
                            port_error_count = 0;
                            warn!(
                                "HTTP request failed: {:?}, duration={:?}",
                                e, request_duration
                            );
                        }
                    }
                }
            }
        });
        request_worker_handles.push(handle);
    }

    // Keep originals for final stats, clone for workers
    let error_401_original = error_401.clone();
    let error_500_original = error_500.clone();
    let error_other_original = error_other.clone();
    let mut worker_handles = Vec::new();

    // Spawn workers that build requests and queue them (no skipping - channel backpressures)
    for mut rx_local in rxs {
        let base_uri_clone = base_uri.clone();
        let http_client_clone = http_client_arc.clone();
        let sent_inner = sent.clone();
        let user_id_clone = user_id.clone();
        let secret_key_clone = secret_key.clone();
        let errors_clone = errors.clone();
        let backpressure_clone = backpressure.clone();
        let error_401_clone = error_401_original.clone();
        let error_500_clone = error_500_original.clone();
        let error_other_clone = error_other_original.clone();
        let request_tx_clone = request_tx.clone();
        let handle = tokio::spawn(async move {
            // Main writer loop - build requests and queue them
            while let Some(cmd) = rx_local.recv().await {
                // Compute signature for the command (without newline)
                let cmd_trimmed = cmd.trim();
                let signature = compute_hmac(&secret_key_clone, cmd_trimmed);

                // Build request (reuse parsed URI)
                let body = Full::new(Bytes::from(cmd.to_string()));
                let req = match Request::builder()
                    .method(Method::POST)
                    .uri(base_uri_clone.clone())
                    .header("Content-Type", "text/plain")
                    .header("X-Auth-User", &user_id_clone)
                    .header("X-Auth-Signature", &signature)
                    .body(body)
                {
                    Ok(r) => r,
                    Err(e) => {
                        warn!("Failed to build HTTP request: {:?}", e);
                        sent_inner.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                };

                // Queue request (backpressure via channel - no skipping!)
                // Channel will naturally backpressure when full, workers will drain it
                if request_tx_clone
                    .send((
                        req,
                        http_client_clone.clone(),
                        errors_clone.clone(),
                        backpressure_clone.clone(),
                        error_401_clone.clone(),
                        error_500_clone.clone(),
                        error_other_clone.clone(),
                    ))
                    .await
                    .is_ok()
                {
                    sent_inner.fetch_add(1, Ordering::Relaxed);
                } else {
                    // Channel closed, stop
                    warn!("Request channel closed, stopping worker");
                    break;
                }
            }
        });
        worker_handles.push(handle);
    }
    drop(request_tx); // Close channel when all builders are done

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
        let user_id_val = rng.gen_range(0..context_pool);
        let evt = random_event_payload(i as u64, ts_start, ts_end, user_id_val as u64, &link_field);
        let cmd = format!("STORE {} FOR {} PAYLOAD {}\n", event_type, ctx_id, evt);
        let txi = &tx_vec[i % tx_vec.len()];
        let _ = txi.send(cmd).await; // backpressure via channel
    }
    drop(tx_vec); // close channels

    // Wait for request builders to finish
    for h in worker_handles {
        let _ = h.await;
    }

    // Wait for distributor to finish
    let _ = distributor_handle.await;

    // Wait for request workers to finish draining their queues
    for h in request_worker_handles {
        let _ = h.await;
    }

    let elapsed = start.elapsed().as_secs_f64();
    let total_errors = errors.load(Ordering::Relaxed);
    let total_backpressure = backpressure.load(Ordering::Relaxed);
    let total_skipped = skipped.load(Ordering::Relaxed);
    let total_401 = error_401_original.load(Ordering::Relaxed);
    let total_500 = error_500_original.load(Ordering::Relaxed);
    let total_other = error_other_original.load(Ordering::Relaxed);

    println!(
        "Ingested {} events in {:.2}s ({:.0} ev/s)",
        total_events,
        elapsed,
        (total_events as f64) / elapsed
    );

    if total_errors > 0 || total_backpressure > 0 || total_skipped > 0 {
        println!(
            "WARNING: errors={} (401={} 500={} other={}) backpressure={} skipped={}",
            total_errors, total_401, total_500, total_other, total_backpressure, total_skipped
        );
    }

    debug!(
        "Final stats: sent={} errors={} backpressure={} skipped={}",
        total_events, total_errors, total_backpressure, total_skipped
    );

    // Stop sampler and reporter
    stop_flag.notify_waiters();
    let _ = sampler.await;
    let _ = reporter.await;

    // Run REPLAY to sample latency (only if connection is still alive)
    // Use first event type for replay
    if let Some(first_event_type) = event_types.first() {
        if let Ok(replay_result) = tokio::time::timeout(Duration::from_secs(2), async {
            let replay_cmd = format!("REPLAY {} FOR {}\n", first_event_type, sample_ctx);
            // Sign the REPLAY command
            let cmd_trimmed = replay_cmd.trim();
            let signature = compute_hmac(&secret_key, cmd_trimmed);
            let t0 = Instant::now();
            match send_http_command(
                &*http_client_arc,
                &base_url,
                &user_id,
                &signature,
                &replay_cmd,
                Some(Duration::from_secs(10)),
            )
            .await
            {
                Ok(_) => {
                    println!(
                        "Replay latency: {:.2} ms",
                        t0.elapsed().as_secs_f64() * 1000.0
                    );
                    Ok::<(), anyhow::Error>(())
                }
                Err(e) => Err(e),
            }
        })
        .await
        {
            let _ = replay_result;
        } else {
            eprintln!("Skipping replay (timeout)");
        }
    }

    // Run QUERY (scoped) to sample latency over time using `created_at` (only if connection alive)
    // Use first event type for regular query
    if let Some(first_event_type) = event_types.first() {
        if let Ok(query_result) = tokio::time::timeout(Duration::from_secs(2), async {
            let since_secs = now_secs_i64 - 86_400; // last 24h
            let query_cmd = format!(
                "QUERY {} SINCE {} USING created_at WHERE id < 100\n",
                first_event_type, since_secs
            );
            // Sign the QUERY command
            let cmd_trimmed = query_cmd.trim();
            let signature = compute_hmac(&secret_key, cmd_trimmed);
            let t1 = Instant::now();
            match send_http_command(
                &*http_client_arc,
                &base_url,
                &user_id,
                &signature,
                &query_cmd,
                Some(Duration::from_secs(10)),
            )
            .await
            {
                Ok(_) => {
                    println!(
                        "Query latency: {:.2} ms",
                        t1.elapsed().as_secs_f64() * 1000.0
                    );
                    Ok::<(), anyhow::Error>(())
                }
                Err(e) => Err(e),
            }
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
    // Add link_field for sequence queries
    if let Value::Object(ref mut map) = obj {
        map.insert(link_field.to_string(), json!(user_id));
    }
    obj.to_string()
}

/// Send HTTP command with authentication headers
async fn send_http_command(
    client: &Client<HttpConnector, Full<Bytes>>,
    url: &str,
    user_id: &str,
    signature: &str,
    cmd: &str,
    timeout: Option<Duration>,
) -> Result<()> {
    let uri = url
        .parse::<hyper::Uri>()
        .with_context(|| format!("Invalid URL: {}", url))?;

    let body = Full::new(Bytes::from(cmd.to_string()));
    let req = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header("Content-Type", "text/plain")
        .header("X-Auth-User", user_id)
        .header("X-Auth-Signature", signature)
        .body(body)
        .with_context(|| "Failed to build HTTP request")?;

    let res = match timeout {
        Some(dur) => tokio::time::timeout(dur, client.request(req))
            .await
            .with_context(|| "HTTP request timeout")?
            .with_context(|| "HTTP request failed")?,
        None => client
            .request(req)
            .await
            .with_context(|| "HTTP request failed")?,
    };

    let status = res.status();
    if !status.is_success() {
        // Read response body for error details
        let body_bytes = http_body_util::BodyExt::collect(res.into_body())
            .await
            .unwrap_or_default()
            .to_bytes();
        let body_str = String::from_utf8_lossy(&body_bytes);
        return Err(anyhow::anyhow!(
            "HTTP request failed with status {}: {}",
            status,
            body_str
        ));
    }

    Ok(())
}

/// Compute HMAC-SHA256 signature
fn compute_hmac(secret_key: &str, message: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret_key.as_bytes()).expect("HMAC can take key of any size");
    mac.update(message.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}
