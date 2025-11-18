use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use hex;
use hmac::{Hmac, Mac};
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
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

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
        .unwrap_or_else(|| vec!["stress_evt_ws".to_string()]);

    let link_field =
        std::env::var("SNEL_STRESS_LINK_FIELD").unwrap_or_else(|_| "user_id".to_string());

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

    let ws_url = format!("ws://{}/ws", CONFIG.server.ws_addr);
    println!("Connecting to {}...", ws_url);

    // Control WebSocket connection for admin commands
    let (control_ws, _) = connect_async(&ws_url)
        .await
        .with_context(|| format!("failed to connect to {}", ws_url))?;
    let (mut control_sender, mut control_receiver) = control_ws.split();

    // Get admin credentials
    let admin_user = CONFIG
        .auth
        .as_ref()
        .and_then(|a| a.initial_admin_user.as_ref())
        .map(|s| s.clone())
        .or_else(|| std::env::var("SNELDB_ADMIN_USER").ok())
        .unwrap_or_else(|| "admin".to_string());
    let admin_key = CONFIG
        .auth
        .as_ref()
        .and_then(|a| a.initial_admin_key.as_ref())
        .map(|s| s.clone())
        .or_else(|| std::env::var("SNELDB_ADMIN_KEY").ok())
        .unwrap_or_else(|| "admin-key-123".to_string());

    // Authenticate as admin and get session token
    println!("Authenticating as admin user '{}'...", admin_user);
    let admin_auth_signature = compute_hmac(&admin_key, &admin_user);
    let admin_auth_cmd = format!("AUTH {}:{}\n", admin_user, admin_auth_signature);

    control_sender.send(Message::Text(admin_auth_cmd)).await?;

    // Wait for OK TOKEN response
    let admin_token = loop {
        match control_receiver.next().await {
            Some(Ok(Message::Text(text))) => {
                let trimmed = text.trim();
                if trimmed.starts_with("OK TOKEN ") {
                    let token = trimmed.strip_prefix("OK TOKEN ").unwrap().trim();
                    println!("Admin authentication successful, token received");
                    break Some(token.to_string());
                } else if trimmed == "OK" {
                    println!("Admin authentication successful (no token)");
                    break None;
                } else if trimmed.starts_with("ERROR") {
                    return Err(anyhow::anyhow!("Admin authentication failed: {}", trimmed));
                }
            }
            Some(Ok(Message::Close(_))) => {
                return Err(anyhow::anyhow!("Connection closed during authentication"));
            }
            Some(Err(e)) => {
                return Err(anyhow::anyhow!(
                    "WebSocket error during authentication: {}",
                    e
                ));
            }
            None => {
                return Err(anyhow::anyhow!("Connection closed unexpectedly"));
            }
            _ => continue,
        }
    };

    // Create a user for stress testing
    let base_user_id = "stress_user";
    let (user_id, secret_key) = {
        let mut current_user_id = base_user_id.to_string();
        let mut user_key = None;

        loop {
            println!("Creating user '{}'...", current_user_id);
            let create_user_cmd = format!("CREATE USER {}\n", current_user_id);
            let cmd_trimmed = create_user_cmd.trim();
            let create_signature = compute_hmac(&admin_key, cmd_trimmed);

            // Use token if available, otherwise use HMAC
            let authenticated_cmd = if let Some(ref token) = admin_token {
                format!("{} TOKEN {}\n", cmd_trimmed, token)
            } else {
                format!("{}:{}\n", create_signature, cmd_trimmed)
            };

            control_sender
                .send(Message::Text(authenticated_cmd))
                .await?;

            // Read response (WebSocket sends multi-line response as single message)
            match control_receiver.next().await {
                Some(Ok(Message::Text(text))) => {
                    // Parse multi-line response
                    let lines: Vec<&str> = text.trim().lines().collect();
                    if lines.is_empty() {
                        continue;
                    }

                    let first_line = lines[0].trim();
                    if first_line.starts_with("200") {
                        // Success - parse the response lines
                        for line in &lines[1..] {
                            let trimmed = line.trim();
                            if trimmed.starts_with("Secret key: ") {
                                if let Some(key_part) = trimmed.strip_prefix("Secret key: ") {
                                    user_key = Some(key_part.trim().to_string());
                                    break;
                                }
                            }
                        }
                        if user_key.is_some() {
                            break; // Success!
                        }
                    } else if first_line.starts_with("400") {
                        // Check if it's "User already exists" error
                        let error_text = text.trim();
                        if error_text.contains("User already exists") {
                            // Try with timestamped username
                            let timestamp = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs();
                            current_user_id = format!("{}_{}", base_user_id, timestamp);
                            println!(
                                "User '{}' already exists, trying '{}'...",
                                base_user_id, current_user_id
                            );
                            continue; // Retry with new user_id
                        } else {
                            return Err(anyhow::anyhow!("CREATE USER failed: {}", error_text));
                        }
                    } else if first_line.starts_with("ERROR") {
                        return Err(anyhow::anyhow!("CREATE USER failed: {}", text.trim()));
                    }
                }
                Some(Ok(Message::Close(_))) => {
                    return Err(anyhow::anyhow!("Connection closed during user creation"));
                }
                Some(Err(e)) => {
                    return Err(anyhow::anyhow!("WebSocket error: {}", e));
                }
                None => {
                    return Err(anyhow::anyhow!("Connection closed unexpectedly"));
                }
                _ => continue,
            }
        }

        match user_key {
            Some(key) => {
                println!("User created successfully. Secret key: {}", key);
                (current_user_id, key)
            }
            None => {
                return Err(anyhow::anyhow!(
                    "Failed to extract secret key from response"
                ));
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
        let schema_cmd = format!(
            "DEFINE {} FIELDS {{ id: \"u64\", v: \"string\", flag: \"bool\", created_at: \"datetime\", {}: \"u64\", plan: [\"type01\", \"type02\", \"type03\", \"type04\", \"type05\", \"type06\", \"type07\", \"type08\", \"type09\", \"type10\", \"type11\", \"type12\", \"type13\", \"type14\", \"type15\", \"type16\", \"type17\", \"type18\", \"type19\", \"type20\"] }}\n",
            event_type, link_field
        );
        let cmd_trimmed = schema_cmd.trim();
        let signature = compute_hmac(&admin_key, cmd_trimmed);
        let authenticated_cmd = if let Some(ref token) = admin_token {
            format!("{} TOKEN {}\n", cmd_trimmed, token)
        } else {
            format!("{}:{}\n", signature, cmd_trimmed)
        };
        control_sender
            .send(Message::Text(authenticated_cmd))
            .await?;
        let _ = drain_response(&mut control_receiver).await;
    }

    // Grant write permissions
    println!(
        "Granting write permissions to user '{}' for {} event type(s)...",
        user_id,
        event_types.len()
    );
    let event_types_list = event_types.join(",");
    let grant_cmd = format!("GRANT WRITE ON {} TO {}\n", event_types_list, user_id);
    let cmd_trimmed = grant_cmd.trim();
    let signature = compute_hmac(&admin_key, cmd_trimmed);
    let authenticated_cmd = if let Some(ref token) = admin_token {
        format!("{} TOKEN {}\n", cmd_trimmed, token)
    } else {
        format!("{}:{}\n", signature, cmd_trimmed)
    };
    control_sender
        .send(Message::Text(authenticated_cmd))
        .await?;
    let _ = drain_response(&mut control_receiver).await;
    println!("Write permissions granted successfully");

    // Pre-generate contexts
    let contexts: Vec<String> = (0..context_pool).map(|i| format!("ctx-{}", i)).collect();

    // Throughput tracking
    let start = Instant::now();
    let sent = Arc::new(AtomicUsize::new(0));
    let errors = Arc::new(AtomicUsize::new(0));

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
    let reporter = tokio::spawn(async move {
        let mut last = 0usize;
        let mut last_t = Instant::now();
        let mut zero_progress_count = 0u32;
        loop {
            sleep(Duration::from_secs(1)).await;
            let now = Instant::now();
            let cur = sent_clone.load(Ordering::Relaxed);
            let errs = errors_clone.load(Ordering::Relaxed);
            let dt = now.duration_since(last_t).as_secs_f64();
            let d = cur - last;
            println!(
                "[PROG] total={} (+{}) errors={} {:.0} ev/s",
                cur,
                d,
                errs,
                (d as f64) / dt
            );
            last = cur;
            last_t = now;

            if cur >= total_events {
                break;
            }

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

    // Connection pool
    let connections = std::env::var("SNEL_STRESS_CONNECTIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(32);

    let mut tx_vec = Vec::new();
    let mut rxs = Vec::new();
    for _ in 0..connections {
        let (txi, rxi) = mpsc::channel::<String>(concurrency);
        tx_vec.push(txi);
        rxs.push(rxi);
    }

    // Spawn WebSocket workers with session token authentication
    let mut worker_handles = Vec::new();
    for mut rx_local in rxs {
        let ws_url_clone = ws_url.clone();
        let sent_inner = sent.clone();
        let errors_inner = errors.clone();
        let user_id_clone = user_id.clone();
        let secret_key_clone = secret_key.clone();
        let handle = tokio::spawn(async move {
            // Connect WebSocket
            let (ws, _) = match connect_async(&ws_url_clone).await {
                Ok(ws) => ws,
                Err(_) => return,
            };
            let (mut sender, mut receiver) = ws.split();

            // Authenticate and get session token
            tracing::info!(target: "stress_ws", user_id = %user_id_clone, "Worker authenticating");
            let auth_signature = compute_hmac(&secret_key_clone, &user_id_clone);
            let auth_cmd = format!("AUTH {}:{}\n", user_id_clone, auth_signature);

            if sender.send(Message::Text(auth_cmd)).await.is_err() {
                tracing::warn!(target: "stress_ws", "Failed to send AUTH command");
                return;
            }

            // Wait for OK TOKEN response
            let session_token = loop {
                match receiver.next().await {
                    Some(Ok(Message::Text(text))) => {
                        let trimmed = text.trim();
                        if trimmed.starts_with("OK TOKEN ") {
                            let token = trimmed
                                .strip_prefix("OK TOKEN ")
                                .unwrap()
                                .trim()
                                .to_string();
                            tracing::info!(target: "stress_ws", user_id = %user_id_clone, "Worker authenticated, token received");
                            break Some(token);
                        } else if trimmed == "OK" {
                            tracing::warn!(target: "stress_ws", user_id = %user_id_clone, "Worker authenticated but no token received");
                            break None; // No token, fall back to HMAC
                        } else if trimmed.starts_with("ERROR") {
                            tracing::error!(target: "stress_ws", user_id = %user_id_clone, error = %trimmed, "Authentication failed");
                            return; // Auth failed
                        }
                    }
                    Some(Ok(Message::Close(_))) => {
                        tracing::warn!(target: "stress_ws", "Connection closed during authentication");
                        return;
                    }
                    Some(Err(e)) => {
                        tracing::error!(target: "stress_ws", error = ?e, "WebSocket error during authentication");
                        return;
                    }
                    None => {
                        tracing::warn!(target: "stress_ws", "Stream ended during authentication");
                        return;
                    }
                    _ => continue,
                }
            };

            // Spawn background task to drain responses (fire-and-forget)
            let mut receiver_clone = receiver;
            let user_id_for_drain = user_id_clone.clone();
            tokio::spawn(async move {
                let mut error_count = 0u64;
                while let Some(msg) = receiver_clone.next().await {
                    match msg {
                        Ok(Message::Text(text)) => {
                            if text.trim().starts_with("ERROR") {
                                error_count += 1;
                                errors_inner.fetch_add(1, Ordering::Relaxed);
                                if error_count <= 5 {
                                    tracing::warn!(target: "stress_ws", user_id = %user_id_for_drain, error = %text.trim(), "Received error response");
                                }
                            }
                        }
                        Ok(Message::Close(_)) => {
                            tracing::info!(target: "stress_ws", user_id = %user_id_for_drain, "Response drainer: connection closed");
                            break;
                        }
                        Err(e) => {
                            tracing::warn!(target: "stress_ws", user_id = %user_id_for_drain, error = ?e, "Response drainer: error");
                            break;
                        }
                        _ => continue,
                    }
                }
                tracing::debug!(target: "stress_ws", user_id = %user_id_for_drain, errors_received = error_count, "Response drainer finished");
            });

            // Main writer loop - send commands with session token (fast path)
            let mut cmd_count = 0u64;
            tracing::info!(target: "stress_ws", user_id = %user_id_clone, "Worker starting command loop");
            while let Some(cmd) = rx_local.recv().await {
                cmd_count += 1;
                let cmd_trimmed = cmd.trim();
                let authenticated_cmd = if let Some(ref token) = session_token {
                    // Fast path: use session token (no HMAC per command)
                    format!("{} TOKEN {}\n", cmd_trimmed, token)
                } else {
                    // Fallback: use HMAC (slower)
                    let signature = compute_hmac(&secret_key_clone, cmd_trimmed);
                    format!("{}:{}\n", signature, cmd_trimmed)
                };

                match sender.send(Message::Text(authenticated_cmd)).await {
                    Ok(_) => {
                        sent_inner.fetch_add(1, Ordering::Relaxed);
                        if cmd_count % 1000 == 0 {
                            tracing::debug!(target: "stress_ws", user_id = %user_id_clone, commands_sent = cmd_count, "Worker progress");
                        }
                    }
                    Err(e) => {
                        tracing::warn!(target: "stress_ws", user_id = %user_id_clone, commands_sent = cmd_count, error = ?e, "Failed to send command, connection lost");
                        break; // Connection lost
                    }
                }
            }
            tracing::info!(target: "stress_ws", user_id = %user_id_clone, total_commands = cmd_count, "Worker finished");
        });
        worker_handles.push(handle);
    }

    // Produce jobs
    tracing::info!(target: "stress_ws", total_events = total_events, connections = connections, "Starting event generation");
    let mut rng = rand::thread_rng();
    for i in 0..total_events {
        let ctx_id = &contexts[i % context_pool];
        let event_type = &event_types[i % event_types.len()];
        let user_id_val = rng.gen_range(0..context_pool);
        let evt = random_event_payload(i as u64, ts_start, ts_end, user_id_val as u64, &link_field);
        let cmd = format!("STORE {} FOR {} PAYLOAD {}", event_type, ctx_id, evt);
        let txi = &tx_vec[i % tx_vec.len()];
        if txi.send(cmd).await.is_err() {
            tracing::warn!(target: "stress_ws", event_index = i, "Channel closed, stopping event generation");
            break;
        }
        if (i + 1) % 10000 == 0 {
            tracing::info!(target: "stress_ws", events_generated = i + 1, "Event generation progress");
        }
    }
    drop(tx_vec); // close channels
    tracing::info!(target: "stress_ws", "Event generation complete, waiting for workers");

    // Wait for workers
    for h in worker_handles {
        let _ = h.await;
    }

    let elapsed = start.elapsed().as_secs_f64();
    let total_sent = sent.load(Ordering::Relaxed);
    let total_errors = errors.load(Ordering::Relaxed);
    println!(
        "Ingested {} events in {:.2}s ({:.0} ev/s), {} errors",
        total_sent,
        elapsed,
        (total_sent as f64) / elapsed,
        total_errors
    );

    // Stop sampler and reporter
    stop_flag.notify_waiters();
    let _ = sampler.await;
    let _ = reporter.await;

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

async fn drain_response(
    receiver: &mut futures_util::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    >,
) {
    // WebSocket sends multi-line responses as a single message, so we only need to read one
    match tokio::time::timeout(Duration::from_secs(5), receiver.next()).await {
        Ok(Some(Ok(Message::Text(_text)))) => {
            // Response received - WebSocket sends everything in one message
            // We could log it here if needed, but for performance we skip it
        }
        Ok(Some(Ok(Message::Close(_)))) => {
            // Connection closed
        }
        Ok(Some(Err(_))) => {
            // Error occurred
        }
        Ok(None) => {
            // Stream ended
        }
        Ok(Some(Ok(_))) => {
            // Other message type, ignore
        }
        Err(_) => {
            // Timeout - no response received, but that's okay for fire-and-forget
        }
    }
}

/// Compute HMAC-SHA256 signature
fn compute_hmac(secret_key: &str, message: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret_key.as_bytes()).expect("HMAC can take key of any size");
    mac.update(message.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}
