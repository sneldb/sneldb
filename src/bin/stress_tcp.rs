use anyhow::{Context, Result};
use rand::Rng;
use rand::distributions::{Alphanumeric, DistString};
use serde_json::{Value, json};
use snel_db::shared::config::CONFIG;
use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use sysinfo::System;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::sleep;

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
    let event_type =
        std::env::var("SNEL_STRESS_EVENT_TYPE").unwrap_or_else(|_| "stress_evt".to_string());
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
    let mut control_reader = BufReader::new(control);

    // Define schema (add enum field `plan` with 20 variants and datetime `created_at`)
    let schema_cmd = format!(
        "DEFINE {} FIELDS {{ id: \"u64\", v: \"string\", flag: \"bool\", created_at: \"datetime\", plan: [\"type01\", \"type02\", \"type03\", \"type04\", \"type05\", \"type06\", \"type07\", \"type08\", \"type09\", \"type10\", \"type11\", \"type12\", \"type13\", \"type14\", \"type15\", \"type16\", \"type17\", \"type18\", \"type19\", \"type20\"] }}\n",
        event_type
    );
    send_and_drain(&mut control_reader, &schema_cmd, wait_dur).await?;

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
        loop {
            sleep(Duration::from_secs(1)).await;
            let now = Instant::now();
            let cur = sent_clone.load(std::sync::atomic::Ordering::Relaxed);
            let dt = now.duration_since(last_t).as_secs_f64();
            let d = cur - last;
            println!("[PROG] total={} (+{}) {:.0} ev/s", cur, d, (d as f64) / dt);
            last = cur;
            last_t = now;
            if cur >= total_events {
                break;
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
        let handle = tokio::spawn(async move {
            let stream = match TcpStream::connect(addr_clone.clone()).await {
                Ok(s) => s,
                Err(_) => return,
            };
            let _ = stream.set_nodelay(true);

            // Split stream into reader and writer
            let (reader_half, writer_half) = stream.into_split();
            let mut reader = BufReader::new(reader_half);

            // Spawn background task to drain responses (abortable)
            let response_drainer = tokio::spawn(async move {
                let mut line = String::new();
                loop {
                    line.clear();
                    match reader.read_line(&mut line).await {
                        Ok(0) | Err(_) => break, // EOF or error
                        Ok(_) => continue,       // Keep draining
                    }
                }
            });

            // Get abort handle to cancel drainer when done
            let drainer_abort = response_drainer.abort_handle();

            // Main writer loop - pipeline requests without waiting
            let mut writer = writer_half;
            while let Some(cmd) = rx_local.recv().await {
                match writer.write_all(cmd.as_bytes()).await {
                    Ok(_) => {
                        sent_inner.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) if e.kind() == ErrorKind::BrokenPipe => {
                        // Try to reconnect once
                        if let Ok(fresh) = TcpStream::connect(addr_clone.clone()).await {
                            let _ = fresh.set_nodelay(true);
                            let (_, new_writer) = fresh.into_split();
                            writer = new_writer;
                            let _ = writer.write_all(cmd.as_bytes()).await;
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

    // Produce jobs
    for i in 0..total_events {
        let ctx_id = &contexts[i % context_pool];
        let evt = random_event_payload(i as u64, ts_start, ts_end);
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

    // Run REPLAY to sample latency
    let replay_cmd = format!("REPLAY {} FOR {}\n", event_type, sample_ctx);
    let t0 = Instant::now();
    send_and_collect_json_with_timeout(&mut control_reader, &replay_cmd, 10, wait_dur).await?;
    println!(
        "Replay latency: {:.2} ms",
        t0.elapsed().as_secs_f64() * 1000.0
    );

    // Run QUERY (scoped) to sample latency over time using `created_at`
    let since_secs = now_secs_i64 - 86_400; // last 24h
    let query_cmd = format!(
        "QUERY {} SINCE {} USING created_at WHERE id < 100\n",
        event_type, since_secs
    );
    let t1 = Instant::now();
    send_and_collect_json_with_timeout(&mut control_reader, &query_cmd, 10, wait_dur).await?;
    println!(
        "Query latency: {:.2} ms",
        t1.elapsed().as_secs_f64() * 1000.0
    );

    Ok(())
}

fn random_event_payload(seq: u64, ts_start: i64, ts_end: i64) -> String {
    let v = Alphanumeric.sample_string(&mut rand::thread_rng(), 12);
    let plan = format!("type{:02}", (seq % 20) + 1);
    let mut rng = rand::thread_rng();
    let low = ts_start.min(ts_end);
    let high = ts_start.max(ts_end);
    let ts = rng.gen_range(low..=high);
    let obj = json!({
        "id": seq,
        "v": v,
        "flag": (seq % 2 == 0),
        "created_at": ts,
        "plan": plan
    });
    obj.to_string()
}

async fn send_and_drain(
    reader: &mut BufReader<TcpStream>,
    cmd: &str,
    wait: Option<Duration>,
) -> Result<()> {
    write_all_with_timeout(reader, cmd.as_bytes(), wait).await?;
    let mut header = String::new();
    read_line_with_timeout(reader, &mut header, wait).await?;
    // Drain any following body lines until next prompt absence; here we assume single-line body for ok-lines
    Ok(())
}

async fn send_and_collect_json_with_timeout(
    reader: &mut BufReader<TcpStream>,
    cmd: &str,
    max_lines: usize,
    wait: Option<Duration>,
) -> Result<Vec<Value>> {
    write_all_with_timeout(reader, cmd.as_bytes(), wait).await?;
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

async fn write_all_with_timeout(
    reader: &mut BufReader<TcpStream>,
    buf: &[u8],
    wait: Option<Duration>,
) -> std::io::Result<()> {
    if let Some(dur) = wait {
        match tokio::time::timeout(dur, reader.get_mut().write_all(buf)).await {
            Ok(res) => res,
            Err(_) => Err(std::io::Error::new(ErrorKind::TimedOut, "write timeout")),
        }
    } else {
        reader.get_mut().write_all(buf).await
    }
}

async fn read_line_with_timeout(
    reader: &mut BufReader<TcpStream>,
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
