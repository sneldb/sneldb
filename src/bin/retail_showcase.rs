use anyhow::{Context, Result};
use rand::Rng;
use serde_json::json;
use snel_db::shared::config::CONFIG;
use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};

// Retail product catalog (prices in cents)
const PRODUCTS: &[(&str, &str, u64, &str)] = &[
    ("p_001", "Running Shoes", 7999, "EUR"),        // 79.99 EUR
    ("p_002", "Wireless Headphones", 12999, "EUR"), // 129.99 EUR
    ("p_003", "Laptop Backpack", 4999, "EUR"),      // 49.99 EUR
    ("p_004", "Smart Watch", 24999, "EUR"),         // 249.99 EUR
    ("p_005", "Yoga Mat", 2999, "EUR"),             // 29.99 EUR
    ("p_006", "Water Bottle", 1999, "EUR"),         // 19.99 EUR
    ("p_007", "Fitness Tracker", 8999, "EUR"),      // 89.99 EUR
    ("p_008", "Gym Bag", 3999, "EUR"),              // 39.99 EUR
    ("p_009", "Protein Shaker", 1499, "EUR"),       // 14.99 EUR
    ("p_010", "Resistance Bands Set", 2499, "EUR"), // 24.99 EUR
    ("p_011", "Basketball", 3499, "EUR"),           // 34.99 EUR
    ("p_012", "Tennis Racket", 8999, "EUR"),        // 89.99 EUR
    ("p_013", "Cycling Helmet", 5999, "EUR"),       // 59.99 EUR
    ("p_014", "Hiking Boots", 11999, "EUR"),        // 119.99 EUR
    ("p_015", "Swimming Goggles", 1699, "EUR"),     // 16.99 EUR
    ("p_016", "Jump Rope", 1299, "EUR"),            // 12.99 EUR
    ("p_017", "Dumbbells Set", 7999, "EUR"),        // 79.99 EUR
    ("p_018", "Foam Roller", 2499, "EUR"),          // 24.99 EUR
    ("p_019", "Pilates Ring", 1999, "EUR"),         // 19.99 EUR
    ("p_020", "Exercise Ball", 2999, "EUR"),        // 29.99 EUR
];

const COURIERS: &[&str] = &["DHL", "UPS", "FedEx", "DPD", "PostNL"];
const REGIONS: &[&str] = &["NL", "DE", "BE", "FR", "UK", "ES", "IT"];
const SOURCES: &[&str] = &[
    "ad_campaign",
    "organic_search",
    "direct",
    "social_media",
    "email_campaign",
    "affiliate",
    "referral",
];

#[derive(Debug, Clone)]
struct CustomerSession {
    context_id: String,
    customer_id: String,
    product_id: String,
    product_name: String,
    price: u64, // Price in cents
    currency: String,
    source: String,
    region: String,
    courier: String,
    session_start: i64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = snel_db::logging::init();

    // Parameters
    let total_sessions: usize = std::env::var("RETAIL_SESSIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);
    let concurrency: usize = std::env::var("RETAIL_CONCURRENCY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(16);
    let connections: usize = std::env::var("RETAIL_CONNECTIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4);

    // Time range for generated events (default: last 7 days)
    let days_back: i64 = std::env::var("RETAIL_DAYS_BACK")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(7);
    let now_secs_i64: i64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let ts_start = now_secs_i64 - days_back * 86_400;
    let ts_end = now_secs_i64;

    let addr = &CONFIG.server.tcp_addr;
    println!("Retail Showcase - Connecting to {}...", addr);
    println!(
        "Generating {} customer sessions with realistic retail events",
        total_sessions
    );

    // Control connection for schema definitions
    let control = TcpStream::connect(addr)
        .await
        .with_context(|| format!("failed to connect to {}", addr))?;
    control.set_nodelay(true)?;
    let mut control_reader = BufReader::new(control);

    // Define schemas for each event type
    let event_types = vec![
        "page_view",
        "add_to_cart",
        "checkout_started",
        "payment_succeeded",
        "order_created",
        "shipment_dispatched",
        "order_delivered",
        "review_submitted",
    ];

    println!("Defining schemas for {} event types...", event_types.len());
    for event_type in &event_types {
        let schema_cmd = match *event_type {
            "page_view" => format!(
                "DEFINE {} FIELDS {{ context_id: \"string\", customer_id: \"string\", product_id: \"string\", product_name: \"string\", price: \"u64\", currency: \"string\", quantity: \"u64\", source: \"string\", created_at: \"datetime\" }}\n",
                event_type
            ),
            "add_to_cart" => format!(
                "DEFINE {} FIELDS {{ context_id: \"string\", customer_id: \"string\", product_id: \"string\", product_name: \"string\", price: \"u64\", currency: \"string\", quantity: \"u64\", created_at: \"datetime\" }}\n",
                event_type
            ),
            "checkout_started" => format!(
                "DEFINE {} FIELDS {{ context_id: \"string\", customer_id: \"string\", created_at: \"datetime\" }}\n",
                event_type
            ),
            "payment_succeeded" => format!(
                "DEFINE {} FIELDS {{ context_id: \"string\", customer_id: \"string\", price: \"u64\", currency: \"string\", created_at: \"datetime\" }}\n",
                event_type
            ),
            "order_created" => format!(
                "DEFINE {} FIELDS {{ context_id: \"string\", customer_id: \"string\", product_id: \"string\", status: \"string\", created_at: \"datetime\" }}\n",
                event_type
            ),
            "shipment_dispatched" => format!(
                "DEFINE {} FIELDS {{ context_id: \"string\", courier: \"string\", region: \"string\", created_at: \"datetime\" }}\n",
                event_type
            ),
            "order_delivered" => format!(
                "DEFINE {} FIELDS {{ context_id: \"string\", customer_id: \"string\", region: \"string\", created_at: \"datetime\" }}\n",
                event_type
            ),
            "review_submitted" => format!(
                "DEFINE {} FIELDS {{ context_id: \"string\", customer_id: \"string\", product_id: \"string\", rating: \"u64\", created_at: \"datetime\" }}\n",
                event_type
            ),
            _ => continue,
        };
        send_and_drain(&mut control_reader, &schema_cmd, None).await?;
    }

    // Throughput tracking
    let start = Instant::now();
    let sent = Arc::new(AtomicUsize::new(0));
    let expected_total = total_sessions * event_types.len();

    // Progress reporter
    let sent_clone = sent.clone();
    let reporter = tokio::spawn(async move {
        let mut last = 0usize;
        let mut last_t = Instant::now();
        loop {
            sleep(Duration::from_secs(1)).await;
            let now = Instant::now();
            let cur = sent_clone.load(Ordering::Relaxed);
            let dt = now.duration_since(last_t).as_secs_f64();
            let d = cur - last;
            if d > 0 {
                println!("[PROG] total={} (+{}) {:.0} ev/s", cur, d, (d as f64) / dt);
            }
            last = cur;
            last_t = now;

            if cur >= expected_total {
                break;
            }
        }
    });

    // Create worker channels
    let mut tx_vec = Vec::new();
    let mut rxs = Vec::new();
    for _ in 0..connections {
        let (txi, rxi) = mpsc::channel::<String>(concurrency);
        tx_vec.push(txi);
        rxs.push(rxi);
    }

    // Spawn workers
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

            let (reader_half, writer_half) = stream.into_split();
            let mut reader = BufReader::new(reader_half);

            // Background task to drain responses
            let response_drainer = tokio::spawn(async move {
                let mut line = String::new();
                loop {
                    line.clear();
                    match reader.read_line(&mut line).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => continue,
                    }
                }
            });

            let drainer_abort = response_drainer.abort_handle();
            let mut writer = writer_half;

            while let Some(cmd) = rx_local.recv().await {
                match writer.write_all(cmd.as_bytes()).await {
                    Ok(_) => {
                        sent_inner.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) if e.kind() == ErrorKind::BrokenPipe => {
                        if let Ok(fresh) = TcpStream::connect(addr_clone.clone()).await {
                            let _ = fresh.set_nodelay(true);
                            let (_, new_writer) = fresh.into_split();
                            writer = new_writer;
                            let _ = writer.write_all(cmd.as_bytes()).await;
                            sent_inner.fetch_add(1, Ordering::Relaxed);
                        }
                        break;
                    }
                    Err(_) => break,
                }
            }

            drainer_abort.abort();
            let _ = response_drainer.await;
        });
        worker_handles.push(handle);
    }

    // Generate retail events for each session
    let mut rng = rand::thread_rng();
    for session_idx in 0..total_sessions {
        let session = generate_session(session_idx, ts_start, ts_end, &mut rng);

        // Generate events in sequence with realistic timing
        let events = generate_session_events(&session, &mut rng);

        for (event_type, payload) in events {
            let cmd = format!(
                "STORE {} FOR {} PAYLOAD {}\n",
                event_type, session.context_id, payload
            );
            let txi = &tx_vec[session_idx % tx_vec.len()];
            let _ = txi.send(cmd).await;
        }
    }
    drop(tx_vec);

    // Wait for workers to finish
    for h in worker_handles {
        let _ = h.await;
    }

    let elapsed = start.elapsed().as_secs_f64();
    let total_events = total_sessions * event_types.len();
    println!(
        "\n✓ Ingested {} events ({} sessions) in {:.2}s ({:.0} ev/s)",
        total_events,
        total_sessions,
        elapsed,
        (total_events as f64) / elapsed
    );

    let _ = reporter.await;

    println!("\n✓ Retail showcase complete!");
    Ok(())
}

fn generate_session(
    session_idx: usize,
    ts_start: i64,
    ts_end: i64,
    rng: &mut impl Rng,
) -> CustomerSession {
    let product = PRODUCTS[rng.gen_range(0..PRODUCTS.len())];
    let region = REGIONS[rng.gen_range(0..REGIONS.len())];
    let source = SOURCES[rng.gen_range(0..SOURCES.len())];
    let courier = COURIERS[rng.gen_range(0..COURIERS.len())];

    // Session start time (random within range)
    let session_start = rng.gen_range(ts_start..=ts_end);

    CustomerSession {
        context_id: format!("sess_{}", session_idx + 1000),
        customer_id: format!("cust_{}", rng.gen_range(500..600)),
        product_id: product.0.to_string(),
        product_name: product.1.to_string(),
        price: product.2,
        currency: product.3.to_string(),
        source: source.to_string(),
        region: region.to_string(),
        courier: courier.to_string(),
        session_start,
    }
}

fn generate_session_events(
    session: &CustomerSession,
    rng: &mut impl Rng,
) -> Vec<(&'static str, String)> {
    let mut events = Vec::new();
    let mut current_time = session.session_start;

    // 1. Page view (session start)
    events.push((
        "page_view",
        json!({
            "context_id": session.context_id,
            "customer_id": session.customer_id,
            "product_id": session.product_id,
            "product_name": session.product_name,
            "price": session.price,
            "currency": session.currency,
            "quantity": 1,
            "source": session.source,
            "created_at": current_time
        })
        .to_string(),
    ));

    // 2. Add to cart (2-5 minutes later, 80% probability)
    if rng.gen_bool(0.8) {
        current_time += rng.gen_range(120..300); // 2-5 minutes
        events.push((
            "add_to_cart",
            json!({
                "context_id": session.context_id,
                "customer_id": session.customer_id,
                "product_id": session.product_id,
                "product_name": session.product_name,
                "price": session.price,
                "currency": session.currency,
                "quantity": 1,
                "created_at": current_time
            })
            .to_string(),
        ));

        // 3. Checkout started (1-3 minutes later, 70% of add_to_cart)
        if rng.gen_bool(0.7) {
            current_time += rng.gen_range(60..180); // 1-3 minutes
            events.push((
                "checkout_started",
                json!({
                    "context_id": session.context_id,
                    "customer_id": session.customer_id,
                    "created_at": current_time
                })
                .to_string(),
            ));

            // 4. Payment succeeded (10-30 seconds later, 90% of checkout)
            if rng.gen_bool(0.9) {
                current_time += rng.gen_range(10..30); // 10-30 seconds
                events.push((
                    "payment_succeeded",
                    json!({
                        "context_id": session.context_id,
                        "customer_id": session.customer_id,
                        "price": session.price,
                        "currency": session.currency,
                        "created_at": current_time
                    })
                    .to_string(),
                ));

                // 5. Order created (30-60 seconds later, 100% of payment)
                current_time += rng.gen_range(30..60); // 30-60 seconds
                events.push((
                    "order_created",
                    json!({
                        "context_id": session.context_id,
                        "customer_id": session.customer_id,
                        "product_id": session.product_id,
                        "status": "confirmed",
                        "created_at": current_time
                    })
                    .to_string(),
                ));

                // 6. Shipment dispatched (2-6 hours later, 95% of orders)
                if rng.gen_bool(0.95) {
                    current_time += rng.gen_range(7200..21600); // 2-6 hours
                    events.push((
                        "shipment_dispatched",
                        json!({
                            "context_id": session.context_id,
                            "courier": session.courier,
                            "region": session.region,
                            "created_at": current_time
                        })
                        .to_string(),
                    ));

                    // 7. Order delivered (1-3 days later, 98% of shipments)
                    if rng.gen_bool(0.98) {
                        current_time += rng.gen_range(86400..259200); // 1-3 days
                        events.push((
                            "order_delivered",
                            json!({
                                "context_id": session.context_id,
                                "customer_id": session.customer_id,
                                "region": session.region,
                                "created_at": current_time
                            })
                            .to_string(),
                        ));

                        // 8. Review submitted (0-7 days after delivery, 30% probability)
                        if rng.gen_bool(0.3) {
                            current_time += rng.gen_range(0..604800); // 0-7 days
                            events.push((
                                "review_submitted",
                                json!({
                                    "context_id": session.context_id,
                                    "customer_id": session.customer_id,
                                    "product_id": session.product_id,
                                    "rating": rng.gen_range(3..6), // 3-5 stars
                                    "created_at": current_time
                                })
                                .to_string(),
                            ));
                        }
                    }
                }
            }
        }
    }

    events
}

async fn send_and_drain(
    reader: &mut BufReader<TcpStream>,
    cmd: &str,
    wait: Option<Duration>,
) -> Result<()> {
    write_all_with_timeout(reader, cmd.as_bytes(), wait).await?;
    let mut header = String::new();
    read_line_with_timeout(reader, &mut header, wait).await?;
    Ok(())
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
