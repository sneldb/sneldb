use crate::integration::config::write_config_for_with_overrides;
use crate::integration::scenarios::TestScenario;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::process::{Command, Stdio};
use tracing::{debug, error, info};

fn parse_sleep_ms(cmd: &str) -> Option<u64> {
    let trimmed = cmd.trim();
    let mut parts = trimmed.split_whitespace();
    if let Some(first) = parts.next() {
        if first.eq_ignore_ascii_case("SLEEP") {
            if let Some(ms_str) = parts.next() {
                if let Ok(ms) = ms_str.parse::<u64>() {
                    return Some(ms);
                }
            }
        }
    }
    None
}

pub fn run_scenario(scenario: &TestScenario) {
    info!("▶ Running scenario: {}", scenario.name);
    let tmp_path = format!("tests/integration/tmp/");
    let _ = std::fs::remove_dir_all(tmp_path);

    let (config_path, socket_path, tcp_addr) =
        write_config_for_with_overrides(&scenario.name, scenario.config.as_ref());
    debug!("Using config path: {}", config_path);
    debug!("Using socket path: {}", socket_path);
    debug!("Using tcp addr: {}", tcp_addr);

    let mut server = Command::new("cargo")
        .args(&["run", "--bin", "snel_db"])
        .env("SNELDB_CONFIG", &config_path)
        .env("SNELDB_PRESERVE_DATA", "1")
        .env("RUST_LOG", "error")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("Failed to start server");

    std::thread::sleep(std::time::Duration::from_secs(2));

    // Helper to (re)connect TCP
    let connect_tcp = || -> TcpStream {
        for _ in 0..20 {
            match TcpStream::connect(&tcp_addr) {
                Ok(s) => return s,
                Err(e) => {
                    debug!("TCP connect failed: {} (retrying)", e);
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
            }
        }
        panic!("Failed to connect to TCP at {}", tcp_addr);
    };

    // Send commands one by one to handle FLUSH delays
    let mut stream = connect_tcp();

    // Accumulate outputs across reconnects (e.g., after RESTART)
    let mut accumulated_output = String::new();

    for (i, command) in scenario.input_commands.iter().enumerate() {
        // Handle SLEEP pseudo-command locally without sending to server
        if let Some(ms) = parse_sleep_ms(command) {
            debug!("SLEEP {} ms (step {})", ms, i + 1);
            std::thread::sleep(std::time::Duration::from_millis(ms));
            continue;
        }

        // Handle RESTART pseudo-command: restart server and reconnect TCP
        if command.trim().eq_ignore_ascii_case("RESTART") {
            debug!(
                "RESTART command received (step {}), restarting server",
                i + 1
            );

            // Close write half and collect output from current TCP connection
            let _ = stream.shutdown(Shutdown::Write);
            let mut buf = Vec::new();
            let _ = stream.read_to_end(&mut buf);
            accumulated_output.push_str(&String::from_utf8_lossy(&buf));

            // Restart server
            let _ = server.kill();
            let _ = server.wait();

            server = Command::new("cargo")
                .args(&["run", "--bin", "snel_db"])
                .env("SNELDB_CONFIG", &config_path)
                .env("SNELDB_PRESERVE_DATA", "1")
                .env("RUST_LOG", "error")
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .spawn()
                .expect("Failed to restart server");

            std::thread::sleep(std::time::Duration::from_secs(2));

            // Reconnect TCP
            stream = connect_tcp();
            continue;
        }

        debug!("Sending command {}: {}", i + 1, command);
        writeln!(stream, "{}", command).expect("Failed to write command");
        stream.flush().expect("Failed to flush stream");

        // If this is a FLUSH command, wait briefly before sending the next command
        if command.trim().eq_ignore_ascii_case("FLUSH") {
            debug!("FLUSH command sent, waiting 100 miliseconds before next command");
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }

    // Close write half to signal end of input and read remaining output
    let _ = stream.shutdown(Shutdown::Write);
    let mut buf = Vec::new();
    let _ = stream.read_to_end(&mut buf);
    accumulated_output.push_str(&String::from_utf8_lossy(&buf));
    let actual = accumulated_output;
    debug!("Actual output:\n{}", actual);

    // Prefer matchers (array) if present, else fallback to matcher (single)
    if let Some(matchers) = &scenario.matchers {
        debug!("Using matchers: {:?}", matchers);
        let all_pass = matchers.iter().all(|m| m.matches(&actual));
        if all_pass {
            info!("✅ {} passed", scenario.name);
        } else {
            let _ = server.kill();
            let _ = server.wait();
            error!("❌ {} failed", scenario.name);
            error!(
                "Expected all matchers to pass. Matchers: {:?}\nActual:\n{}",
                matchers, actual
            );
            panic!("Scenario '{}' failed", scenario.name);
        }
    } else if let Some(matcher) = &scenario.matcher {
        debug!("Using matcher: {:?}", matcher);
        if matcher.matches(&actual) {
            info!("✅ {} passed", scenario.name);
        } else {
            let _ = server.kill();
            let _ = server.wait();
            error!("❌ {} failed", scenario.name);
            error!("Expected {:?}\nActual:\n{}", matcher, actual);
            panic!("Scenario '{}' failed", scenario.name);
        }
    }

    let _ = server.kill();
    let _ = server.wait();

    // Clean up socket file if it still exists (noop for TCP-only runs)
    let _ = std::fs::remove_file(socket_path);

    // Clean up tmp directory for this scenario
    let tmp_path = format!("tests/integration/tmp/{}", scenario.name);
    let _ = std::fs::remove_dir_all(tmp_path);
}
