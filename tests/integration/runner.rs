use crate::integration::config::write_config_for_with_overrides;
use crate::integration::scenarios::TestScenario;
use std::io::Write;
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

    let (config_path, socket_path) =
        write_config_for_with_overrides(&scenario.name, scenario.config.as_ref());
    debug!("Using config path: {}", config_path);
    debug!("Using socket path: {}", socket_path);

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

    // Helper to (re)connect socat
    let mut spawn_socat = || -> (std::process::Child, std::process::ChildStdin) {
        let mut sp = Command::new("socat")
            .arg("-")
            .arg(format!("UNIX-CONNECT:{}", socket_path))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("Failed to start socat");
        let stdin = sp.stdin.take().expect("Failed to get stdin");
        (sp, stdin)
    };

    // Send commands one by one to handle FLUSH delays
    let (mut socat_process, mut stdin) = spawn_socat();

    // Accumulate outputs across reconnects (e.g., after RESTART)
    let mut accumulated_output = String::new();

    for (i, command) in scenario.input_commands.iter().enumerate() {
        // Handle SLEEP pseudo-command locally without sending to server
        if let Some(ms) = parse_sleep_ms(command) {
            debug!("SLEEP {} ms (step {})", ms, i + 1);
            std::thread::sleep(std::time::Duration::from_millis(ms));
            continue;
        }

        // Handle RESTART pseudo-command: restart server and reconnect socat
        if command.trim().eq_ignore_ascii_case("RESTART") {
            debug!(
                "RESTART command received (step {}), restarting server",
                i + 1
            );

            // Close current socat input and collect its output
            drop(stdin);
            let out = socat_process
                .wait_with_output()
                .expect("Failed to get socat output during restart");
            accumulated_output.push_str(&String::from_utf8_lossy(&out.stdout));

            // Restart server
            let _ = server.kill();
            let _ = server.wait();
            let _ = std::fs::remove_file(&socket_path);

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

            // Reconnect socat
            let pair = spawn_socat();
            socat_process = pair.0;
            stdin = pair.1;
            continue;
        }

        debug!("Sending command {}: {}", i + 1, command);
        writeln!(stdin, "{}", command).expect("Failed to write command");
        stdin.flush().expect("Failed to flush stdin");

        // If this is a FLUSH command, wait briefly before sending the next command
        if command.trim().eq_ignore_ascii_case("FLUSH") {
            debug!("FLUSH command sent, waiting 100 miliseconds before next command");
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }

    // Close stdin to signal end of input
    drop(stdin);

    // Wait for socat to finish and get output (append to accumulated)
    let output_result = socat_process
        .wait_with_output()
        .expect("Failed to get socat output");
    accumulated_output.push_str(&String::from_utf8_lossy(&output_result.stdout));
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

    // Clean up socket file if it still exists
    let _ = std::fs::remove_file(socket_path);

    // Clean up tmp directory for this scenario
    let tmp_path = format!("tests/integration/tmp/{}", scenario.name);
    let _ = std::fs::remove_dir_all(tmp_path);
}

use std::str;

fn kill_process_using_socket(socket_path: &str) {
    // Find the PID using lsof
    let output = Command::new("lsof")
        .arg("-t")
        .arg(socket_path)
        .output()
        .expect("Failed to execute lsof");

    if output.status.success() {
        let pid_str = str::from_utf8(&output.stdout).unwrap().trim();
        if !pid_str.is_empty() {
            println!("Killing process with PID: {}", pid_str);
            let _ = Command::new("kill").arg("-9").arg(pid_str).status();
        } else {
            println!("No process is using the socket: {}", socket_path);
        }
    } else {
        println!("lsof failed or no process is using the socket.");
    }
}
