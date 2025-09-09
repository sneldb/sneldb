use crate::integration::config::write_config_for;
use crate::integration::scenarios::TestScenario;
use std::io::Write;
use std::process::{Command, Stdio};
use tracing::{debug, error, info};

pub fn run_scenario(scenario: &TestScenario) {
    info!("▶ Running scenario: {}", scenario.name);
    let tmp_path = format!("tests/integration/tmp/");
    let _ = std::fs::remove_dir_all(tmp_path);

    let (config_path, socket_path) = write_config_for(&scenario.name);
    debug!("Using config path: {}", config_path);
    debug!("Using socket path: {}", socket_path);

    let mut server = Command::new("cargo")
        .args(&["run", "--bin", "snel_db"])
        .env("SNELDB_CONFIG", &config_path)
        .env("RUST_LOG", "error")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("Failed to start server");

    std::thread::sleep(std::time::Duration::from_secs(2));

    // Send commands one by one to handle FLUSH delays
    let mut socat_process = Command::new("socat")
        .arg("-")
        .arg(format!("UNIX-CONNECT:{}", socket_path))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to start socat");

    let stdin = socat_process.stdin.as_mut().expect("Failed to get stdin");

    for (i, command) in scenario.input_commands.iter().enumerate() {
        debug!("Sending command {}: {}", i + 1, command);
        writeln!(stdin, "{}", command).expect("Failed to write command");
        stdin.flush().expect("Failed to flush stdin");

        // If this is a FLUSH command, wait 2 second before sending the next command
        if command.trim() == "FLUSH" {
            debug!("FLUSH command sent, waiting 100 miliseconds before next command");
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }

    // Close stdin to signal end of input
    drop(stdin);

    // Wait for socat to finish and get output
    let output_result = socat_process
        .wait_with_output()
        .expect("Failed to get socat output");
    let actual = String::from_utf8_lossy(&output_result.stdout).to_string();
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
