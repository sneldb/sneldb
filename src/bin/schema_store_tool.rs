use std::env;
use std::path::PathBuf;

use serde::Serialize;

use snel_db::engine::schema::store::SchemaStore;

#[derive(Serialize)]
struct Report {
    version: Option<u16>,
    valid_records: usize,
    skipped_records: usize,
    issues: Vec<String>,
    repaired_to: Option<String>,
}

fn usage_and_exit() -> ! {
    eprintln!("Usage:");
    eprintln!("  schema_store_tool verify <path/to/schemas.bin>");
    eprintln!("  schema_store_tool repair <path/to/schemas.bin> <path/to/output.bin>");
    std::process::exit(1);
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        usage_and_exit();
    }

    match args[1].as_str() {
        "verify" => {
            let path = PathBuf::from(&args[2]);
            let store = SchemaStore::new(path).unwrap_or_else(|e| {
                eprintln!("Failed to open schema store: {}", e);
                std::process::exit(1);
            });

            let diagnostics = store.diagnose().unwrap_or_else(|e| {
                eprintln!("Failed to diagnose schema store: {}", e);
                std::process::exit(1);
            });

            let report = Report {
                version: diagnostics.version,
                valid_records: diagnostics.valid_records,
                skipped_records: diagnostics.skipped_records,
                issues: diagnostics.issues,
                repaired_to: None,
            };

            print_report(&report);
        }
        "repair" => {
            if args.len() != 4 {
                usage_and_exit();
            }
            let input = PathBuf::from(&args[2]);
            let output = PathBuf::from(&args[3]);

            let store = SchemaStore::new(input).unwrap_or_else(|e| {
                eprintln!("Failed to open schema store: {}", e);
                std::process::exit(1);
            });

            let diagnostics = store.repair_to(output.clone()).unwrap_or_else(|e| {
                eprintln!("Failed to repair schema store: {}", e);
                std::process::exit(1);
            });

            let report = Report {
                version: diagnostics.version,
                valid_records: diagnostics.valid_records,
                skipped_records: diagnostics.skipped_records,
                issues: diagnostics.issues,
                repaired_to: Some(output.display().to_string()),
            };

            print_report(&report);
        }
        _ => usage_and_exit(),
    }
}

fn print_report(report: &Report) {
    match serde_json::to_string_pretty(report) {
        Ok(json) => println!("{}", json),
        Err(e) => {
            eprintln!("Failed to serialize report: {}", e);
            std::process::exit(1);
        }
    }
}
