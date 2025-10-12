use snel_db::engine::core::wal::wal_archiver::WalArchiver;
use snel_db::engine::core::WalEntry;
use serde_json::json;
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::time::Instant;
use tempfile::TempDir;

fn create_test_wal_file(dir: &PathBuf, log_id: u64, size_mb: usize) -> std::io::Result<()> {
    let wal_path = dir.join(format!("wal-{:05}.log", log_id));
    let mut file = File::create(wal_path)?;

    // Create entries to fill approximately size_mb megabytes
    let entries_per_mb = 1000; // Approximate
    let total_entries = size_mb * entries_per_mb;

    for i in 0..total_entries {
        let entry = WalEntry {
            timestamp: 1700000000 + i as u64,
            context_id: format!("user-{}", i % 100),
            event_type: "benchmark_event".to_string(),
            payload: json!({
                "index": i,
                "data": "x".repeat(100), // Some payload data
                "nested": {
                    "field1": "value1",
                    "field2": i % 10,
                }
            }),
        };
        writeln!(file, "{}", serde_json::to_string(&entry).unwrap())?;
    }

    Ok(())
}

fn benchmark_archiving() {
    println!("WAL Archiving Performance Benchmark");
    println!("====================================\n");

    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    let archive_dir = temp_dir.path().join("archive");
    fs::create_dir_all(&wal_dir).unwrap();

    // Configuration
    let num_files = 5;
    let file_size_mb = 2;

    println!("Setup:");
    println!("  Files: {}", num_files);
    println!("  Size per file: ~{}MB", file_size_mb);
    println!("  Total: ~{}MB\n", num_files * file_size_mb);

    // Create test WAL files
    print!("Creating test WAL files... ");
    let setup_start = Instant::now();
    for i in 0..num_files {
        create_test_wal_file(&wal_dir, i as u64, file_size_mb).unwrap();
    }
    println!("done ({:.2}s)", setup_start.elapsed().as_secs_f64());

    // Calculate total size
    let total_size: u64 = fs::read_dir(&wal_dir)
        .unwrap()
        .flatten()
        .map(|e| e.metadata().unwrap().len())
        .sum();
    println!("  Actual total size: {:.2}MB\n", total_size as f64 / 1_048_576.0);

    // Benchmark archiving
    println!("Benchmarking parallel archiving...");
    let archiver = WalArchiver::with_dirs(1, wal_dir.clone(), archive_dir.clone(), 3);

    let start = Instant::now();
    let results = archiver.archive_logs_up_to(num_files as u64);
    let duration = start.elapsed();

    let success_count = results.iter().filter(|r| r.is_ok()).count();

    println!("\nResults:");
    println!("  Duration: {:.3}s", duration.as_secs_f64());
    println!("  Files archived: {}/{}", success_count, num_files);
    println!(
        "  Throughput: {:.2} MB/s",
        total_size as f64 / 1_048_576.0 / duration.as_secs_f64()
    );

    // Calculate compression ratio
    let archive_size: u64 = fs::read_dir(&archive_dir)
        .unwrap()
        .flatten()
        .map(|e| e.metadata().unwrap().len())
        .sum();

    println!("\nCompression:");
    println!("  Original: {:.2}MB", total_size as f64 / 1_048_576.0);
    println!("  Compressed: {:.2}MB", archive_size as f64 / 1_048_576.0);
    println!(
        "  Ratio: {:.2}% ({}× reduction)",
        (archive_size as f64 / total_size as f64) * 100.0,
        total_size as f64 / archive_size as f64
    );

    println!("\nPer-file metrics:");
    println!(
        "  Avg time per file: {:.3}s",
        duration.as_secs_f64() / num_files as f64
    );
    println!(
        "  Avg original size: {:.2}MB",
        total_size as f64 / 1_048_576.0 / num_files as f64
    );
    println!(
        "  Avg compressed size: {:.2}MB",
        archive_size as f64 / 1_048_576.0 / num_files as f64
    );

    println!("\n✅ Benchmark complete!");
}

fn main() {
    benchmark_archiving();
}

