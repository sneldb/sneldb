use snel_db::engine::core::wal::wal_archive::WalArchive;
use snel_db::engine::core::wal::wal_archive_recovery::{ArchiveInfo, WalArchiveRecovery};
use snel_db::engine::core::wal::wal_archiver::WalArchiver;
use snel_db::shared::config::CONFIG;
use std::env;
use std::path::PathBuf;

fn print_usage() {
    println!("WAL Archive Manager");
    println!();
    println!("Usage:");
    println!("  wal_archive_manager <command> [args]");
    println!();
    println!("Commands:");
    println!("  list <shard_id>                    - List all archives for a shard");
    println!("  info <archive_path>                - Show detailed info about an archive");
    println!("  export <archive_path> <output.json> - Export archive to JSON");
    println!("  recover <shard_id>                 - Recover all entries from archives");
    println!("  archive <shard_id> <log_id>        - Manually archive a WAL log file");
    println!();
    println!("Examples:");
    println!("  wal_archive_manager list 0");
    println!(
        "  wal_archive_manager info ../data/wal/archived/shard-0/wal-00001-1700000000-1700003600.wal.zst"
    );
    println!("  wal_archive_manager export archive.wal.zst output.json");
    println!("  wal_archive_manager recover 0");
    println!("  wal_archive_manager archive 0 5");
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} bytes", bytes)
    }
}

#[allow(dead_code)]
fn format_archive_info(info: &ArchiveInfo) -> String {
    let file_size = std::fs::metadata(&info.path)
        .map(|m| format_bytes(m.len()))
        .unwrap_or_else(|_| "unknown".to_string());

    format!(
        "Archive: {}\n\
         Shard:       {}\n\
         Log ID:      {:05}\n\
         Entries:     {}\n\
         Time Range:  {} to {}\n\
         Created:     {}\n\
         Compression: {} (level {})\n\
         File Size:   {}\n\
         Version:     {}",
        info.path.file_name().unwrap_or_default().to_string_lossy(),
        info.shard_id,
        info.log_id,
        info.entry_count,
        snel_db::shared::time::format_timestamp(info.start_timestamp),
        snel_db::shared::time::format_timestamp(info.end_timestamp),
        snel_db::shared::time::format_timestamp(info.created_at),
        info.compression,
        info.compression_level,
        file_size,
        info.version
    )
}

fn cmd_list(shard_id: usize) -> Result<(), Box<dyn std::error::Error>> {
    let archive_dir = PathBuf::from(&CONFIG.wal.archive_dir).join(format!("shard-{}", shard_id));

    let recovery = WalArchiveRecovery::new(shard_id, archive_dir);
    let infos = recovery.list_archive_info();

    if infos.is_empty() {
        println!("No archives found for shard {}", shard_id);
        return Ok(());
    }

    println!("Found {} archive(s) for shard {}:", infos.len(), shard_id);
    println!();

    let mut total_entries = 0u64;
    let mut total_size = 0u64;

    for info in &infos {
        let file_size = std::fs::metadata(&info.path)?.len();
        total_entries += info.entry_count;
        total_size += file_size;

        println!(
            "  {} | Log {:05} | {} entries | {}",
            info.path.file_name().unwrap_or_default().to_string_lossy(),
            info.log_id,
            info.entry_count,
            format_bytes(file_size)
        );
    }

    println!();
    println!(
        "Total: {} entries across {} archives ({})",
        total_entries,
        infos.len(),
        format_bytes(total_size)
    );

    Ok(())
}

fn cmd_info(archive_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let path = PathBuf::from(archive_path);

    if !path.exists() {
        return Err(format!("Archive not found: {}", archive_path).into());
    }

    println!("Reading archive...");
    let archive = WalArchive::read_from_file(&path)?;

    println!();
    println!("=== Archive Information ===");
    println!();
    println!("Version:          {}", archive.header.version);
    println!("Shard ID:         {}", archive.header.shard_id);
    println!("Log ID:           {:05}", archive.header.log_id);
    println!("Entry Count:      {}", archive.header.entry_count);
    println!(
        "Start Timestamp:  {} ({})",
        archive.header.start_timestamp,
        snel_db::shared::time::format_timestamp(archive.header.start_timestamp)
    );
    println!(
        "End Timestamp:    {} ({})",
        archive.header.end_timestamp,
        snel_db::shared::time::format_timestamp(archive.header.end_timestamp)
    );
    println!(
        "Created At:       {} ({})",
        archive.header.created_at,
        snel_db::shared::time::format_timestamp(archive.header.created_at)
    );
    println!("Compression:      {}", archive.header.compression);
    println!("Compression Level: {}", archive.header.compression_level);
    println!();

    let file_size = std::fs::metadata(&path)?.len();
    println!("File Size:        {}", format_bytes(file_size));

    if archive.header.entry_count > 0 {
        let avg_size = file_size / archive.header.entry_count;
        println!("Avg per entry:    {}", format_bytes(avg_size));
    }

    // Show sample entries
    if !archive.body.entries.is_empty() {
        println!();
        println!("=== Sample Entries ===");
        let sample_count = archive.body.entries.len().min(5);
        for (i, entry) in archive.body.entries.iter().take(sample_count).enumerate() {
            println!();
            println!("Entry {}:", i + 1);
            println!("  Event Type:  {}", entry.event_type);
            println!("  Context ID:  {}", entry.context_id);
            println!(
                "  Timestamp:   {} ({})",
                entry.timestamp,
                snel_db::shared::time::format_timestamp(entry.timestamp)
            );
            println!("  Payload:     {}", serde_json::to_string(&entry.payload)?);
        }

        if archive.body.entries.len() > sample_count {
            println!();
            println!(
                "... and {} more entries",
                archive.body.entries.len() - sample_count
            );
        }
    }

    Ok(())
}

fn cmd_export(archive_path: &str, output_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let archive_path = PathBuf::from(archive_path);
    let output_path = PathBuf::from(output_path);

    println!("Reading archive: {}", archive_path.display());
    let archive = WalArchive::read_from_file(&archive_path)?;

    println!(
        "Exporting {} entries to JSON...",
        archive.header.entry_count
    );

    let json = serde_json::to_string_pretty(&archive.body.entries)?;
    std::fs::write(&output_path, json)?;

    let file_size = std::fs::metadata(&output_path)?.len();
    println!("Successfully exported to: {}", output_path.display());
    println!("Output size: {}", format_bytes(file_size));

    Ok(())
}

fn cmd_recover(shard_id: usize) -> Result<(), Box<dyn std::error::Error>> {
    let archive_dir = PathBuf::from(&CONFIG.wal.archive_dir).join(format!("shard-{}", shard_id));

    println!("Recovering all entries from shard {} archives...", shard_id);

    let recovery = WalArchiveRecovery::new(shard_id, archive_dir);
    let entries = recovery.recover_all()?;

    println!();
    println!("Recovered {} total entries", entries.len());

    if !entries.is_empty() {
        println!();
        println!("First entry:");
        println!("  Event Type:  {}", entries[0].event_type);
        println!("  Context ID:  {}", entries[0].context_id);
        println!(
            "  Timestamp:   {} ({})",
            entries[0].timestamp,
            snel_db::shared::time::format_timestamp(entries[0].timestamp)
        );

        println!();
        println!("Last entry:");
        let last = &entries[entries.len() - 1];
        println!("  Event Type:  {}", last.event_type);
        println!("  Context ID:  {}", last.context_id);
        println!(
            "  Timestamp:   {} ({})",
            last.timestamp,
            snel_db::shared::time::format_timestamp(last.timestamp)
        );
    }

    Ok(())
}

fn cmd_archive(shard_id: usize, log_id: u64) -> Result<(), Box<dyn std::error::Error>> {
    println!("Archiving WAL log {} for shard {}...", log_id, shard_id);

    let archiver = WalArchiver::new(shard_id);
    let archive_path = archiver.archive_log(log_id)?;

    println!("Successfully archived to: {}", archive_path.display());

    let file_size = std::fs::metadata(&archive_path)?.len();
    println!("Archive size: {}", format_bytes(file_size));

    Ok(())
}

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        print_usage();
        std::process::exit(1);
    }

    let result = match args[1].as_str() {
        "list" => {
            if args.len() < 3 {
                eprintln!("Error: Missing shard_id argument");
                print_usage();
                std::process::exit(1);
            }
            let shard_id: usize = args[2].parse().expect("Invalid shard_id");
            cmd_list(shard_id)
        }
        "info" => {
            if args.len() < 3 {
                eprintln!("Error: Missing archive_path argument");
                print_usage();
                std::process::exit(1);
            }
            cmd_info(&args[2])
        }
        "export" => {
            if args.len() < 4 {
                eprintln!("Error: Missing archive_path or output_path argument");
                print_usage();
                std::process::exit(1);
            }
            cmd_export(&args[2], &args[3])
        }
        "recover" => {
            if args.len() < 3 {
                eprintln!("Error: Missing shard_id argument");
                print_usage();
                std::process::exit(1);
            }
            let shard_id: usize = args[2].parse().expect("Invalid shard_id");
            cmd_recover(shard_id)
        }
        "archive" => {
            if args.len() < 4 {
                eprintln!("Error: Missing shard_id or log_id argument");
                print_usage();
                std::process::exit(1);
            }
            let shard_id: usize = args[2].parse().expect("Invalid shard_id");
            let log_id: u64 = args[3].parse().expect("Invalid log_id");
            cmd_archive(shard_id, log_id)
        }
        "help" | "--help" | "-h" => {
            print_usage();
            Ok(())
        }
        _ => {
            eprintln!("Error: Unknown command '{}'", args[1]);
            print_usage();
            std::process::exit(1);
        }
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
