use crate::engine::core::WalEntry;
use crate::shared::config::CONFIG;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use tracing::{debug, info, warn};

pub struct InnerWalWriter {
    pub dir: PathBuf,
    pub file: Option<BufWriter<File>>,
    pub current_log_id: u64,
    pub entries_written: u64,
    current_path: PathBuf,
}

impl InnerWalWriter {
    pub fn new(dir: PathBuf) -> std::io::Result<Self> {
        std::fs::create_dir_all(&dir)?;
        let next_id = Self::find_next_wal_id(&dir);
        let entries_written = Self::count_entries(&dir);
        let current_path = dir.join(format!("wal-{:05}.log", next_id));

        info!(
            target: "inner_wal_writer::new",
            ?dir, next_id, entries_written,
            "Initialized WAL writer"
        );

        Ok(Self {
            dir,
            file: None,
            current_log_id: next_id,
            entries_written,
            current_path,
        })
    }

    fn count_entries(dir: &Path) -> u64 {
        let latest_id = Self::last_wal_id(dir);
        let path = dir.join(format!("wal-{:05}.log", latest_id));
        let count = match File::open(&path) {
            Ok(file) => BufReader::new(file).lines().count() as u64,
            Err(_) => 0,
        };

        debug!(
            target: "inner_wal_writer::count_entries",
            latest_id, count,
            "Counted entries in last WAL file"
        );
        count
    }

    fn last_wal_id(dir: &Path) -> u64 {
        std::fs::read_dir(dir)
            .ok()
            .into_iter()
            .flat_map(|dir| dir)
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| {
                let name = entry.file_name().to_string_lossy().to_string();
                name.strip_prefix("wal-")
                    .and_then(|s| s.strip_suffix(".log"))
                    .and_then(|n| n.parse::<u64>().ok())
            })
            .max()
            .unwrap_or(0)
    }

    pub fn start_next_log_file(&mut self) -> std::io::Result<()> {
        self.current_path = self.dir.join(format!("wal-{:05}.log", self.current_log_id));

        info!(
            target: "inner_wal_writer::start_next_log_file",
            path = ?self.current_path, log_id = self.current_log_id,
            "Starting new WAL log file"
        );

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&self.current_path)?;

        let writer = if CONFIG.wal.buffered {
            BufWriter::with_capacity(CONFIG.wal.buffer_size, file)
        } else {
            BufWriter::with_capacity(0, file)
        };

        self.file = Some(writer);
        self.entries_written = Self::count_entries(&self.dir);
        Ok(())
    }

    pub fn rotate_log_file(&mut self) -> std::io::Result<()> {
        info!(
            target: "inner_wal_writer::rotate_log_file",
            old_log = self.current_log_id,
            "Rotating WAL log file"
        );
        self.flush_and_close()?;
        self.current_log_id += 1;
        self.start_next_log_file()
    }

    pub fn append_immediate(&mut self, entry: &WalEntry) -> std::io::Result<()> {
        info!(
            target: "inner_wal_writer::append_immediate",
            path = ?self.current_path,
            "Appending entry to WAL"
        );
        if let Some(file) = self.file.as_mut() {
            let json = serde_json::to_string(entry)?;
            debug!(target: "inner_wal_writer::append_immediate", json = %json, "Serialized WAL entry");

            file.write_all(json.as_bytes())?;
            file.write_all(b"\n")?;

            if CONFIG.wal.flush_each_write {
                file.flush()?;
            }

            if CONFIG.wal.fsync
                && self.entries_written % CONFIG.wal.fsync_every_n.unwrap_or(32) as u64 == 0
            {
                file.get_ref().sync_all()?;
            }

            self.entries_written += 1;
            debug!(
                target: "inner_wal_writer::append_immediate",
                timestamp = entry.timestamp, total = self.entries_written,
                "WAL entry appended"
            );
        } else {
            panic!("WAL segment must be started before appending");
        }
        Ok(())
    }

    pub fn flush_and_close(&mut self) -> std::io::Result<()> {
        if let Some(mut file) = self.file.take() {
            debug!(
                target: "inner_wal_writer::flush_and_close",
                log_id = self.current_log_id, entries = self.entries_written,
                "Flushing WAL log file"
            );
            file.flush()?;

            if CONFIG.wal.fsync {
                file.get_ref().sync_all()?;
            }

            let file_path = self.dir.join(format!("wal-{:05}.log", self.current_log_id));
            match std::fs::metadata(&file_path) {
                Ok(metadata) => debug!(
                    target: "inner_wal_writer::flush_and_close",
                    path = ?file_path, size = metadata.len(),
                    "WAL file closed"
                ),
                Err(_) => warn!(
                    target: "inner_wal_writer::flush_and_close",
                    path = ?file_path,
                    "Could not read WAL file metadata after flush"
                ),
            }
        }
        Ok(())
    }

    fn find_next_wal_id(wal_dir: &Path) -> u64 {
        let last_log_id = Self::last_wal_id(wal_dir);
        debug!(target: "inner_wal_writer::find_next_wal_id", last_log_id, "Finding next WAL ID");

        if last_log_id == 0 {
            return 0;
        }

        let last_log_path = wal_dir.join(format!("wal-{:05}.log", last_log_id));
        if let Ok(file) = File::open(&last_log_path) {
            let reader = BufReader::new(file);
            let line_count = reader.lines().count();
            debug!(
                target: "inner_wal_writer::find_next_wal_id",
                last_log_id, line_count, threshold = CONFIG.engine.flush_threshold,
                "Checking if rollover needed"
            );
            if line_count < CONFIG.engine.flush_threshold {
                return last_log_id;
            }
        }

        last_log_id + 1
    }
}
