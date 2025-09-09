use crate::shared::config::CONFIG;
use sysinfo::Disks;
use tracing::info;

pub struct IoMonitor {
    previous: u64,
}

impl IoMonitor {
    pub fn new(disks: &Disks) -> Self {
        let total_written = disks.list().iter().map(|d| d.usage().written_bytes).sum();
        Self {
            previous: total_written,
        }
    }

    pub fn is_under_pressure(&mut self, disks: &Disks) -> bool {
        let current_written: u64 = disks.list().iter().map(|d| d.usage().written_bytes).sum();
        let delta = current_written.saturating_sub(self.previous);
        self.previous = current_written;

        let delta_mb = delta as f64 / (1024.0 * 1024.0);
        let delta_mb_per_sec = delta_mb / CONFIG.engine.compaction_interval as f64;
        info!("delta_mb_per_sec: {}", delta_mb_per_sec);
        delta_mb_per_sec > CONFIG.engine.sys_io_threshold as f64
    }
}
