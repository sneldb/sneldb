use crate::shared::config::CONFIG;
use sysinfo::System;
use tracing::info;

pub struct MemoryMonitor;

impl MemoryMonitor {
    pub fn new() -> Self {
        Self
    }

    pub fn is_under_pressure(&self, system: &System) -> bool {
        // Calculate available memory as total - used
        let total_memory = system.total_memory();
        let used_memory = system.used_memory();
        let available_memory = total_memory.saturating_sub(used_memory);
        let available_mb = available_memory / (1024 * 1024);
        let threshold_mb = CONFIG.engine.sys_memory_threshold_mb as u64;

        info!(
            "available_memory_mb: {}, threshold_mb: {}",
            available_mb, threshold_mb
        );

        available_mb < threshold_mb
    }
}
