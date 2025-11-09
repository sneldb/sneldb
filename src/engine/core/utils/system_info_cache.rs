use crate::shared::config::CONFIG;
use once_cell::sync::Lazy;
use std::sync::Arc;
use std::time::{Duration, Instant};
use sysinfo::{Disks, System};
use tokio::sync::RwLock;
use tokio::time::interval;
use tracing::debug;

/// Cached system information that refreshes in the background
pub struct SystemInfoCache {
    /// Cached system information
    system: Arc<RwLock<System>>,
    /// Cached disk information
    disks: Arc<RwLock<Disks>>,
    /// Last refresh time
    last_refresh: Arc<RwLock<Instant>>,
    /// Refresh interval
    refresh_interval: Duration,
}

impl SystemInfoCache {
    /// Create a new SystemInfoCache with the given refresh interval
    pub fn new(refresh_interval: Duration) -> Self {
        let system = Arc::new(RwLock::new(System::new_all()));
        let disks = Arc::new(RwLock::new(Disks::new_with_refreshed_list()));
        let last_refresh = Arc::new(RwLock::new(Instant::now()));

        Self {
            system,
            disks,
            last_refresh,
            refresh_interval,
        }
    }

    /// Start background refresh task
    /// This spawns a tokio task that periodically refreshes system info
    pub fn spawn_refresh_task(self: Arc<Self>) {
        let cache = Arc::clone(&self);
        let refresh_interval = self.refresh_interval;
        tokio::spawn(async move {
            let mut refresh_timer = interval(refresh_interval);

            loop {
                refresh_timer.tick().await;

                // Refresh in background (non-blocking for queries)
                // Clone Arc references before moving into spawn_blocking
                let system_clone = Arc::clone(&cache.system);
                let disks_clone = Arc::clone(&cache.disks);
                let last_refresh_clone = Arc::clone(&cache.last_refresh);

                // Use spawn_blocking for heavy sysinfo operations
                tokio::task::spawn_blocking(move || {
                    // Heavy sysinfo operations in blocking thread
                    let mut sys = System::new_all();
                    sys.refresh_all();

                    let disks = Disks::new_with_refreshed_list();

                    // Update cache atomically
                    *system_clone.blocking_write() = sys;
                    *disks_clone.blocking_write() = disks;
                    *last_refresh_clone.blocking_write() = Instant::now();

                    if tracing::enabled!(tracing::Level::DEBUG) {
                        debug!(target: "sneldb::system_info", "Refreshed system info cache");
                    }
                })
                .await
                .ok();
            }
        });
    }

    /// Get cached system info (fast, non-blocking)
    /// Returns a reference guard that can be used to access system info
    pub async fn get_system(&self) -> tokio::sync::RwLockReadGuard<'_, System> {
        self.system.read().await
    }

    /// Get cached disks info (fast, non-blocking)
    /// Returns a reference guard that can be used to access disk info
    pub async fn get_disks(&self) -> tokio::sync::RwLockReadGuard<'_, Disks> {
        self.disks.read().await
    }

    /// Check if cache is stale (for force refresh if needed)
    pub async fn is_stale(&self) -> bool {
        let last = *self.last_refresh.read().await;
        last.elapsed() > self.refresh_interval
    }

    /// Get last refresh time
    pub async fn last_refresh(&self) -> Instant {
        *self.last_refresh.read().await
    }
}

/// Global singleton SystemInfoCache
/// Refreshes every 5 seconds by default
static SYSTEM_INFO_CACHE: Lazy<Arc<SystemInfoCache>> = Lazy::new(|| {
    let refresh_interval =
        Duration::from_secs(CONFIG.engine.system_info_refresh_interval.unwrap_or(5));
    let cache = Arc::new(SystemInfoCache::new(refresh_interval));
    let cache_clone = Arc::clone(&cache);
    cache_clone.spawn_refresh_task();
    cache
});

/// Get the global SystemInfoCache instance
pub fn get_system_info_cache() -> Arc<SystemInfoCache> {
    Arc::clone(&SYSTEM_INFO_CACHE)
}
