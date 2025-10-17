#![feature(portable_simd)]
use snel_db::engine::core::read::cache::{
    GlobalColumnBlockCache, GlobalZoneIndexCache, GlobalZoneSurfCache,
};
use snel_db::frontend::start_all;
use snel_db::logging;
use snel_db::shared::config::CONFIG;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    info!("Starting SnelDB");
    logging::init()?;

    // Configure process-wide cache capacities from config at startup
    if let Some(q) = CONFIG.query.as_ref() {
        if let Some(cap) = q.zone_index_cache_max_entries {
            GlobalZoneIndexCache::instance().resize(cap);
        }
        if let Some(bytes) = q.column_block_cache_max_bytes {
            GlobalColumnBlockCache::instance().resize_bytes(bytes);
        }
        if let Some(bytes) = q.zone_surf_cache_max_bytes {
            GlobalZoneSurfCache::instance().resize_bytes(bytes);
        }
    }

    tracing::info!("SnelDB is starting...");
    let _ = start_all().await;

    Ok(())
}
