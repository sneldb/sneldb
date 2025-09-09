use snel_db::frontend::start_all;
use snel_db::logging;
use snel_db::shared::config::CONFIG;
use std::fs;
use tracing::info;

fn clear_debug_data() -> std::io::Result<()> {
    println!("Clearing debug data");
    fs::remove_dir_all(&CONFIG.schema.def_dir);
    fs::remove_dir_all(&CONFIG.engine.data_dir);
    fs::remove_dir_all(&CONFIG.engine.index_dir);
    fs::remove_dir_all(&CONFIG.logging.log_dir);
    fs::remove_dir_all(&CONFIG.wal.dir);
    info!("Cleared all contents of data/*");
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    info!("Starting SnelDB");
    logging::init()?;
    clear_debug_data()?;

    tracing::info!("SnelDB is starting...");
    start_all().await;

    Ok(())
}
