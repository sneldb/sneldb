use snel_db::frontend::start_all;
use snel_db::logging;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    info!("Starting SnelDB");
    logging::init()?;

    tracing::info!("SnelDB is starting...");
    let _ = start_all().await;

    Ok(())
}
