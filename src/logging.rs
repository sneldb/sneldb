use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;

use crate::shared::config::CONFIG;
use tracing::info;

pub fn init() -> anyhow::Result<()> {
    info!("Initializing logging");
    let cfg = &CONFIG.logging;
    let stdout_filter = cfg
        .stdout_level
        .parse::<tracing_subscriber::filter::LevelFilter>()?;
    let file_filter = cfg
        .file_level
        .parse::<tracing_subscriber::filter::LevelFilter>()?;

    let stdout_layer = fmt::layer().with_ansi(true).with_filter(stdout_filter);

    let file_appender = tracing_appender::rolling::daily(&cfg.log_dir, "sneldb.log");
    let file_layer = fmt::layer()
        .with_ansi(false)
        .with_writer(file_appender)
        .with_filter(file_filter);

    tracing_subscriber::registry()
        .with(stdout_layer)
        .with(file_layer)
        .init();

    info!("Logging initialized");
    Ok(())
}

#[cfg(test)]
pub fn init_for_tests() {
    use std::sync::Once;
    use tracing_subscriber::EnvFilter;

    static INIT: Once = Once::new();

    INIT.call_once(|| {
        let filter = EnvFilter::from_default_env().add_directive("snel_db=debug".parse().unwrap()); // or any level you want

        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_test_writer() // sends logs to captured test output
            .init();
    });
}
