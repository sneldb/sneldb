pub mod http;
pub mod tcp;
pub mod unix;

pub async fn start_all() -> anyhow::Result<()> {
    tokio::try_join!(
        //tcp::listener::run_tcp_server(),
        tcp::listener::run_tcp_server(),
        http::listener::run_http_server(),
    )?;
    Ok(())
}
