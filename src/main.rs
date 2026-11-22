use anyhow::Result;
use cross_venue_margin::config::Config;
use cross_venue_margin::feed::Feeds;
use cross_venue_margin::{server, venue};
use tokio_util::sync::CancellationToken;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    // rustls needs one process-wide crypto provider chosen before the first
    // TLS connection.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let config = Config::from_env()?;
    info!(?config, "starting");

    let (feeds, publishers) = Feeds::channels(&config.coins);
    let shutdown = CancellationToken::new();

    let connector = tokio::spawn(venue::hyperliquid::run(
        config.hl_ws_url.clone(),
        publishers,
        shutdown.clone(),
    ));
    let api = tokio::spawn(server::serve(config.bind, feeds, shutdown.clone()));

    tokio::signal::ctrl_c().await?;
    info!("shutdown requested");
    shutdown.cancel();

    connector.await?;
    api.await??;
    Ok(())
}
