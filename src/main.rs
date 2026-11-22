use anyhow::Result;
use cross_venue_margin::config::Config;
use cross_venue_margin::engine::task::{self, EngineHandle};
use cross_venue_margin::engine::{Engine, Event};
use cross_venue_margin::feed::Feeds;
use cross_venue_margin::metrics::Metrics;
use cross_venue_margin::risk::{MarginTable, MarketId, Schedule, Thresholds, Venue};
use cross_venue_margin::store::{self, EventStore};
use cross_venue_margin::{server, venue};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Commands waiting for the engine. Senders wait when it is full.
const COMMAND_QUEUE: usize = 64;
/// Events waiting for the log. The engine waits when it is full.
const EVENT_QUEUE: usize = 1024;

/// Receives every engine event when there is no database, and writes each
/// one to the process log.
async fn log_events(mut events: mpsc::Receiver<Event>) {
    while let Some(event) = events.recv().await {
        match serde_json::to_string(&event) {
            Ok(json) => info!(event = %json, "engine event"),
            Err(error) => warn!(%error, "event did not serialize"),
        }
    }
}

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

    let mut markets = Vec::new();
    for coin in &config.coins {
        markets.push(MarketId::new(Venue::Hyperliquid, coin.as_str()));
        if venue::okx::instrument(coin).is_some() {
            markets.push(MarketId::new(Venue::Okx, coin.as_str()));
        } else {
            warn!(coin, "no OKX instrument, tracking it on Hyperliquid only");
        }
    }
    let (feeds, mut publishers) = Feeds::channels(&markets);
    let shutdown = CancellationToken::new();
    let metrics = Metrics::new();

    let mut schedule = Schedule::new();
    for market in &markets {
        let table: Option<MarginTable> = match market.venue {
            Venue::Hyperliquid => {
                venue::hyperliquid::maintenance_table(&market.coin).map(Into::into)
            }
            Venue::Okx => venue::okx::maintenance_table(&market.coin).map(Into::into),
        };
        match table {
            Some(table) => {
                schedule.insert(market.clone(), table);
            }
            None => warn!(%market, "no maintenance table, positions will be refused"),
        }
    }
    let thresholds = Thresholds::new(config.margin_call_multiple)?;
    let (engine, store) = match &config.database_url {
        Some(url) => {
            let store = EventStore::connect(url.expose()).await?;
            let events = store.load().await?;
            let engine = Engine::restore(schedule, thresholds, &events)?;
            info!(
                events = events.len(),
                "accounts restored from the event log"
            );
            (engine, Some(store))
        }
        None => {
            warn!("DATABASE_URL is not set, accounts will not survive a restart");
            (Engine::new(schedule, thresholds), None)
        }
    };
    let marks = feeds
        .mark_receivers()
        .map(|(market, rx)| (market.clone(), rx))
        .collect();
    let (command_tx, command_rx) = mpsc::channel(COMMAND_QUEUE);
    let (event_tx, event_rx) = mpsc::channel(EVENT_QUEUE);

    let hyperliquid = tokio::spawn(venue::hyperliquid::run(
        config.hl_ws_url.clone(),
        publishers.take(Venue::Hyperliquid),
        metrics.clone(),
        shutdown.clone(),
    ));
    let okx = tokio::spawn(venue::okx::run(
        config.okx_ws_url.clone(),
        publishers.take(Venue::Okx),
        metrics.clone(),
        shutdown.clone(),
    ));
    let risk = tokio::spawn(task::run(
        engine,
        marks,
        command_rx,
        event_tx,
        metrics.clone(),
        shutdown.clone(),
    ));
    let event_log = match store {
        Some(store) => tokio::spawn(store::run(
            store,
            event_rx,
            metrics.clone(),
            shutdown.clone(),
        )),
        None => tokio::spawn(log_events(event_rx)),
    };
    let api = tokio::spawn(server::serve(
        config.bind,
        feeds,
        EngineHandle::new(command_tx),
        metrics,
        shutdown.clone(),
    ));

    tokio::signal::ctrl_c().await?;
    info!("shutdown requested");
    shutdown.cancel();

    hyperliquid.await?;
    okx.await?;
    risk.await?;
    event_log.await?;
    api.await??;
    Ok(())
}
