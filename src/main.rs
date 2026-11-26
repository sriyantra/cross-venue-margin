use anyhow::Result;
use cross_venue_margin::config::{self, Config};
use cross_venue_margin::engine::task::{self, EngineHandle};
use cross_venue_margin::engine::{Engine, Event};
use cross_venue_margin::execution::{self, Close, Executor};
use cross_venue_margin::feed::Feeds;
use cross_venue_margin::metrics::Metrics;
use cross_venue_margin::risk::{
    CollateralRule, MarginTable, MarketId, Schedule, Thresholds, Venue,
};
use cross_venue_margin::store::{self, EventStore};
use cross_venue_margin::{server, venue};
use std::sync::Arc;

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

/// Passes every engine event on to the log, and asks for a close on the
/// venue for each position the engine liquidates. It never waits on the
/// venue, so the log is not held up by it.
async fn forward_events(
    mut from: mpsc::Receiver<Event>,
    to: mpsc::Sender<Event>,
    closes: mpsc::UnboundedSender<Close>,
) {
    while let Some(event) = from.recv().await {
        if let Some(close) = Close::from_event(&event) {
            let _ = closes.send(close);
        }
        if to.send(event).await.is_err() {
            break;
        }
    }
}

/// Resolves on Ctrl-C, or on SIGTERM, which is what a container runtime
/// sends to stop a process.
async fn stop_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = signal(SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await
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
                let fee = match market.venue {
                    Venue::Hyperliquid => venue::hyperliquid::TAKER_FEE,
                    Venue::Okx => venue::okx::TAKER_FEE,
                };
                schedule.set_fee(market.clone(), fee)?;
            }
            None => warn!(%market, "no maintenance table, positions will be refused"),
        }
    }
    // A coin held as collateral is valued at its Hyperliquid mark.
    for coin in &config.coins {
        if let Some(haircut) = config::collateral_haircut(coin) {
            let price = MarketId::new(Venue::Hyperliquid, coin.as_str());
            schedule.insert_collateral(coin.as_str(), CollateralRule::new(price, haircut)?);
        }
    }
    schedule.set_hedge_rate(config.hedge_margin_rate)?;
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
    let books = feeds
        .book_receivers()
        .map(|(market, rx)| (market.clone(), rx))
        .collect();
    let fundings = feeds
        .funding_receivers()
        .map(|(market, rx)| (market.clone(), rx))
        .collect();
    let (command_tx, command_rx) = mpsc::channel(COMMAND_QUEUE);
    let (event_tx, event_rx) = mpsc::channel(EVENT_QUEUE);

    let executor = match &config.hl_testnet_key {
        Some(key) => {
            let executor = Executor::connect(execution::TESTNET_API, key.expose()).await?;
            info!(
                address = executor.address(),
                "Hyperliquid fills are orders on testnet"
            );
            Some(Arc::new(executor))
        }
        None => None,
    };
    // With an executor, events pass through a task that also asks for
    // liquidated positions to be closed on the venue.
    let (event_rx, venue_tasks) = match &executor {
        Some(executor) => {
            let (log_tx, log_rx) = mpsc::channel(EVENT_QUEUE);
            let (close_tx, close_rx) = mpsc::unbounded_channel();
            let forward = tokio::spawn(forward_events(event_rx, log_tx, close_tx));
            let closes = tokio::spawn(execution::run_closes(executor.clone(), close_rx));
            (log_rx, Some((forward, closes)))
        }
        None => (event_rx, None),
    };

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
        books,
        fundings,
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
        config.read_only,
        executor,
        shutdown.clone(),
    ));

    stop_signal().await?;
    info!("shutdown requested");
    shutdown.cancel();

    hyperliquid.await?;
    okx.await?;
    risk.await?;
    if let Some((forward, closes)) = venue_tasks {
        forward.await?;
        closes.await?;
    }
    event_log.await?;
    api.await??;
    Ok(())
}
