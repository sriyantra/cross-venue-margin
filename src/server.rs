//! HTTP and WebSocket API.
//!
//! - `GET /health`               liveness of every tracked market
//! - `GET /book/{venue}/{coin}`  the current book, once
//! - `GET /mark/{venue}/{coin}`  the venue's current mark price
//! - `GET /ws/{venue}/{coin}`    WebSocket: the current book, then every update
//! - `POST /accounts`             open a paper account
//! - `GET /accounts/{id}`         health, state and positions
//! - `POST /accounts/{id}/deposits`  add collateral: dollars or a coin
//! - `POST /accounts/{id}/fills`  fill a paper order at the mark
//! - `GET /metrics`              process metrics for a Prometheus scraper
//!
//! Market data handlers only read from watch channels. They never touch a
//! `Book`. Account handlers send a command to the engine task and wait for
//! its reply. They never touch an account.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{FromRef, Path, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::engine::task::EngineHandle;
use crate::engine::{AccountId, EngineError};
use crate::feed::{BookRx, BookView, Feeds, MarkView, Slot};
use crate::metrics::{self, Metrics};
use crate::risk::{MarketId, Side, Venue};

/// What every handler can reach. Each handler asks for only the part it
/// needs, through the `FromRef` impls below.
#[derive(Clone)]
struct AppState {
    feeds: Arc<Feeds>,
    engine: EngineHandle,
    metrics: Metrics,
}

impl FromRef<AppState> for Arc<Feeds> {
    fn from_ref(state: &AppState) -> Self {
        state.feeds.clone()
    }
}

impl FromRef<AppState> for EngineHandle {
    fn from_ref(state: &AppState) -> Self {
        state.engine.clone()
    }
}

impl FromRef<AppState> for Metrics {
    fn from_ref(state: &AppState) -> Self {
        state.metrics.clone()
    }
}

/// Amounts are JSON strings. A JSON number would pass through a binary
/// float on the way in.
#[derive(Debug, Deserialize)]
struct CreateAccount {
    #[serde(with = "rust_decimal::serde::str")]
    collateral: Decimal,
}

#[derive(Debug, Deserialize)]
struct DepositRequest {
    asset: String,
    #[serde(with = "rust_decimal::serde::str")]
    amount: Decimal,
}

#[derive(Debug, Deserialize)]
struct FillRequest {
    venue: Venue,
    coin: String,
    side: Side,
    #[serde(with = "rust_decimal::serde::str")]
    size: Decimal,
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
}

/// What a client receives for one market, over REST or WebSocket.
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum BookMessage<'a> {
    Book(&'a BookView),
    /// The book is not trustworthy right now. Clients must stop using the
    /// last book they received.
    Stale {
        #[serde(flatten)]
        market: &'a MarketId,
    },
}

impl<'a> BookMessage<'a> {
    fn new(market: &'a MarketId, slot: &'a Slot) -> Self {
        match slot {
            Some(view) => Self::Book(view),
            None => Self::Stale { market },
        }
    }
}

/// What a client receives for one market's mark price.
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum MarkMessage<'a> {
    Mark(&'a MarkView),
    Stale {
        #[serde(flatten)]
        market: &'a MarketId,
    },
}

#[derive(Debug, Serialize)]
struct MarketHealth {
    #[serde(flatten)]
    market: MarketId,
    live: bool,
    mark_live: bool,
    /// Milliseconds between the venue timestamp of the last applied message
    /// and this machine's clock.
    age_ms: Option<u64>,
}

#[derive(Debug, Serialize)]
struct Health {
    ok: bool,
    markets: Vec<MarketHealth>,
}

/// With `read_only` set, the routes that change an account answer 403 and
/// never reach the engine.
pub fn router(feeds: Feeds, engine: EngineHandle, metrics: Metrics, read_only: bool) -> Router {
    let writes = if read_only {
        Router::new()
            .route("/accounts", post(refuse_write))
            .route("/accounts/{id}/deposits", post(refuse_write))
            .route("/accounts/{id}/fills", post(refuse_write))
    } else {
        Router::new()
            .route("/accounts", post(create_account))
            .route("/accounts/{id}/deposits", post(deposit))
            .route("/accounts/{id}/fills", post(fill))
    };
    Router::new()
        .route("/health", get(health))
        .route("/book/{venue}/{coin}", get(book))
        .route("/mark/{venue}/{coin}", get(mark))
        .route("/ws/{venue}/{coin}", get(ws_upgrade))
        .route("/accounts/{id}", get(account))
        .route("/metrics", get(render_metrics))
        .merge(writes)
        .with_state(AppState {
            feeds: Arc::new(feeds),
            engine,
            metrics,
        })
}

/// Serves the API until `shutdown` is cancelled.
pub async fn serve(
    bind: SocketAddr,
    feeds: Feeds,
    engine: EngineHandle,
    metrics: Metrics,
    read_only: bool,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(bind).await?;
    info!(addr = %listener.local_addr()?, read_only, "api listening");
    axum::serve(listener, router(feeds, engine, metrics, read_only))
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await?;
    Ok(())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

async fn health(State(feeds): State<Arc<Feeds>>) -> Response {
    let now = now_ms();
    let markets: Vec<MarketHealth> = feeds
        .markets()
        .map(|market| {
            let slot = feeds.latest(market).flatten();
            MarketHealth {
                market: market.clone(),
                live: slot.is_some(),
                mark_live: feeds.latest_mark(market).flatten().is_some(),
                age_ms: slot.map(|view| now.saturating_sub(view.time)),
            }
        })
        .collect();

    let ok = markets.iter().all(|m| m.live && m.mark_live);
    let status = if ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(Health { ok, markets })).into_response()
}

async fn book(
    State(feeds): State<Arc<Feeds>>,
    Path((venue, coin)): Path<(Venue, String)>,
) -> Response {
    let market = MarketId::new(venue, coin);
    match feeds.latest(&market) {
        None => (StatusCode::NOT_FOUND, "unknown market").into_response(),
        Some(slot) => {
            let status = if slot.is_some() {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            };
            (status, Json(BookMessage::new(&market, &slot))).into_response()
        }
    }
}

async fn mark(
    State(feeds): State<Arc<Feeds>>,
    Path((venue, coin)): Path<(Venue, String)>,
) -> Response {
    let market = MarketId::new(venue, coin);
    match feeds.latest_mark(&market) {
        None => (StatusCode::NOT_FOUND, "unknown market").into_response(),
        Some(Some(view)) => Json(MarkMessage::Mark(&view)).into_response(),
        Some(None) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(MarkMessage::Stale { market: &market }),
        )
            .into_response(),
    }
}

async fn render_metrics(State(metrics): State<Metrics>) -> Response {
    (
        [(header::CONTENT_TYPE, metrics::CONTENT_TYPE)],
        metrics.render(),
    )
        .into_response()
}

fn engine_error(error: EngineError) -> Response {
    let status = match &error {
        EngineError::UnknownAccount(_) => StatusCode::NOT_FOUND,
        EngineError::AccountClosed(_)
        | EngineError::InsufficientMargin
        | EngineError::InsufficientLiquidity { .. } => StatusCode::CONFLICT,
        EngineError::NoMark(_) | EngineError::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        EngineError::NegativeCollateral(_)
        | EngineError::NonPositiveDeposit(_)
        | EngineError::UnsupportedAsset(_)
        | EngineError::UnsupportedMarket(_)
        | EngineError::PositionTooLarge { .. }
        | EngineError::InvalidFill(_) => StatusCode::BAD_REQUEST,
        EngineError::Risk(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    let body = ErrorBody {
        error: error.to_string(),
    };
    (status, Json(body)).into_response()
}

async fn refuse_write() -> Response {
    let body = ErrorBody {
        error: "this deployment is read-only".into(),
    };
    (StatusCode::FORBIDDEN, Json(body)).into_response()
}

async fn create_account(
    State(engine): State<EngineHandle>,
    Json(body): Json<CreateAccount>,
) -> Response {
    match engine.create_account(body.collateral).await {
        Ok(view) => (StatusCode::CREATED, Json(view)).into_response(),
        Err(error) => engine_error(error),
    }
}

async fn account(State(engine): State<EngineHandle>, Path(id): Path<u64>) -> Response {
    match engine.get(AccountId(id)).await {
        Ok(Some(view)) => Json(view).into_response(),
        Ok(None) => engine_error(EngineError::UnknownAccount(AccountId(id))),
        Err(error) => engine_error(error),
    }
}

async fn deposit(
    State(engine): State<EngineHandle>,
    Path(id): Path<u64>,
    Json(body): Json<DepositRequest>,
) -> Response {
    match engine.deposit(AccountId(id), body.asset, body.amount).await {
        Ok(view) => Json(view).into_response(),
        Err(error) => engine_error(error),
    }
}

async fn fill(
    State(engine): State<EngineHandle>,
    Path(id): Path<u64>,
    Json(body): Json<FillRequest>,
) -> Response {
    let market = MarketId::new(body.venue, body.coin);
    match engine
        .fill(AccountId(id), market, body.side, body.size)
        .await
    {
        Ok(view) => Json(view).into_response(),
        Err(error) => engine_error(error),
    }
}

async fn ws_upgrade(
    State(feeds): State<Arc<Feeds>>,
    Path((venue, coin)): Path<(Venue, String)>,
    upgrade: WebSocketUpgrade,
) -> Response {
    let market = MarketId::new(venue, coin);
    match feeds.subscribe(&market) {
        None => (StatusCode::NOT_FOUND, "unknown market").into_response(),
        Some(rx) => upgrade.on_upgrade(move |socket| stream_book(socket, market, rx)),
    }
}

/// Pushes the current book, then each change, until the client goes away.
async fn stream_book(mut socket: WebSocket, market: MarketId, mut rx: BookRx) {
    // Mark the current value as unseen so the first loop pass sends it.
    rx.mark_changed();
    loop {
        tokio::select! {
            changed = rx.changed() => {
                // An error here means the connector task is gone.
                if changed.is_err() {
                    break;
                }
                // Serialize while holding the borrow, then release it before
                // the await below. A watch borrow is a read lock, and holding
                // one across an await would block the connector's next write.
                let payload = {
                    let slot = rx.borrow_and_update();
                    serde_json::to_string(&BookMessage::new(&market, &slot))
                };
                let Ok(payload) = payload else { break };
                if socket.send(Message::Text(payload.into())).await.is_err() {
                    break;
                }
            }
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    Some(Ok(_)) => {}
                }
            }
        }
    }
    debug!(%market, "ws client disconnected");
}
