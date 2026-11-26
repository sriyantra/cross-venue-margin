//! Runs the API with an executor pointed at a stand-in for the venue, and
//! checks what reaches the venue and what the engine records.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use cross_venue_margin::engine::Engine;
use cross_venue_margin::engine::task::{self, Command, EngineHandle};
use cross_venue_margin::execution::{self, Close, Executor};
use cross_venue_margin::feed::{Feeds, MarkView};
use cross_venue_margin::metrics::Metrics;
use cross_venue_margin::risk::{MarketId, Schedule, Side, Thresholds, TierTable, Venue};
use cross_venue_margin::server;
use rust_decimal::dec;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

const WAIT: Duration = Duration::from_secs(5);
const KEY: &str = "0x0101010101010101010101010101010101010101010101010101010101010101";
const ADDRESS: &str = "0x1a642f0e3c3af545e7acbd38b07251b3990914f1";

/// What the stand-in venue has been sent, and what it answers.
struct StandIn {
    orders: Vec<Value>,
    order_reply: Value,
    position: &'static str,
}

type Shared = Arc<Mutex<StandIn>>;

fn filled(size: &str, px: &str) -> Value {
    json!({"status": "ok", "response": {"type": "order", "data": {"statuses": [
        {"filled": {"totalSz": size, "avgPx": px, "oid": 42}}
    ]}}})
}

async fn info(State(venue): State<Shared>, Json(body): Json<Value>) -> Json<Value> {
    match body["type"].as_str() {
        Some("meta") => Json(json!({"universe": [
            {"name": "ETH", "szDecimals": 4},
            {"name": "BTC", "szDecimals": 5}
        ]})),
        Some("clearinghouseState") => {
            assert_eq!(body["user"], ADDRESS);
            let position = venue.lock().unwrap().position;
            Json(json!({
                "marginSummary": {"accountValue": "110.0"},
                "assetPositions": [{"position": {"coin": "BTC", "szi": position}}]
            }))
        }
        other => panic!("unexpected info request {other:?}"),
    }
}

async fn exchange(State(venue): State<Shared>, Json(body): Json<Value>) -> Json<Value> {
    let mut venue = venue.lock().unwrap();
    venue.orders.push(body);
    Json(venue.order_reply.clone())
}

struct Harness {
    api: String,
    venue: Shared,
    http: reqwest::Client,
    _shutdown: tokio_util::sync::DropGuard,
}

async fn listen(router: Router) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    addr
}

async fn start(order_reply: Value) -> Harness {
    let venue: Shared = Arc::new(Mutex::new(StandIn {
        orders: Vec::new(),
        order_reply,
        position: "0.0",
    }));
    let venue_addr = listen(
        Router::new()
            .route("/info", post(info))
            .route("/exchange", post(exchange))
            .with_state(venue.clone()),
    )
    .await;
    let executor = Executor::connect(&format!("http://{venue_addr}"), KEY)
        .await
        .unwrap();
    assert_eq!(executor.address(), ADDRESS);

    let markets = [
        MarketId::new(Venue::Hyperliquid, "BTC"),
        MarketId::new(Venue::Okx, "BTC"),
    ];
    let (feeds, mut publishers) = Feeds::channels(&markets);
    let mut schedule = Schedule::new();
    for market in &markets {
        schedule.insert(market.clone(), TierTable::flat(dec!(0.0125)).unwrap());
    }
    let engine = Engine::new(schedule, Thresholds::new(dec!(1.5)).unwrap());
    let marks = feeds
        .mark_receivers()
        .map(|(market, rx)| (market.clone(), rx))
        .collect();
    for market in &markets {
        let publisher = publishers.take(market.venue).remove("BTC").unwrap();
        publisher.mark.send_replace(Some(MarkView {
            venue: market.venue,
            coin: "BTC".into(),
            mark: dec!(60000),
            received: tokio::time::Instant::now(),
        }));
        // The channel closes with its sender, so keep it for the test.
        std::mem::forget(publisher);
    }

    let (command_tx, command_rx) = mpsc::channel::<Command>(8);
    let (event_tx, mut events) = mpsc::channel(64);
    let shutdown = CancellationToken::new();
    tokio::spawn(task::run(
        engine,
        marks,
        Vec::new(),
        command_rx,
        event_tx,
        Metrics::new(),
        shutdown.clone(),
    ));
    tokio::spawn(async move { while events.recv().await.is_some() {} });

    let api = listen(server::router(
        feeds,
        EngineHandle::new(command_tx),
        Metrics::new(),
        false,
        Some(Arc::new(executor)),
    ))
    .await;
    Harness {
        api: format!("http://{api}"),
        venue,
        http: reqwest::Client::builder().timeout(WAIT).build().unwrap(),
        _shutdown: shutdown.drop_guard(),
    }
}

impl Harness {
    async fn post(&self, path: &str, body: Value) -> (u16, Value) {
        let reply = self
            .http
            .post(format!("{}{path}", self.api))
            .json(&body)
            .send()
            .await
            .unwrap();
        (reply.status().as_u16(), reply.json().await.unwrap())
    }

    async fn get(&self, path: &str) -> (u16, Value) {
        let reply = self
            .http
            .get(format!("{}{path}", self.api))
            .send()
            .await
            .unwrap();
        (reply.status().as_u16(), reply.json().await.unwrap())
    }

    async fn open(&self, collateral: &str) -> u64 {
        let (status, account) = self
            .post("/accounts", json!({ "collateral": collateral }))
            .await;
        assert_eq!(status, 201);
        account["id"].as_u64().unwrap()
    }

    /// Sends a fill, retrying while the engine has not seen the mark yet.
    async fn fill(&self, id: u64, venue: &str, side: &str, size: &str) -> (u16, Value) {
        let body = json!({"venue": venue, "coin": "BTC", "side": side, "size": size});
        tokio::time::timeout(WAIT, async {
            loop {
                let (status, reply) = self
                    .post(&format!("/accounts/{id}/fills"), body.clone())
                    .await;
                if status != 503 {
                    return (status, reply);
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("timed out waiting for a mark")
    }

    fn orders(&self) -> Vec<Value> {
        self.venue.lock().unwrap().orders.clone()
    }
}

#[tokio::test]
async fn a_fill_is_sent_to_the_venue_and_recorded_as_the_venue_filled_it() {
    // The venue fills half of what is asked, above the engine's mark.
    let harness = start(filled("0.01", "60100.0")).await;
    let id = harness.open("5000").await;

    let (status, account) = harness.fill(id, "hyperliquid", "long", "0.02").await;
    assert_eq!(status, 200, "{account}");
    let position = &account["positions"][0];
    assert_eq!(position["size"], "0.01");
    assert_eq!(position["entry_px"], "60100.0");

    let orders = harness.orders();
    assert_eq!(orders.len(), 1);
    // BTC is the second market the venue lists. The limit is 5% above the
    // mark, and the order cancels what does not fill at once.
    assert_eq!(
        orders[0]["action"],
        json!({"type": "order", "grouping": "na", "orders": [{
            "a": 1, "b": true, "p": "63000", "s": "0.02", "r": false,
            "t": {"limit": {"tif": "Ioc"}}
        }]})
    );
    assert!(orders[0]["nonce"].as_u64().unwrap() > 0);
    assert!(
        orders[0]["signature"]["r"]
            .as_str()
            .unwrap()
            .starts_with("0x")
    );
    assert!(orders[0]["vaultAddress"].is_null());
}

#[tokio::test]
async fn an_order_the_engine_refuses_never_reaches_the_venue() {
    let harness = start(filled("1", "60000.0")).await;
    let id = harness.open("100").await;

    let (status, reply) = harness.fill(id, "hyperliquid", "long", "1").await;
    assert_eq!(status, 409, "{reply}");
    assert!(harness.orders().is_empty());
}

#[tokio::test]
async fn an_order_the_venue_refuses_changes_nothing() {
    let refusal = json!({"status": "ok", "response": {"type": "order", "data": {"statuses": [
        {"error": "Order must have minimum value of $10."}
    ]}}});
    let harness = start(refusal).await;
    let id = harness.open("5000").await;

    let (status, reply) = harness.fill(id, "hyperliquid", "long", "0.0001").await;
    assert_eq!(status, 409, "{reply}");
    assert!(reply["error"].as_str().unwrap().contains("minimum value"));
    assert_eq!(harness.orders().len(), 1);

    let (_, account) = harness.get(&format!("/accounts/{id}")).await;
    assert_eq!(account["positions"], json!([]));
}

#[tokio::test]
async fn a_size_finer_than_the_venue_allows_is_refused_before_sending() {
    let harness = start(filled("0.000001", "60000.0")).await;
    let id = harness.open("5000").await;

    let (status, reply) = harness.fill(id, "hyperliquid", "long", "0.000001").await;
    assert_eq!(status, 400, "{reply}");
    assert!(harness.orders().is_empty());
}

#[tokio::test]
async fn fills_on_the_other_venue_stay_on_paper() {
    let harness = start(filled("0.01", "60100.0")).await;
    let id = harness.open("5000").await;

    let (status, account) = harness.fill(id, "okx", "short", "0.01").await;
    assert_eq!(status, 200, "{account}");
    assert_eq!(account["positions"][0]["entry_px"], "60000");
    assert!(harness.orders().is_empty());
}

#[tokio::test]
async fn reconciliation_compares_the_accounts_with_the_wallet() {
    let harness = start(filled("0.01", "60100.0")).await;
    let first = harness.open("5000").await;
    let second = harness.open("5000").await;
    harness.fill(first, "hyperliquid", "long", "0.01").await;
    harness.fill(second, "hyperliquid", "long", "0.01").await;

    // The wallet holds what both accounts hold together.
    harness.venue.lock().unwrap().position = "0.02";
    let (status, report) = harness.get("/venues/hyperliquid/reconciliation").await;
    assert_eq!(status, 200);
    assert_eq!(
        report,
        json!({
            "address": ADDRESS,
            "account_value": "110.0",
            "matched": true,
            "coins": [{"coin": "BTC", "engine": "0.02", "venue": "0.02", "difference": "0"}]
        })
    );

    // The wallet is short of what the accounts hold.
    harness.venue.lock().unwrap().position = "0.015";
    let (_, report) = harness.get("/venues/hyperliquid/reconciliation").await;
    assert_eq!(report["matched"], false);
    assert_eq!(report["coins"][0]["difference"], "-0.005");
}

#[tokio::test]
async fn a_liquidated_long_is_sold_on_the_venue() {
    let venue: Shared = Arc::new(Mutex::new(StandIn {
        orders: Vec::new(),
        order_reply: filled("0.5", "59900.0"),
        position: "0.0",
    }));
    let addr = listen(
        Router::new()
            .route("/info", post(info))
            .route("/exchange", post(exchange))
            .with_state(venue.clone()),
    )
    .await;
    let executor = Executor::connect(&format!("http://{addr}"), KEY)
        .await
        .unwrap();

    let (closes, rx) = mpsc::unbounded_channel();
    let task = tokio::spawn(execution::run_closes(Arc::new(executor), rx));
    closes
        .send(Close {
            coin: "BTC".into(),
            side: Side::Long,
            size: dec!(0.5),
            px: dec!(60000),
        })
        .unwrap();
    drop(closes);
    tokio::time::timeout(WAIT, task).await.unwrap().unwrap();

    // A sell, 10% below the price the engine closed at.
    let orders = venue.lock().unwrap().orders.clone();
    assert_eq!(orders.len(), 1);
    let order = &orders[0]["action"]["orders"][0];
    assert_eq!(order["b"], false);
    assert_eq!(order["p"], "54000");
    assert_eq!(order["s"], "0.5");
    assert_eq!(order["r"], false);
}
