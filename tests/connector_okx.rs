//! Runs the OKX connector against a local fake venue to exercise the paths
//! a healthy live feed never takes: a sequence gap and a reconnect.

use std::time::Duration;

use cross_venue_margin::feed::{BookRx, Feeds, MarkRx};
use cross_venue_margin::metrics::Metrics;
use cross_venue_margin::risk::{MarketId, Venue};
use cross_venue_margin::venue::okx;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::dec;
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

type FakeVenue = WebSocketStream<TcpStream>;

const WAIT: Duration = Duration::from_secs(5);
const INST: &str = "BTC-USDT-SWAP";

async fn accept(listener: &TcpListener) -> FakeVenue {
    let (stream, _) = tokio::time::timeout(WAIT, listener.accept())
        .await
        .unwrap()
        .unwrap();
    tokio_tungstenite::accept_async(stream).await.unwrap()
}

/// Reads client frames until one is a request with the given `op`,
/// skipping pings, and returns its `args`.
async fn expect_op(venue: &mut FakeVenue, op: &str) -> Value {
    loop {
        let frame = tokio::time::timeout(WAIT, venue.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let Message::Text(text) = frame else { continue };
        if text.as_str() == "ping" {
            continue;
        }
        let value: Value = serde_json::from_str(text.as_str()).unwrap();
        assert_eq!(value["op"], op, "unexpected client frame: {value}");
        return value["args"].clone();
    }
}

fn book(action: &str, ts: u64, seq: i64, prev_seq: i64, bids: Value, asks: Value) -> Message {
    let body = json!({
        "arg": { "channel": "books", "instId": INST },
        "action": action,
        "data": [{
            "asks": asks, "bids": bids, "ts": ts.to_string(),
            "checksum": 0, "seqId": seq, "prevSeqId": prev_seq,
        }],
    });
    Message::text(body.to_string())
}

fn snapshot(ts: u64, seq: i64) -> Message {
    book(
        "snapshot",
        ts,
        seq,
        -1,
        json!([["99", "250", "0", "3"]]),
        json!([["101", "100", "0", "1"]]),
    )
}

fn mark(px: &str) -> Message {
    let body = json!({
        "arg": { "channel": "mark-price", "instId": INST },
        "data": [{ "instId": INST, "instType": "SWAP", "markPx": px, "ts": "1" }],
    });
    Message::text(body.to_string())
}

async fn wait_for(rx: &mut BookRx, want: impl Fn(Option<u64>) -> bool) -> Option<u64> {
    let slot = tokio::time::timeout(
        WAIT,
        rx.wait_for(|slot| want(slot.as_ref().map(|v| v.time))),
    )
    .await
    .expect("timed out waiting for book state")
    .unwrap();
    slot.as_ref().map(|v| v.time)
}

async fn wait_for_mark(rx: &mut MarkRx, want: Option<&str>) {
    let want = want.map(|px| px.parse().unwrap());
    tokio::time::timeout(
        WAIT,
        rx.wait_for(|slot| slot.as_ref().map(|view| view.mark) == want),
    )
    .await
    .expect("timed out waiting for mark state")
    .unwrap();
}

async fn start() -> (TcpListener, BookRx, MarkRx, CancellationToken) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let btc = MarketId::new(Venue::Okx, "BTC");
    let (feeds, mut publishers) = Feeds::channels(std::slice::from_ref(&btc));
    let shutdown = CancellationToken::new();
    tokio::spawn(okx::run(
        url,
        publishers.take(Venue::Okx),
        Metrics::new(),
        shutdown.clone(),
    ));
    (
        listener,
        feeds.subscribe(&btc).unwrap(),
        feeds.subscribe_mark(&btc).unwrap(),
        shutdown,
    )
}

async fn expect_subscriptions(venue: &mut FakeVenue) {
    assert_eq!(
        expect_op(venue, "subscribe").await,
        json!([
            { "channel": "books", "instId": INST },
            { "channel": "mark-price", "instId": INST },
        ])
    );
}

#[tokio::test]
async fn book_is_published_in_coin_units() {
    let (listener, mut rx, _marks, shutdown) = start().await;
    let mut venue = accept(&listener).await;
    expect_subscriptions(&mut venue).await;

    venue.send(snapshot(100, 10)).await.unwrap();
    wait_for(&mut rx, |t| t == Some(100)).await;
    venue
        .send(book(
            "update",
            101,
            11,
            10,
            json!([["98", "50", "0", "1"]]),
            json!([]),
        ))
        .await
        .unwrap();
    wait_for(&mut rx, |t| t == Some(101)).await;

    let view = rx.borrow().clone().unwrap();
    assert_eq!(view.venue, Venue::Okx);
    assert_eq!(view.coin, "BTC");
    // 250 and 50 contracts of 0.01 BTC.
    assert_eq!(view.bids[0].sz, dec!(2.5));
    assert_eq!(view.bids[1].sz, dec!(0.5));
    assert_eq!(view.asks[0].sz, dec!(1));

    shutdown.cancel();
}

#[tokio::test]
async fn sequence_gap_marks_book_stale_then_resubscribes() {
    let (listener, mut rx, mut marks, shutdown) = start().await;
    let mut venue = accept(&listener).await;
    expect_subscriptions(&mut venue).await;

    venue.send(mark("60000.5")).await.unwrap();
    wait_for_mark(&mut marks, Some("60000.5")).await;
    venue.send(snapshot(100, 10)).await.unwrap();
    wait_for(&mut rx, |t| t == Some(100)).await;

    // This update says the message before it was 11. The connector saw 10.
    venue
        .send(book("update", 101, 12, 11, json!([]), json!([])))
        .await
        .unwrap();
    assert_eq!(wait_for(&mut rx, |t| t.is_none()).await, None);
    // A book gap says nothing about the mark.
    assert!(marks.borrow().is_some());

    let books_only = json!([{ "channel": "books", "instId": INST }]);
    assert_eq!(expect_op(&mut venue, "unsubscribe").await, books_only);
    assert_eq!(expect_op(&mut venue, "subscribe").await, books_only);

    // An update from the old subscription arrives before the new snapshot.
    // It must be dropped without publishing anything.
    venue
        .send(book("update", 102, 13, 12, json!([]), json!([])))
        .await
        .unwrap();
    venue.send(snapshot(103, 20)).await.unwrap();
    assert_eq!(wait_for(&mut rx, |t| t.is_some()).await, Some(103));

    shutdown.cancel();
}

#[tokio::test]
async fn dropped_connection_withdraws_book_and_mark_then_reconnects() {
    let (listener, mut rx, mut marks, shutdown) = start().await;

    let mut first = accept(&listener).await;
    expect_subscriptions(&mut first).await;
    first.send(snapshot(100, 10)).await.unwrap();
    first.send(mark("60000.5")).await.unwrap();
    wait_for(&mut rx, |t| t == Some(100)).await;
    wait_for_mark(&mut marks, Some("60000.5")).await;

    drop(first);
    assert_eq!(wait_for(&mut rx, |t| t.is_none()).await, None);
    wait_for_mark(&mut marks, None).await;

    // Sequence numbers start over on a new connection.
    let mut second = accept(&listener).await;
    expect_subscriptions(&mut second).await;
    second.send(snapshot(200, 3)).await.unwrap();
    second
        .send(book("update", 201, 4, 3, json!([]), json!([])))
        .await
        .unwrap();
    assert_eq!(wait_for(&mut rx, |t| t == Some(201)).await, Some(201));

    shutdown.cancel();
}
