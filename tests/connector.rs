//! Runs the Hyperliquid connector against a local fake venue to exercise the
//! paths a healthy live feed never takes: desync recovery and reconnect.

use std::time::Duration;

use cross_venue_margin::feed::{BookRx, Feeds, MarkRx};
use cross_venue_margin::risk::{MarketId, Venue};
use cross_venue_margin::venue::hyperliquid;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

type FakeVenue = WebSocketStream<TcpStream>;

const WAIT: Duration = Duration::from_secs(5);

async fn accept(listener: &TcpListener) -> FakeVenue {
    let (stream, _) = tokio::time::timeout(WAIT, listener.accept())
        .await
        .unwrap()
        .unwrap();
    tokio_tungstenite::accept_async(stream).await.unwrap()
}

/// Reads client frames until one has the given `method`, skipping pings.
async fn expect_method(venue: &mut FakeVenue, method: &str) -> Value {
    loop {
        let frame = tokio::time::timeout(WAIT, venue.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let Message::Text(text) = frame else { continue };
        let value: Value = serde_json::from_str(text.as_str()).unwrap();
        if value["method"] == "ping" {
            continue;
        }
        assert_eq!(value["method"], method, "unexpected client frame: {value}");
        return value;
    }
}

fn snapshot(time: u64, bid: &str, ask: &str) -> Message {
    let body = json!({ "channel": "l2", "data": { "s": {
        "coin": "BTC", "time": time,
        "levels": [[{ "px": bid, "sz": "1", "n": 1 }], [{ "px": ask, "sz": "1", "n": 1 }]],
    }}});
    Message::text(body.to_string())
}

fn diff(time: u64, removed_bids: &[usize]) -> Message {
    let body = json!({ "channel": "l2", "data": { "u": {
        "c": "BTC", "t": time, "l": [[], []], "r": [removed_bids, []],
    }}});
    Message::text(body.to_string())
}

/// Waits until the published book satisfies `want` and returns its time.
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

fn mark(px: &str) -> Message {
    let body = json!({ "channel": "activeAssetCtx", "data": {
        "coin": "BTC", "ctx": { "markPx": px, "oraclePx": px, "funding": "0" },
    }});
    Message::text(body.to_string())
}

/// Reads the two subscriptions the connector sends for a market on connect.
async fn expect_subscriptions(venue: &mut FakeVenue) {
    let book = expect_method(venue, "subscribe").await;
    assert_eq!(book["subscription"], json!({ "type": "l2", "c": "BTC" }));
    let ctx = expect_method(venue, "subscribe").await;
    assert_eq!(
        ctx["subscription"],
        json!({ "type": "activeAssetCtx", "coin": "BTC" })
    );
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

async fn start_with_marks() -> (TcpListener, BookRx, MarkRx, CancellationToken) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let btc = MarketId::new(Venue::Hyperliquid, "BTC");
    let (feeds, mut publishers) = Feeds::channels(std::slice::from_ref(&btc));
    let shutdown = CancellationToken::new();
    tokio::spawn(hyperliquid::run(
        url,
        publishers.take(Venue::Hyperliquid),
        shutdown.clone(),
    ));
    (
        listener,
        feeds.subscribe(&btc).unwrap(),
        feeds.subscribe_mark(&btc).unwrap(),
        shutdown,
    )
}

async fn start() -> (TcpListener, BookRx, CancellationToken) {
    let (listener, book, _mark, shutdown) = start_with_marks().await;
    (listener, book, shutdown)
}

#[tokio::test]
async fn bad_diff_marks_book_stale_then_resubscribes() {
    let (listener, mut rx, shutdown) = start().await;
    let mut venue = accept(&listener).await;

    expect_subscriptions(&mut venue).await;

    venue.send(snapshot(100, "99", "101")).await.unwrap();
    assert_eq!(wait_for(&mut rx, |t| t == Some(100)).await, Some(100));

    // The book has one bid, so removing index 5 cannot be applied.
    venue.send(diff(101, &[5])).await.unwrap();
    assert_eq!(wait_for(&mut rx, |t| t.is_none()).await, None);

    expect_method(&mut venue, "unsubscribe").await;
    expect_method(&mut venue, "subscribe").await;

    // A diff from the old subscription arrives before the new snapshot. It
    // must be dropped without publishing anything.
    venue.send(diff(102, &[])).await.unwrap();
    venue.send(snapshot(103, "98", "102")).await.unwrap();
    assert_eq!(wait_for(&mut rx, |t| t.is_some()).await, Some(103));

    shutdown.cancel();
}

#[tokio::test]
async fn dropped_connection_marks_book_stale_then_reconnects() {
    let (listener, mut rx, shutdown) = start().await;

    let mut first = accept(&listener).await;
    expect_subscriptions(&mut first).await;
    first.send(snapshot(100, "99", "101")).await.unwrap();
    wait_for(&mut rx, |t| t == Some(100)).await;

    drop(first);
    assert_eq!(wait_for(&mut rx, |t| t.is_none()).await, None);

    let mut second = accept(&listener).await;
    expect_subscriptions(&mut second).await;
    second.send(snapshot(200, "99", "101")).await.unwrap();
    assert_eq!(wait_for(&mut rx, |t| t == Some(200)).await, Some(200));

    shutdown.cancel();
}

#[tokio::test]
async fn mark_is_published_and_withdrawn_on_disconnect() {
    let (listener, mut book, mut marks, shutdown) = start_with_marks().await;

    let mut first = accept(&listener).await;
    expect_subscriptions(&mut first).await;
    first.send(mark("60787.0")).await.unwrap();
    wait_for_mark(&mut marks, Some("60787.0")).await;

    // A book desync rebuilds the book and leaves the mark alone.
    first.send(snapshot(100, "99", "101")).await.unwrap();
    wait_for(&mut book, |t| t == Some(100)).await;
    first.send(diff(101, &[5])).await.unwrap();
    wait_for(&mut book, |t| t.is_none()).await;
    assert!(marks.borrow().is_some());

    drop(first);
    wait_for_mark(&mut marks, None).await;

    let mut second = accept(&listener).await;
    expect_subscriptions(&mut second).await;
    second.send(mark("60800.5")).await.unwrap();
    wait_for_mark(&mut marks, Some("60800.5")).await;

    shutdown.cancel();
}
