//! Runs the API router on a local port and checks the read-only switch.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use cross_venue_margin::engine::task::{Command, EngineHandle};
use cross_venue_margin::feed::Feeds;
use cross_venue_margin::metrics::Metrics;
use cross_venue_margin::risk::{MarketId, Venue};
use cross_venue_margin::server;
use tokio::sync::mpsc;

const WAIT: Duration = Duration::from_secs(5);

async fn start(read_only: bool) -> (SocketAddr, mpsc::Receiver<Command>) {
    let (feeds, _publishers) = Feeds::channels(&[MarketId::new(Venue::Hyperliquid, "BTC")]);
    let (command_tx, command_rx) = mpsc::channel::<Command>(8);
    let router = server::router(
        feeds,
        EngineHandle::new(command_tx),
        Metrics::new(),
        read_only,
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (addr, command_rx)
}

/// Sends one request and returns the status code of the reply.
async fn status(
    addr: SocketAddr,
    method: &'static str,
    path: &'static str,
    body: &'static str,
) -> u16 {
    tokio::task::spawn_blocking(move || {
        let mut stream = TcpStream::connect(addr).unwrap();
        stream.set_read_timeout(Some(WAIT)).unwrap();
        write!(
            stream,
            "{method} {path} HTTP/1.1\r\nhost: test\r\nconnection: close\r\n\
             content-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
            body.len()
        )
        .unwrap();
        let mut reply = String::new();
        stream.read_to_string(&mut reply).unwrap();
        reply.split(' ').nth(1).unwrap().parse().unwrap()
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn read_only_refuses_writes_before_the_engine() {
    let (addr, mut commands) = start(true).await;

    assert_eq!(
        status(addr, "POST", "/accounts", r#"{"collateral":"5000"}"#).await,
        403
    );
    let fill = r#"{"venue":"hyperliquid","coin":"BTC","side":"long","size":"1"}"#;
    assert_eq!(status(addr, "POST", "/accounts/1/fills", fill).await, 403);
    let deposit = r#"{"asset":"BTC","amount":"1"}"#;
    assert_eq!(
        status(addr, "POST", "/accounts/1/deposits", deposit).await,
        403
    );

    assert!(
        commands.try_recv().is_err(),
        "a refused write reached the engine"
    );
}

#[tokio::test]
async fn read_only_still_serves_reads() {
    let (addr, _commands) = start(true).await;

    assert_eq!(status(addr, "GET", "/metrics", "").await, 200);
    assert_eq!(status(addr, "GET", "/health", "").await, 503);
}

#[tokio::test]
async fn writes_reach_the_engine_by_default() {
    let (addr, mut commands) = start(false).await;

    let request = tokio::spawn(status(
        addr,
        "POST",
        "/accounts",
        r#"{"collateral":"5000"}"#,
    ));
    let command = tokio::time::timeout(WAIT, commands.recv()).await.unwrap();
    assert!(command.is_some());
    drop(command);
    drop(commands);
    assert_eq!(request.await.unwrap(), 503);
}
