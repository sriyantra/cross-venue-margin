//! Hyperliquid connector.
//!
//! Subscribes to the `l2` channel, which sends one full snapshot per market
//! followed by diffs. Diffs normally arrive base64 encoded and compressed
//! with raw DEFLATE. Also subscribes to `activeAssetCtx`, which carries the
//! venue's mark price and funding rate for one market.
//!
//! The module has two halves. `decode` is pure: text frame in, `Event` out.
//! `run` is the I/O loop: it owns the socket and the books, and it is the
//! only code that mutates them.

use std::collections::HashMap;
use std::io::Read;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use flate2::read::DeflateDecoder;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::{Decimal, dec};
use serde::Deserialize;
use serde_json::json;
use thiserror::Error;
use tokio::time::{Instant, MissedTickBehavior};
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use super::market::{MARK_CHECK_EVERY, Market};
use crate::book::{Desync, Level, SideDiff};
use crate::feed::Publisher;
use crate::metrics::Metrics;
use crate::risk::{TierTable, Venue};

const VENUE: Venue = Venue::Hyperliquid;

/// How often to send an application-level ping.
const PING_EVERY: Duration = Duration::from_secs(5);
/// If no frame of any kind arrives for this long, treat the connection as
/// dead even if the socket still looks open.
const STALE_AFTER: Duration = Duration::from_secs(10);
const BACKOFF_START: Duration = Duration::from_millis(500);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
/// Upper bound on one decompressed diff. Real diffs are a few kilobytes.
const MAX_INFLATED_BYTES: u64 = 1 << 20;

/// Maintenance margin table for a market, from the max-leverage brackets in
/// the venue's margin tiers documentation as read on 2026-11-22. Markets
/// that are not listed here have no table, and the engine refuses to open
/// positions in them.
pub fn maintenance_table(coin: &str) -> Option<TierTable> {
    let brackets: &[(Decimal, Decimal)] = match coin {
        "BTC" => &[(dec!(0), dec!(40)), (dec!(150_000_000), dec!(20))],
        "ETH" => &[(dec!(0), dec!(25)), (dec!(100_000_000), dec!(15))],
        _ => return None,
    };
    TierTable::from_max_leverage(brackets).ok()
}

// ---------------------------------------------------------------------------
// Wire format
// ---------------------------------------------------------------------------

/// Every frame is `{"channel": "...", "data": ...}`. `data` is absent on pong.
#[derive(Debug, Deserialize)]
struct Envelope {
    channel: String,
    #[serde(default)]
    data: serde_json::Value,
}

/// Payload of the `l2` channel: exactly one of the keys `s`, `u`, `c`.
#[derive(Debug, Deserialize)]
enum L2Payload {
    #[serde(rename = "s")]
    Snapshot(WireBook),
    #[serde(rename = "u")]
    Diff(WireDiff),
    #[serde(rename = "c")]
    Compressed(String),
}

#[derive(Debug, Deserialize)]
struct WireBook {
    coin: String,
    time: u64,
    /// `[bids, asks]`
    levels: [Vec<WireLevel>; 2],
}

#[derive(Debug, Deserialize)]
struct WireLevel {
    px: Decimal,
    sz: Decimal,
}

#[derive(Debug, Deserialize)]
struct WireDiff {
    c: String,
    t: u64,
    /// `[bids, asks]` levels added or changed.
    l: [Vec<WireUpdate>; 2],
    /// `[bids, asks]` indexes of removed levels in the previous book.
    r: [Vec<usize>; 2],
}

#[derive(Debug, Deserialize)]
struct WireUpdate {
    p: Decimal,
    s: Decimal,
}

/// Payload of the `activeAssetCtx` channel. The mark price and the funding
/// rate are read.
#[derive(Debug, Deserialize)]
struct WireAssetCtx {
    coin: String,
    ctx: WireCtx,
}

#[derive(Debug, Deserialize)]
struct WireCtx {
    #[serde(rename = "markPx")]
    mark_px: Decimal,
    #[serde(default)]
    funding: Option<Decimal>,
}

/// A decoded frame, in terms the rest of the program understands.
#[derive(Debug, PartialEq, Eq)]
pub enum Event {
    Snapshot {
        coin: String,
        time: u64,
        bids: Vec<Level>,
        asks: Vec<Level>,
    },
    Diff {
        coin: String,
        time: u64,
        bids: SideDiff,
        asks: SideDiff,
    },
    Mark {
        coin: String,
        mark: Decimal,
        /// The rate for the current hour, if the frame carried one.
        funding: Option<Decimal>,
    },
    Pong,
    Subscribed,
    /// The venue reported an error on the `error` channel.
    VenueError(String),
    /// A channel this connector does not use.
    Ignored(String),
}

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("frame is not valid JSON for its channel: {0}")]
    Json(#[from] serde_json::Error),
    #[error("compressed diff is not valid base64: {0}")]
    Base64(#[from] base64::DecodeError),
    #[error("compressed diff failed to inflate: {0}")]
    Inflate(#[from] std::io::Error),
    #[error("compressed diff inflates past {MAX_INFLATED_BYTES} bytes")]
    TooLarge,
}

/// Funding is paid every hour, on the hour.
const FUNDING_EVERY_MS: u64 = 3_600_000;

/// The first hour boundary after `now`, in milliseconds.
pub fn next_hour(now: u64) -> u64 {
    (now / FUNDING_EVERY_MS + 1) * FUNDING_EVERY_MS
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Decodes one text frame.
pub fn decode(text: &str) -> Result<Event, DecodeError> {
    let envelope: Envelope = serde_json::from_str(text)?;
    match envelope.channel.as_str() {
        "l2" => match serde_json::from_value(envelope.data)? {
            L2Payload::Snapshot(book) => Ok(snapshot_event(book)),
            L2Payload::Diff(diff) => Ok(diff_event(diff)),
            L2Payload::Compressed(encoded) => {
                let json = inflate(&encoded)?;
                Ok(diff_event(serde_json::from_slice(&json)?))
            }
        },
        "activeAssetCtx" => {
            let ctx: WireAssetCtx = serde_json::from_value(envelope.data)?;
            Ok(Event::Mark {
                coin: ctx.coin,
                mark: ctx.ctx.mark_px,
                funding: ctx.ctx.funding,
            })
        }
        "pong" => Ok(Event::Pong),
        "subscriptionResponse" => Ok(Event::Subscribed),
        "error" => Ok(Event::VenueError(envelope.data.to_string())),
        other => Ok(Event::Ignored(other.to_string())),
    }
}

/// base64, then raw DEFLATE (no zlib or gzip header).
fn inflate(encoded: &str) -> Result<Vec<u8>, DecodeError> {
    let compressed = BASE64.decode(encoded)?;
    let mut out = Vec::with_capacity(compressed.len() * 4);
    // `take` caps how much the decoder may produce, so a hostile or corrupt
    // frame cannot make the process allocate without limit.
    let mut limited = DeflateDecoder::new(compressed.as_slice()).take(MAX_INFLATED_BYTES + 1);
    limited.read_to_end(&mut out)?;
    if out.len() as u64 > MAX_INFLATED_BYTES {
        return Err(DecodeError::TooLarge);
    }
    Ok(out)
}

fn snapshot_event(book: WireBook) -> Event {
    let [bids, asks] = book.levels;
    let convert = |side: Vec<WireLevel>| {
        side.into_iter()
            .map(|l| Level { px: l.px, sz: l.sz })
            .collect()
    };
    Event::Snapshot {
        coin: book.coin,
        time: book.time,
        bids: convert(bids),
        asks: convert(asks),
    }
}

fn diff_event(diff: WireDiff) -> Event {
    let [bid_updates, ask_updates] = diff.l;
    let [bid_removed, ask_removed] = diff.r;
    let side = |removed: Vec<usize>, updates: Vec<WireUpdate>| SideDiff {
        removed,
        upserts: updates
            .into_iter()
            .map(|u| Level { px: u.p, sz: u.s })
            .collect(),
    };
    Event::Diff {
        coin: diff.c,
        time: diff.t,
        bids: side(bid_removed, bid_updates),
        asks: side(ask_removed, ask_updates),
    }
}

// ---------------------------------------------------------------------------
// Connection loop
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
enum SessionError {
    #[error("websocket error: {0}")]
    Ws(#[from] tungstenite::Error),
    #[error("server closed the connection")]
    Closed,
    #[error("no frames for {0:?}")]
    Stale(Duration),
}

/// Why a session ended without an error.
enum SessionEnd {
    Shutdown,
}

/// Runs the connector until `shutdown` is cancelled.
///
/// Reconnects forever with exponential backoff. Every reconnect starts from
/// empty books and no marks, and readers see `None` for each market until
/// fresh data arrives.
pub async fn run(
    url: String,
    publishers: HashMap<String, Publisher>,
    metrics: Metrics,
    shutdown: CancellationToken,
) {
    let mut markets: HashMap<String, Market> = publishers
        .into_iter()
        .map(|(coin, publisher)| (coin.clone(), Market::new(VENUE, coin, publisher)))
        .collect();

    let mut backoff = BACKOFF_START;
    loop {
        let mut got_snapshot = false;
        let outcome = session(&url, &mut markets, &metrics, &shutdown, &mut got_snapshot).await;

        for market in markets.values_mut() {
            market.invalidate();
            market.invalidate_mark();
        }

        match outcome {
            Ok(SessionEnd::Shutdown) => {
                info!(venue = %VENUE, "connector stopped");
                return;
            }
            Err(error) => warn!(venue = %VENUE, %error, "session ended"),
        }

        // A session that got as far as a snapshot was healthy, so the next
        // attempt starts from the short delay again.
        if got_snapshot {
            backoff = BACKOFF_START;
        }
        metrics.reconnect(VENUE);
        info!(venue = %VENUE, delay = ?backoff, "reconnecting");
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(BACKOFF_MAX);
    }
}

fn subscription(method: &str, coin: &str) -> Message {
    let body = json!({ "method": method, "subscription": { "type": "l2", "c": coin } });
    Message::text(body.to_string())
}

fn mark_subscription(coin: &str) -> Message {
    let body = json!({
        "method": "subscribe",
        "subscription": { "type": "activeAssetCtx", "coin": coin },
    });
    Message::text(body.to_string())
}

async fn session(
    url: &str,
    markets: &mut HashMap<String, Market>,
    metrics: &Metrics,
    shutdown: &CancellationToken,
    got_snapshot: &mut bool,
) -> Result<SessionEnd, SessionError> {
    let (mut ws, _response) = tokio::select! {
        _ = shutdown.cancelled() => return Ok(SessionEnd::Shutdown),
        connected = tokio_tungstenite::connect_async(url) => connected?,
    };
    info!(venue = %VENUE, url, "connected");

    for coin in markets.keys() {
        ws.send(subscription("subscribe", coin)).await?;
        ws.send(mark_subscription(coin)).await?;
    }

    let mut mark_check = tokio::time::interval(MARK_CHECK_EVERY);
    mark_check.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut ping = tokio::time::interval(PING_EVERY);
    ping.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut last_frame = Instant::now();

    loop {
        // `select!` waits on all branches at once and runs the first one that
        // is ready. The other branches are dropped, so each one must be safe
        // to abandon midway. `ws.next()`, `tick()`, `sleep_until()` and
        // `cancelled()` all are.
        tokio::select! {
            _ = shutdown.cancelled() => {
                let _ = ws.close(None).await;
                return Ok(SessionEnd::Shutdown);
            }
            _ = ping.tick() => {
                ws.send(Message::text(json!({ "method": "ping" }).to_string())).await?;
            }
            _ = mark_check.tick() => {
                let now = Instant::now();
                for (coin, market) in markets.iter_mut() {
                    if market.expire_mark(now) {
                        warn!(venue = %VENUE, coin, "mark went stale");
                    }
                }
            }
            _ = tokio::time::sleep_until(last_frame + STALE_AFTER) => {
                return Err(SessionError::Stale(STALE_AFTER));
            }
            frame = ws.next() => {
                let message = frame.ok_or(SessionError::Closed)??;
                last_frame = Instant::now();
                match message {
                    Message::Text(text) => {
                        let resubscribe =
                            handle_text(text.as_str(), last_frame, markets, got_snapshot);
                        for coin in resubscribe {
                            metrics.resync(VENUE);
                            ws.send(subscription("unsubscribe", &coin)).await?;
                            ws.send(subscription("subscribe", &coin)).await?;
                        }
                    }
                    Message::Close(_) => return Err(SessionError::Closed),
                    // Protocol-level ping and pong are answered by the library.
                    _ => {}
                }
            }
        }
    }
}

/// Applies one text frame. Returns the markets that need a resubscribe.
fn handle_text(
    text: &str,
    received: Instant,
    markets: &mut HashMap<String, Market>,
    got_snapshot: &mut bool,
) -> Vec<String> {
    let event = match decode(text) {
        Ok(event) => event,
        Err(error) => {
            // The coin is inside the payload that failed to decode, so there
            // is no way to know which book just missed an update. Rebuild all.
            warn!(venue = %VENUE, %error, "undecodable frame, resyncing every market");
            return markets
                .iter_mut()
                .filter_map(|(coin, market)| {
                    market.invalidate_mark();
                    resync(coin, market)
                })
                .collect();
        }
    };

    match event {
        Event::Snapshot {
            coin,
            time,
            bids,
            asks,
        } => {
            let Some(market) = markets.get_mut(&coin) else {
                return Vec::new();
            };
            match market.book.apply_snapshot(time, bids, asks) {
                Ok(()) => {
                    *got_snapshot = true;
                    market.publish();
                    debug!(venue = %VENUE, coin, "snapshot applied");
                    Vec::new()
                }
                Err(error) => {
                    warn!(venue = %VENUE, coin, %error, "snapshot rejected");
                    resync(&coin, market).into_iter().collect()
                }
            }
        }
        Event::Diff {
            coin,
            time,
            bids,
            asks,
        } => {
            let Some(market) = markets.get_mut(&coin) else {
                return Vec::new();
            };
            match market.book.apply_diff(time, &bids, &asks) {
                Ok(()) => {
                    market.publish();
                    Vec::new()
                }
                // Expected while a resubscribe is in flight: diffs from the
                // old subscription keep arriving until the new snapshot lands.
                Err(Desync::NoSnapshot) => Vec::new(),
                Err(error) => {
                    warn!(venue = %VENUE, coin, %error, "book desynced");
                    resync(&coin, market).into_iter().collect()
                }
            }
        }
        Event::Mark {
            coin,
            mark,
            funding,
        } => {
            if let Some(market) = markets.get_mut(&coin) {
                market.set_mark(mark, received);
                if let Some(rate) = funding {
                    market.set_funding(rate, next_hour(now_ms()));
                }
            }
            Vec::new()
        }
        Event::VenueError(message) => {
            warn!(venue = %VENUE, message, "venue reported an error");
            Vec::new()
        }
        Event::Pong | Event::Subscribed | Event::Ignored(_) => Vec::new(),
    }
}

/// Clears one market and names it if a resubscribe should be sent now.
fn resync(coin: &str, market: &mut Market) -> Option<String> {
    market.resync().then(|| coin.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn documented_markets_have_a_maintenance_table() {
        assert_eq!(
            maintenance_table("BTC").unwrap().maintenance(dec!(60_000)),
            Ok(dec!(750))
        );
        assert_eq!(
            maintenance_table("ETH").unwrap().maintenance(dec!(50_000)),
            Ok(dec!(1_000))
        );
        assert!(maintenance_table("DOGE").is_none());
    }

    #[test]
    fn decodes_snapshot() {
        let frame = r#"{"channel":"l2","data":{"s":{"coin":"BTC","time":5,
            "levels":[[{"px":"99.5","sz":"1","n":2}],[{"px":"100.5","sz":"3","n":1}]]}}}"#;
        assert_eq!(
            decode(frame).unwrap(),
            Event::Snapshot {
                coin: "BTC".into(),
                time: 5,
                bids: vec![Level {
                    px: dec!(99.5),
                    sz: dec!(1)
                }],
                asks: vec![Level {
                    px: dec!(100.5),
                    sz: dec!(3)
                }],
            }
        );
    }

    #[test]
    fn decodes_uncompressed_diff() {
        let frame = r#"{"channel":"l2","data":{"u":{"c":"BTC","t":6,
            "l":[[{"p":"99","s":"2"}],[]],"r":[[0],[1,2]]}}}"#;
        assert_eq!(
            decode(frame).unwrap(),
            Event::Diff {
                coin: "BTC".into(),
                time: 6,
                bids: SideDiff {
                    removed: vec![0],
                    upserts: vec![Level {
                        px: dec!(99),
                        sz: dec!(2)
                    }]
                },
                asks: SideDiff {
                    removed: vec![1, 2],
                    upserts: vec![]
                },
            }
        );
    }

    #[test]
    fn decodes_compressed_diff() {
        use flate2::{Compression, write::DeflateEncoder};
        use std::io::Write;

        let inner = br#"{"c":"ETH","t":7,"l":[[],[{"p":"10","s":"1"}]],"r":[[],[]]}"#;
        let mut encoder = DeflateEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(inner).unwrap();
        let encoded = BASE64.encode(encoder.finish().unwrap());
        let frame = format!(r#"{{"channel":"l2","data":{{"c":"{encoded}"}}}}"#);

        match decode(&frame).unwrap() {
            Event::Diff {
                coin, time, asks, ..
            } => {
                assert_eq!((coin.as_str(), time), ("ETH", 7));
                assert_eq!(
                    asks.upserts,
                    vec![Level {
                        px: dec!(10),
                        sz: dec!(1)
                    }]
                );
            }
            other => panic!("expected a diff, got {other:?}"),
        }
    }

    #[test]
    fn decodes_mark() {
        // Shape recorded from the live feed on 2026-11-22.
        let frame = r#"{"channel":"activeAssetCtx","data":{"coin":"BTC","ctx":{
            "funding":"0.0000020754","openInterest":"39525.79214","prevDayPx":"60252.0",
            "dayNtlVlm":"3172722573.4906549454","premium":"-0.0004801052",
            "oraclePx":"60820.0","markPx":"60787.0","midPx":"60784.5",
            "impactPxs":["60784.0","60790.8"],"dayBaseVlm":"52784.00396"}}}"#;
        assert_eq!(
            decode(frame).unwrap(),
            Event::Mark {
                coin: "BTC".into(),
                mark: dec!(60787.0),
                funding: Some(dec!(0.0000020754)),
            }
        );
    }

    #[test]
    fn funding_settles_on_the_next_hour() {
        assert_eq!(next_hour(0), 3_600_000);
        assert_eq!(next_hour(3_599_999), 3_600_000);
        // On the boundary the hour that just ended has settled.
        assert_eq!(next_hour(3_600_000), 7_200_000);
    }

    #[test]
    fn decodes_control_frames() {
        assert_eq!(decode(r#"{"channel":"pong"}"#).unwrap(), Event::Pong);
        assert_eq!(
            decode(r#"{"channel":"subscriptionResponse","data":{"method":"subscribe"}}"#).unwrap(),
            Event::Subscribed
        );
        assert_eq!(
            decode(r#"{"channel":"trades","data":[]}"#).unwrap(),
            Event::Ignored("trades".into())
        );
    }

    #[test]
    fn rejects_garbage() {
        assert!(matches!(decode("not json"), Err(DecodeError::Json(_))));
        assert!(matches!(
            decode(r#"{"channel":"l2","data":{"c":"***"}}"#),
            Err(DecodeError::Base64(_))
        ));
        assert!(matches!(
            decode(r#"{"channel":"l2","data":{"c":"AAAA"}}"#),
            Err(DecodeError::Inflate(_) | DecodeError::Json(_))
        ));
    }
}
