//! OKX connector for USDT-margined perpetual swaps.
//!
//! Subscribes to the `books` channel, which sends one 400-level snapshot per
//! instrument followed by updates keyed by price, and to `mark-price` and
//! `funding-rate`.
//! Every book message carries `seqId` and `prevSeqId`. An update is only
//! applied if its `prevSeqId` is the `seqId` of the message before it, so a
//! dropped message is detected instead of silently corrupting the book.
//!
//! OKX quotes these books in contracts. This module converts sizes to units
//! of the coin, so a book from OKX and a book from another venue can be
//! compared directly.
//!
//! Like the other connectors, the module has two halves. `decode` is pure:
//! text frame in, `Event` out. `run` is the I/O loop that owns the socket
//! and the books.

use std::collections::HashMap;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use rust_decimal::{Decimal, dec};
use serde::Deserialize;
use serde::de::IgnoredAny;
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
use crate::risk::{SizeTable, Venue};

const VENUE: Venue = Venue::Okx;

/// The venue's taker fee for perpetuals at its first fee level, from its
/// help center (read on 2026-11-26).
pub const TAKER_FEE: Decimal = dec!(0.0005);

/// How often to send the text `ping` the venue answers with `pong`.
const PING_EVERY: Duration = Duration::from_secs(5);
/// If no frame of any kind arrives for this long, treat the connection as
/// dead even if the socket still looks open.
const STALE_AFTER: Duration = Duration::from_secs(10);
const BACKOFF_START: Duration = Duration::from_millis(500);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

/// How a coin trades on this venue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Instrument {
    pub inst_id: String,
    /// Units of the coin in one contract.
    pub contract_value: Decimal,
}

/// The USDT-margined perpetual for a coin, with the contract value the
/// venue's public instruments endpoint reported on 2026-11-22. Coins that
/// are not listed here are not tracked.
pub fn instrument(coin: &str) -> Option<Instrument> {
    let contract_value = match coin {
        "BTC" => dec!(0.01),
        "ETH" => dec!(0.1),
        _ => return None,
    };
    Some(Instrument {
        inst_id: format!("{coin}-USDT-SWAP"),
        contract_value,
    })
}

/// Margin brackets for a coin's USDT perpetual in cross margin, from the
/// venue's public position tiers endpoint. Maintenance rates as read on
/// 2026-11-22, initial rates as read on 2026-11-25. The initial rate starts
/// at 1% (100x) and rises by half a percent per bracket.
/// The venue lists them in contracts, and they are stored here in units of
/// the coin. Only the first 12 of 99 tiers are kept, which covers positions
/// up to 2,000 BTC and 25,000 ETH. Larger positions are refused.
pub fn maintenance_table(coin: &str) -> Option<SizeTable> {
    const RATES: [Decimal; 12] = [
        dec!(0.004),
        dec!(0.005),
        dec!(0.0075),
        dec!(0.0125),
        dec!(0.0175),
        dec!(0.0225),
        dec!(0.0275),
        dec!(0.0325),
        dec!(0.0375),
        dec!(0.0425),
        dec!(0.0475),
        dec!(0.0525),
    ];
    let max_contracts: [u32; 12] = match coin {
        "BTC" => [
            1_000, 5_000, 20_000, 40_000, 60_000, 80_000, 100_000, 120_000, 140_000, 160_000,
            180_000, 200_000,
        ],
        "ETH" => [
            5_000, 10_000, 25_000, 50_000, 75_000, 100_000, 125_000, 150_000, 175_000, 200_000,
            225_000, 250_000,
        ],
        _ => return None,
    };
    let contract_value = instrument(coin)?.contract_value;
    let brackets: Vec<(Decimal, Decimal, Decimal)> = max_contracts
        .iter()
        .zip(RATES)
        .enumerate()
        .map(|(i, (contracts, rate))| {
            let initial_rate = dec!(0.01) + dec!(0.005) * Decimal::from(i as u32);
            (
                Decimal::from(*contracts) * contract_value,
                rate,
                initial_rate,
            )
        })
        .collect();
    SizeTable::with_initial_rates(&brackets).ok()
}

// ---------------------------------------------------------------------------
// Wire format
// ---------------------------------------------------------------------------

/// Data frames have `arg` and `data`. Control frames have `event`.
#[derive(Debug, Deserialize)]
struct Frame {
    event: Option<String>,
    arg: Option<Arg>,
    action: Option<String>,
    #[serde(default)]
    data: serde_json::Value,
    code: Option<String>,
    msg: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Arg {
    channel: String,
    #[serde(rename = "instId", default)]
    inst_id: String,
}

#[derive(Debug, Deserialize)]
struct WireBook {
    asks: Vec<WireLevel>,
    bids: Vec<WireLevel>,
    /// Milliseconds, sent as a string.
    ts: String,
    #[serde(rename = "seqId")]
    seq_id: i64,
    #[serde(rename = "prevSeqId")]
    prev_seq_id: i64,
}

/// `[price, size in contracts, deprecated, order count]`
#[derive(Debug, Deserialize)]
struct WireLevel(Decimal, Decimal, IgnoredAny, IgnoredAny);

#[derive(Debug, Deserialize)]
struct WireMark {
    #[serde(rename = "markPx")]
    mark_px: Decimal,
}

#[derive(Debug, Deserialize)]
struct WireFunding {
    #[serde(rename = "fundingRate")]
    funding_rate: Decimal,
    /// When this rate is charged, in milliseconds, as a string.
    #[serde(rename = "fundingTime")]
    funding_time: String,
}

/// A decoded frame. Book sizes are still in contracts here.
#[derive(Debug, PartialEq, Eq)]
pub enum Event {
    Snapshot {
        inst_id: String,
        time: u64,
        seq: i64,
        bids: Vec<Level>,
        asks: Vec<Level>,
    },
    Update {
        inst_id: String,
        time: u64,
        seq: i64,
        prev_seq: i64,
        bids: Vec<Level>,
        asks: Vec<Level>,
    },
    Mark {
        inst_id: String,
        mark: Decimal,
    },
    Funding {
        inst_id: String,
        rate: Decimal,
        settles_at: u64,
    },
    Pong,
    Subscribed,
    /// The venue reported an error.
    VenueError(String),
    /// A frame this connector does not use.
    Ignored(String),
}

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("frame is not valid JSON for its channel: {0}")]
    Json(#[from] serde_json::Error),
    #[error("timestamp {0:?} is not a number")]
    Timestamp(String),
    #[error("{0} frame has no data")]
    Empty(&'static str),
    #[error("book frame has unknown action {0:?}")]
    Action(String),
}

/// Decodes one text frame.
pub fn decode(text: &str) -> Result<Event, DecodeError> {
    if text == "pong" {
        return Ok(Event::Pong);
    }
    let frame: Frame = serde_json::from_str(text)?;

    if let Some(event) = frame.event {
        return Ok(match event.as_str() {
            "subscribe" | "unsubscribe" => Event::Subscribed,
            "error" => Event::VenueError(format!(
                "{}: {}",
                frame.code.unwrap_or_default(),
                frame.msg.unwrap_or_default()
            )),
            _ => Event::Ignored(event),
        });
    }
    let Some(arg) = frame.arg else {
        return Ok(Event::Ignored("frame without arg".into()));
    };

    match arg.channel.as_str() {
        "books" => {
            let [book]: [WireBook; 1] =
                serde_json::from_value(frame.data).map_err(|_| DecodeError::Empty("books"))?;
            let time = book
                .ts
                .parse()
                .map_err(|_| DecodeError::Timestamp(book.ts.clone()))?;
            let levels = |side: Vec<WireLevel>| {
                side.into_iter()
                    .map(|WireLevel(px, sz, ..)| Level { px, sz })
                    .collect()
            };
            match frame.action.as_deref() {
                Some("snapshot") => Ok(Event::Snapshot {
                    inst_id: arg.inst_id,
                    time,
                    seq: book.seq_id,
                    bids: levels(book.bids),
                    asks: levels(book.asks),
                }),
                Some("update") => Ok(Event::Update {
                    inst_id: arg.inst_id,
                    time,
                    seq: book.seq_id,
                    prev_seq: book.prev_seq_id,
                    bids: levels(book.bids),
                    asks: levels(book.asks),
                }),
                other => Err(DecodeError::Action(other.unwrap_or_default().to_string())),
            }
        }
        "mark-price" => {
            let [mark]: [WireMark; 1] =
                serde_json::from_value(frame.data).map_err(|_| DecodeError::Empty("mark-price"))?;
            Ok(Event::Mark {
                inst_id: arg.inst_id,
                mark: mark.mark_px,
            })
        }
        "funding-rate" => {
            let [funding]: [WireFunding; 1] = serde_json::from_value(frame.data)
                .map_err(|_| DecodeError::Empty("funding-rate"))?;
            let settles_at = funding
                .funding_time
                .parse()
                .map_err(|_| DecodeError::Timestamp(funding.funding_time.clone()))?;
            Ok(Event::Funding {
                inst_id: arg.inst_id,
                rate: funding.funding_rate,
                settles_at,
            })
        }
        other => Ok(Event::Ignored(other.to_string())),
    }
}

/// Converts level sizes from contracts to units of the coin. A size of
/// zero stays zero, which is how an update removes a level.
pub fn in_coin(levels: Vec<Level>, contract_value: Decimal) -> Vec<Level> {
    levels
        .into_iter()
        .map(|level| Level {
            px: level.px,
            sz: level.sz * contract_value,
        })
        .collect()
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

/// One instrument's state inside the connector task.
struct Tracked {
    market: Market,
    instrument: Instrument,
    /// `seqId` of the last book message applied. `None` while waiting for a
    /// snapshot.
    last_seq: Option<i64>,
}

impl Tracked {
    /// Clears the book and reports whether to resubscribe now.
    fn resync(&mut self) -> bool {
        self.last_seq = None;
        self.market.resync()
    }
}

/// Runs the connector until `shutdown` is cancelled.
///
/// `publishers` is keyed by coin. A coin with no known instrument is
/// dropped with a warning. Reconnects forever with exponential backoff, and
/// every reconnect starts from empty books and no marks.
pub async fn run(
    url: String,
    publishers: HashMap<String, Publisher>,
    metrics: Metrics,
    shutdown: CancellationToken,
) {
    let mut tracked: HashMap<String, Tracked> = HashMap::new();
    for (coin, publisher) in publishers {
        let Some(instrument) = instrument(&coin) else {
            warn!(venue = %VENUE, coin, "no instrument for this coin, not tracking it");
            continue;
        };
        let state = Tracked {
            market: Market::new(VENUE, coin, publisher),
            instrument: instrument.clone(),
            last_seq: None,
        };
        tracked.insert(instrument.inst_id, state);
    }

    let mut backoff = BACKOFF_START;
    loop {
        let mut got_snapshot = false;
        let outcome = session(&url, &mut tracked, &metrics, &shutdown, &mut got_snapshot).await;

        for state in tracked.values_mut() {
            state.last_seq = None;
            state.market.invalidate();
            state.market.invalidate_mark();
        }

        match outcome {
            Ok(SessionEnd::Shutdown) => {
                info!(venue = %VENUE, "connector stopped");
                return;
            }
            Err(error) => warn!(venue = %VENUE, %error, "session ended"),
        }

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

fn request(op: &str, channels: &[&str], inst_ids: &[&str]) -> Message {
    let args: Vec<_> = inst_ids
        .iter()
        .flat_map(|inst_id| {
            channels
                .iter()
                .map(move |channel| json!({ "channel": channel, "instId": inst_id }))
        })
        .collect();
    Message::text(json!({ "op": op, "args": args }).to_string())
}

async fn session(
    url: &str,
    tracked: &mut HashMap<String, Tracked>,
    metrics: &Metrics,
    shutdown: &CancellationToken,
    got_snapshot: &mut bool,
) -> Result<SessionEnd, SessionError> {
    let (mut ws, _response) = tokio::select! {
        _ = shutdown.cancelled() => return Ok(SessionEnd::Shutdown),
        connected = tokio_tungstenite::connect_async(url) => connected?,
    };
    info!(venue = %VENUE, url, "connected");

    let inst_ids: Vec<&str> = tracked.keys().map(String::as_str).collect();
    if !inst_ids.is_empty() {
        ws.send(request(
            "subscribe",
            &["books", "mark-price", "funding-rate"],
            &inst_ids,
        ))
        .await?;
    }

    let mut mark_check = tokio::time::interval(MARK_CHECK_EVERY);
    mark_check.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut ping = tokio::time::interval(PING_EVERY);
    ping.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut last_frame = Instant::now();

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                let _ = ws.close(None).await;
                return Ok(SessionEnd::Shutdown);
            }
            _ = ping.tick() => {
                ws.send(Message::text("ping")).await?;
            }
            _ = mark_check.tick() => {
                let now = Instant::now();
                for (inst_id, state) in tracked.iter_mut() {
                    if state.market.expire_mark(now) {
                        warn!(venue = %VENUE, inst_id, "mark went stale");
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
                            handle_text(text.as_str(), last_frame, tracked, got_snapshot);
                        for inst_id in resubscribe {
                            metrics.resync(VENUE);
                            ws.send(request("unsubscribe", &["books"], &[&inst_id])).await?;
                            ws.send(request("subscribe", &["books"], &[&inst_id])).await?;
                        }
                    }
                    Message::Close(_) => return Err(SessionError::Closed),
                    _ => {}
                }
            }
        }
    }
}

/// Applies one text frame. Returns the instruments whose book needs a
/// resubscribe.
fn handle_text(
    text: &str,
    received: Instant,
    tracked: &mut HashMap<String, Tracked>,
    got_snapshot: &mut bool,
) -> Vec<String> {
    let event = match decode(text) {
        Ok(event) => event,
        Err(error) => {
            // There is no telling which book the bad frame belonged to.
            warn!(venue = %VENUE, %error, "undecodable frame, resyncing every market");
            return tracked
                .iter_mut()
                .filter_map(|(inst_id, state)| {
                    state.market.invalidate_mark();
                    state.resync().then(|| inst_id.clone())
                })
                .collect();
        }
    };

    match event {
        Event::Snapshot {
            inst_id,
            time,
            seq,
            bids,
            asks,
        } => {
            let Some(state) = tracked.get_mut(&inst_id) else {
                return Vec::new();
            };
            let ct = state.instrument.contract_value;
            match state
                .market
                .book
                .apply_snapshot(time, in_coin(bids, ct), in_coin(asks, ct))
            {
                Ok(()) => {
                    *got_snapshot = true;
                    state.last_seq = Some(seq);
                    state.market.publish();
                    debug!(venue = %VENUE, inst_id, "snapshot applied");
                    Vec::new()
                }
                Err(error) => {
                    warn!(venue = %VENUE, inst_id, %error, "snapshot rejected");
                    state.resync().then_some(inst_id).into_iter().collect()
                }
            }
        }
        Event::Update {
            inst_id,
            time,
            seq,
            prev_seq,
            bids,
            asks,
        } => {
            let Some(state) = tracked.get_mut(&inst_id) else {
                return Vec::new();
            };
            // Expected while a resubscribe is in flight: updates from the
            // old subscription keep arriving until the new snapshot lands.
            let Some(last_seq) = state.last_seq else {
                return Vec::new();
            };
            if prev_seq != last_seq {
                warn!(venue = %VENUE, inst_id, last_seq, prev_seq, "sequence gap");
                return state.resync().then_some(inst_id).into_iter().collect();
            }

            let ct = state.instrument.contract_value;
            let diff = |upserts| SideDiff {
                removed: Vec::new(),
                upserts,
            };
            let applied = state.market.book.apply_diff(
                time,
                &diff(in_coin(bids, ct)),
                &diff(in_coin(asks, ct)),
            );
            match applied {
                Ok(()) => {
                    state.last_seq = Some(seq);
                    state.market.publish();
                    Vec::new()
                }
                Err(Desync::NoSnapshot) => Vec::new(),
                Err(error) => {
                    warn!(venue = %VENUE, inst_id, %error, "book desynced");
                    state.resync().then_some(inst_id).into_iter().collect()
                }
            }
        }
        Event::Mark { inst_id, mark } => {
            if let Some(state) = tracked.get_mut(&inst_id) {
                state.market.set_mark(mark, received);
            }
            Vec::new()
        }
        Event::Funding {
            inst_id,
            rate,
            settles_at,
        } => {
            if let Some(state) = tracked.get_mut(&inst_id) {
                state.market.set_funding(rate, settles_at);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_coins_map_to_usdt_swaps() {
        assert_eq!(
            instrument("BTC"),
            Some(Instrument {
                inst_id: "BTC-USDT-SWAP".into(),
                contract_value: dec!(0.01)
            })
        );
        assert_eq!(instrument("ETH").unwrap().contract_value, dec!(0.1));
        assert_eq!(instrument("DOGE"), None);
    }

    #[test]
    fn listed_coins_have_a_maintenance_table_in_coin_units() {
        let btc = maintenance_table("BTC").unwrap();
        // 1,000 contracts is 10 BTC, 5,000 is 50 BTC.
        assert_eq!(btc.rate_for(dec!(10)), Ok(dec!(0.004)));
        assert_eq!(btc.rate_for(dec!(10.01)), Ok(dec!(0.005)));
        assert_eq!(btc.rate_for(dec!(2_000)), Ok(dec!(0.0525)));
        assert!(btc.rate_for(dec!(2_000.01)).is_err());

        let eth = maintenance_table("ETH").unwrap();
        assert_eq!(eth.rate_for(dec!(500)), Ok(dec!(0.004)));
        assert_eq!(eth.rate_for(dec!(25_000)), Ok(dec!(0.0525)));
        assert!(maintenance_table("DOGE").is_none());
    }

    #[test]
    fn decodes_snapshot() {
        let frame = r#"{"arg":{"channel":"books","instId":"BTC-USDT-SWAP"},"action":"snapshot",
            "data":[{"asks":[["60711.7","486.94","0","55"]],"bids":[["60711.6","313.29","0","27"]],
            "ts":"1795372786201","checksum":0,"seqId":356565078713,"prevSeqId":-1}]}"#;
        assert_eq!(
            decode(frame).unwrap(),
            Event::Snapshot {
                inst_id: "BTC-USDT-SWAP".into(),
                time: 1795372786201,
                seq: 356565078713,
                bids: vec![Level {
                    px: dec!(60711.6),
                    sz: dec!(313.29)
                }],
                asks: vec![Level {
                    px: dec!(60711.7),
                    sz: dec!(486.94)
                }],
            }
        );
    }

    #[test]
    fn decodes_funding() {
        // Recorded from the live feed on 2026-11-26.
        let frame = r#"{"arg":{"channel":"funding-rate","instId":"BTC-USDT-SWAP"},"data":[{
            "formulaType":"withRate","fundingRate":"0.0000520660769816",
            "fundingTime":"1795737600000","impactValue":"20000.0000000000000000",
            "instId":"BTC-USDT-SWAP","instType":"SWAP","interestRate":"0.0001000000000000",
            "maxFundingRate":"0.00375","method":"current_period","minFundingRate":"-0.00375",
            "nextFundingRate":"","nextFundingTime":"1795766400000",
            "premium":"-0.0004652068190741","prevFundingTime":"1795708800000",
            "settFundingRate":"0.0000448840561709","settState":"settled",
            "ts":"1795712949581"}]}"#;
        assert_eq!(
            decode(frame).unwrap(),
            Event::Funding {
                inst_id: "BTC-USDT-SWAP".into(),
                rate: dec!(0.0000520660769816),
                settles_at: 1_795_737_600_000,
            }
        );
    }

    #[test]
    fn decodes_update_with_its_sequence_numbers() {
        let frame = r#"{"arg":{"channel":"books","instId":"BTC-USDT-SWAP"},"action":"update",
            "data":[{"asks":[],"bids":[["60708.3","0","0","0"]],
            "ts":"1795372786301","checksum":0,"seqId":356565078800,"prevSeqId":356565078713}]}"#;
        assert_eq!(
            decode(frame).unwrap(),
            Event::Update {
                inst_id: "BTC-USDT-SWAP".into(),
                time: 1795372786301,
                seq: 356565078800,
                prev_seq: 356565078713,
                bids: vec![Level {
                    px: dec!(60708.3),
                    sz: dec!(0)
                }],
                asks: vec![],
            }
        );
    }

    #[test]
    fn decodes_mark() {
        let frame = r#"{"arg":{"channel":"mark-price","instId":"ETH-USDT-SWAP"},
            "data":[{"instId":"ETH-USDT-SWAP","instType":"SWAP","markPx":"1714.65","ts":"1795372785741"}]}"#;
        assert_eq!(
            decode(frame).unwrap(),
            Event::Mark {
                inst_id: "ETH-USDT-SWAP".into(),
                mark: dec!(1714.65)
            }
        );
    }

    #[test]
    fn decodes_control_frames() {
        assert_eq!(decode("pong").unwrap(), Event::Pong);
        assert_eq!(
            decode(r#"{"event":"subscribe","arg":{"channel":"books","instId":"BTC-USDT-SWAP"},"connId":"a"}"#)
                .unwrap(),
            Event::Subscribed
        );
        assert_eq!(
            decode(r#"{"event":"error","code":"60012","msg":"Illegal request","connId":"a"}"#)
                .unwrap(),
            Event::VenueError("60012: Illegal request".into())
        );
        assert_eq!(
            decode(r#"{"event":"notice","msg":"port change","code":"64010","connId":"a"}"#)
                .unwrap(),
            Event::Ignored("notice".into())
        );
        assert_eq!(
            decode(r#"{"arg":{"channel":"tickers","instId":"BTC-USDT-SWAP"},"data":[{}]}"#)
                .unwrap(),
            Event::Ignored("tickers".into())
        );
    }

    #[test]
    fn rejects_garbage() {
        assert!(matches!(decode("not json"), Err(DecodeError::Json(_))));
        assert!(matches!(
            decode(r#"{"arg":{"channel":"books","instId":"X"},"action":"update","data":[]}"#),
            Err(DecodeError::Empty("books"))
        ));
        assert!(matches!(
            decode(
                r#"{"arg":{"channel":"books","instId":"X"},"action":"partial",
                "data":[{"asks":[],"bids":[],"ts":"1","seqId":2,"prevSeqId":1}]}"#
            ),
            Err(DecodeError::Action(_))
        ));
        assert!(matches!(
            decode(
                r#"{"arg":{"channel":"books","instId":"X"},"action":"update",
                "data":[{"asks":[],"bids":[],"ts":"soon","seqId":2,"prevSeqId":1}]}"#
            ),
            Err(DecodeError::Timestamp(_))
        ));
    }

    #[test]
    fn sizes_convert_from_contracts_to_coin() {
        let levels = vec![
            Level {
                px: dec!(60711.7),
                sz: dec!(486.94),
            },
            Level {
                px: dec!(60708.3),
                sz: dec!(0),
            },
        ];
        let converted = in_coin(levels, dec!(0.01));
        assert_eq!(converted[0].sz, dec!(4.8694));
        assert!(converted[1].sz.is_zero());
    }
}
