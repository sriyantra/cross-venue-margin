//! Sends orders to Hyperliquid testnet.
//!
//! With a testnet key configured, a fill on a Hyperliquid market is a real
//! order on the venue's testnet, and the engine records what the venue
//! reports: the size it filled and the average price. Every account's
//! orders go through one wallet, so the wallet's position in a coin is the
//! sum of the accounts' positions in it.
//!
//! Every order carries an id the engine logged before it was sent. When an
//! answer does not arrive, `repair` asks the venue what became of that id
//! and records the fill or drops the order.
//!
//! `sign` is pure. The price and reply handling in this module is pure too.
//! `Executor` is the I/O: it owns the HTTP client and the wallet.

pub mod sign;

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rust_decimal::{Decimal, dec};
use serde::Deserialize;
use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::{Mutex, MutexGuard, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use self::sign::{OrderAction, SignError, Wallet};
use crate::engine::task::EngineHandle;
use crate::engine::{EngineError, Event};
use crate::risk::{Side, Venue};

/// The only HTTP endpoint orders are ever sent to.
pub const TESTNET_API: &str = "https://api.hyperliquid-testnet.xyz";
/// The market data endpoint that goes with it. Prices on testnet are not
/// mainnet's, so an order sent there is checked against testnet's book.
pub const TESTNET_WS: &str = "wss://api.hyperliquid-testnet.xyz/ws";

/// How far past the expected price an order may fill. The order is a limit
/// order this far through the book that cancels whatever does not fill at
/// once.
const SLIPPAGE: Decimal = dec!(0.05);
/// A liquidation has to get out, so its order reaches further.
const LIQUIDATION_SLIPPAGE: Decimal = dec!(0.10);
/// The venue accepts prices of at most this many significant figures.
const PRICE_FIGURES: u32 = 5;
/// A price may have this many decimals, less the decimals of the size.
const PRICE_DECIMALS: u32 = 6;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// How often unanswered orders are asked after.
const REPAIR_EVERY: Duration = Duration::from_secs(15);
/// An order the venue still does not know this long after it was sent is
/// taken as never having arrived.
const GIVE_UP_AFTER_MS: u64 = 60_000;
/// Fills are searched for from this long before the order was sent, to
/// allow for a difference between this clock and the venue's.
const CLOCK_SLACK_MS: u64 = 60_000;

#[derive(Debug, Error)]
pub enum ExecutionError {
    #[error(transparent)]
    Sign(#[from] SignError),
    #[error("{0} is not listed on the venue")]
    UnknownCoin(String),
    #[error("size {size} has more than the {decimals} decimals the venue allows")]
    InvalidSize { size: Decimal, decimals: u32 },
    #[error("no limit price could be worked out from {0}")]
    InvalidPrice(Decimal),
    /// The request did not complete. The order may or may not have reached
    /// the venue, so the wallet may hold a position the engine does not
    /// know about yet. The order stays open until `repair` settles it.
    #[error("the venue could not be reached: {0}")]
    Transport(String),
    #[error("the venue refused the order: {0}")]
    Rejected(String),
    #[error("the venue filled nothing at the limit price")]
    NotFilled,
    #[error("the venue's reply was not understood: {0}")]
    Reply(String),
}

impl ExecutionError {
    /// True when the order may have filled all the same.
    pub fn outcome_unknown(&self) -> bool {
        matches!(self, Self::Transport(_) | Self::Reply(_))
    }
}

/// What the venue did with an order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Execution {
    /// The size that filled. It can be less than what was asked for.
    pub size: Decimal,
    /// The average price of the fill.
    pub px: Decimal,
    pub order_id: u64,
}

/// The wallet's account on the venue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VenueAccount {
    pub account_value: Decimal,
    /// Signed size per coin: longs positive, shorts negative.
    pub positions: BTreeMap<String, Decimal>,
}

#[derive(Debug, Clone, Copy)]
struct Asset {
    index: u32,
    size_decimals: u32,
}

/// The limit price for an order expected to fill near `reference`: above it
/// for a buy and below it for a sell, rounded to what the venue accepts.
fn limit_price(
    reference: Decimal,
    side: Side,
    slippage: Decimal,
    size_decimals: u32,
) -> Option<Decimal> {
    let factor = match side {
        Side::Long => Decimal::ONE + slippage,
        Side::Short => Decimal::ONE - slippage,
    };
    let px = reference
        .checked_mul(factor)?
        .round_sf(PRICE_FIGURES)?
        .round_dp(PRICE_DECIMALS.saturating_sub(size_decimals));
    (px > Decimal::ZERO).then(|| px.normalize())
}

#[derive(Deserialize)]
struct Filled {
    #[serde(rename = "totalSz")]
    total_sz: Decimal,
    #[serde(rename = "avgPx")]
    avg_px: Decimal,
    oid: u64,
}

/// Reads the venue's reply to an order of one leg.
fn parse_order_reply(reply: &Value) -> Result<Execution, ExecutionError> {
    let unexpected = || ExecutionError::Reply(reply.to_string());
    if reply["status"] != "ok" {
        return Err(match reply["response"].as_str() {
            Some(message) => ExecutionError::Rejected(message.to_string()),
            None => unexpected(),
        });
    }
    let status = &reply["response"]["data"]["statuses"][0];
    if let Some(message) = status["error"].as_str() {
        // An order that crosses nothing is reported as an error too.
        return Err(if message.contains("could not immediately match") {
            ExecutionError::NotFilled
        } else {
            ExecutionError::Rejected(message.to_string())
        });
    }
    let filled: Filled =
        serde_json::from_value(status["filled"].clone()).map_err(|_| unexpected())?;
    if filled.total_sz <= Decimal::ZERO {
        return Err(ExecutionError::NotFilled);
    }
    Ok(Execution {
        size: filled.total_sz,
        px: filled.avg_px,
        order_id: filled.oid,
    })
}

/// What became of an order whose answer did not arrive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Filled(Execution),
    /// The venue filled none of it and never will.
    Unfilled,
    /// Not known yet. Ask again later.
    Unknown,
}

/// What the venue says about an order looked up by id.
#[derive(Debug, Clone, PartialEq, Eq)]
enum OrderStatus {
    /// The venue has no order with that id.
    NotFound,
    /// The order is still working.
    Open,
    /// The order is finished. `filled` is true when all of `size` traded.
    Done {
        order_id: u64,
        size: Decimal,
        filled: bool,
    },
}

fn parse_order_status(reply: &Value) -> Option<OrderStatus> {
    match reply["status"].as_str()? {
        "unknownOid" => Some(OrderStatus::NotFound),
        "order" => {
            let order = &reply["order"]["order"];
            match reply["order"]["status"].as_str()? {
                "open" => Some(OrderStatus::Open),
                status => Some(OrderStatus::Done {
                    order_id: order["oid"].as_u64()?,
                    size: order["origSz"].as_str()?.parse().ok()?,
                    filled: status == "filled",
                }),
            }
        }
        _ => None,
    }
}

/// The size and average price of one order's fills among a wallet's
/// fills. `None` if the reply is not a list of fills.
fn sum_fills(reply: &Value, order_id: u64) -> Option<(Decimal, Decimal)> {
    let mut size = Decimal::ZERO;
    let mut notional = Decimal::ZERO;
    for fill in reply.as_array()? {
        if fill["oid"].as_u64()? != order_id {
            continue;
        }
        let px: Decimal = fill["px"].as_str()?.parse().ok()?;
        let sz: Decimal = fill["sz"].as_str()?.parse().ok()?;
        size = size.checked_add(sz)?;
        notional = notional.checked_add(px.checked_mul(sz)?)?;
    }
    if size.is_zero() {
        return Some((size, Decimal::ZERO));
    }
    Some((size.normalize(), notional.checked_div(size)?.normalize()))
}

fn parse_account(reply: &Value) -> Option<VenueAccount> {
    let account_value = reply["marginSummary"]["accountValue"]
        .as_str()?
        .parse()
        .ok()?;
    let mut positions = BTreeMap::new();
    for entry in reply["assetPositions"].as_array()? {
        let position = &entry["position"];
        let size: Decimal = position["szi"].as_str()?.parse().ok()?;
        if !size.is_zero() {
            positions.insert(position["coin"].as_str()?.to_string(), size);
        }
    }
    Some(VenueAccount {
        account_value,
        positions,
    })
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Places orders for one wallet.
#[derive(Debug)]
pub struct Executor {
    http: reqwest::Client,
    base: String,
    wallet: Wallet,
    assets: HashMap<String, Asset>,
    last_nonce: AtomicU64,
    turn: Mutex<()>,
}

impl Executor {
    /// Reads the venue's list of markets from `base` and gets ready to
    /// trade them with `key`. `base` is `TESTNET_API` outside of tests.
    pub async fn connect(base: &str, key: &str) -> Result<Self, ExecutionError> {
        let wallet = Wallet::from_hex(key)?;
        // Harmless if a provider is already installed.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|e| ExecutionError::Transport(e.to_string()))?;
        let mut executor = Self {
            http,
            base: base.trim_end_matches('/').to_string(),
            wallet,
            assets: HashMap::new(),
            last_nonce: AtomicU64::new(0),
            turn: Mutex::new(()),
        };
        let meta = executor.post("/info", &json!({ "type": "meta" })).await?;
        let universe = meta["universe"]
            .as_array()
            .ok_or_else(|| ExecutionError::Reply(meta.to_string()))?;
        for (index, asset) in universe.iter().enumerate() {
            if let (Some(name), Some(decimals)) =
                (asset["name"].as_str(), asset["szDecimals"].as_u64())
            {
                executor.assets.insert(
                    name.to_string(),
                    Asset {
                        index: index as u32,
                        size_decimals: decimals as u32,
                    },
                );
            }
        }
        Ok(executor)
    }

    pub fn address(&self) -> String {
        self.wallet.address()
    }

    /// Held by a caller across checking an order, sending it and recording
    /// the result, so that two orders never interleave those steps.
    pub async fn turn(&self) -> MutexGuard<'_, ()> {
        self.turn.lock().await
    }

    /// The venue wants every nonce from a wallet to be new. This is the
    /// time in milliseconds, moved forward if the clock has not advanced.
    fn nonce(&self) -> u64 {
        let now = now_ms();
        let mut last = self.last_nonce.load(Ordering::Relaxed);
        loop {
            let next = now.max(last + 1);
            match self.last_nonce.compare_exchange_weak(
                last,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return next,
                Err(seen) => last = seen,
            }
        }
    }

    async fn post(&self, path: &str, body: &Value) -> Result<Value, ExecutionError> {
        let transport = |e: reqwest::Error| ExecutionError::Transport(e.to_string());
        let response = self
            .http
            .post(format!("{}{path}", self.base))
            .json(body)
            .send()
            .await
            .map_err(transport)?;
        let status = response.status();
        let text = response.text().await.map_err(transport)?;
        // A failure on the way says nothing about what the venue did.
        if status.is_server_error() {
            return Err(ExecutionError::Transport(format!("{status}: {text}")));
        }
        if !status.is_success() {
            return Err(ExecutionError::Rejected(format!("{status}: {text}")));
        }
        serde_json::from_str(&text).map_err(|_| ExecutionError::Reply(text))
    }

    /// An id for the next order. It is also the order's nonce, so it is
    /// new for every order this wallet sends.
    pub fn next_order(&self) -> u64 {
        self.nonce()
    }

    /// Buys or sells `size` of `coin` at once, at no worse than `slippage`
    /// past `reference`. Whatever does not fill is cancelled. `order` comes
    /// from `next_order`.
    async fn order(
        &self,
        coin: &str,
        side: Side,
        size: Decimal,
        reference: Decimal,
        slippage: Decimal,
        order: u64,
    ) -> Result<Execution, ExecutionError> {
        let asset = self
            .assets
            .get(coin)
            .ok_or_else(|| ExecutionError::UnknownCoin(coin.to_string()))?;
        if size.round_dp(asset.size_decimals) != size {
            return Err(ExecutionError::InvalidSize {
                size,
                decimals: asset.size_decimals,
            });
        }
        let px = limit_price(reference, side, slippage, asset.size_decimals)
            .ok_or(ExecutionError::InvalidPrice(reference))?;
        let action = OrderAction::immediate(
            asset.index,
            side == Side::Long,
            px.to_string(),
            size.normalize().to_string(),
            order,
        );
        let nonce = order;
        let signature = self.wallet.sign(&action, nonce)?;
        let body = json!({
            "action": action,
            "nonce": nonce,
            "signature": signature,
            "vaultAddress": null,
        });
        let execution = parse_order_reply(&self.post("/exchange", &body).await?)?;
        info!(
            coin,
            ?side,
            %size,
            limit = %px,
            filled = %execution.size,
            px = %execution.px,
            order,
            order_id = execution.order_id,
            "order filled on testnet"
        );
        Ok(execution)
    }

    /// Sends an account's order to the venue. `reference` is the price the
    /// engine expects it to fill at, and `order` comes from `next_order`.
    pub async fn market_order(
        &self,
        coin: &str,
        side: Side,
        size: Decimal,
        reference: Decimal,
        order: u64,
    ) -> Result<Execution, ExecutionError> {
        self.order(coin, side, size, reference, SLIPPAGE, order)
            .await
    }

    /// Asks the venue what became of an order sent at `sent_at`.
    pub async fn outcome(&self, order: u64, sent_at: u64) -> Result<Outcome, ExecutionError> {
        let body = json!({
            "type": "orderStatus",
            "user": self.address(),
            "oid": sign::client_id(order),
        });
        let reply = self.post("/info", &body).await?;
        let status =
            parse_order_status(&reply).ok_or_else(|| ExecutionError::Reply(reply.to_string()))?;
        let (order_id, size, filled) = match status {
            OrderStatus::NotFound if now_ms() > sent_at.saturating_add(GIVE_UP_AFTER_MS) => {
                return Ok(Outcome::Unfilled);
            }
            OrderStatus::NotFound | OrderStatus::Open => return Ok(Outcome::Unknown),
            OrderStatus::Done {
                order_id,
                size,
                filled,
            } => (order_id, size, filled),
        };
        let body = json!({
            "type": "userFillsByTime",
            "user": self.address(),
            "startTime": sent_at.saturating_sub(CLOCK_SLACK_MS),
        });
        let reply = self.post("/info", &body).await?;
        let (traded, px) =
            sum_fills(&reply, order_id).ok_or_else(|| ExecutionError::Reply(reply.to_string()))?;
        Ok(if filled && traded != size {
            // The order is filled and its fills are not all listed yet.
            Outcome::Unknown
        } else if traded.is_zero() {
            Outcome::Unfilled
        } else {
            Outcome::Filled(Execution {
                size: traded,
                px,
                order_id,
            })
        })
    }

    /// The wallet's account value and positions on the venue.
    pub async fn account(&self) -> Result<VenueAccount, ExecutionError> {
        let body = json!({ "type": "clearinghouseState", "user": self.address() });
        let reply = self.post("/info", &body).await?;
        parse_account(&reply).ok_or_else(|| ExecutionError::Reply(reply.to_string()))
    }
}

/// A position the engine has liquidated and the venue still holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Close {
    pub coin: String,
    /// The side of the position that was liquidated.
    pub side: Side,
    pub size: Decimal,
    /// The price the engine closed it at.
    pub px: Decimal,
}

impl Close {
    /// The close an event calls for, if it is a liquidation on this venue.
    pub fn from_event(event: &Event) -> Option<Self> {
        match event {
            Event::Liquidated {
                market,
                side,
                size,
                px,
                ..
            } if market.venue == Venue::Hyperliquid => Some(Self {
                coin: market.coin.clone(),
                side: *side,
                size: *size,
                px: *px,
            }),
            _ => None,
        }
    }
}

/// Closes on the venue every position the engine liquidates, until the
/// sender is dropped.
///
/// The engine does not wait for this. It has already closed the position
/// in its own books at the price its copy of the book gave. The order here
/// brings the wallet back in line, and the price it gets can differ.
pub async fn run_closes(
    executor: std::sync::Arc<Executor>,
    mut closes: mpsc::UnboundedReceiver<Close>,
) {
    while let Some(close) = closes.recv().await {
        let closing = match close.side {
            Side::Long => Side::Short,
            Side::Short => Side::Long,
        };
        let _turn = executor.turn().await;
        match executor
            .order(
                &close.coin,
                closing,
                close.size,
                close.px,
                LIQUIDATION_SLIPPAGE,
                executor.next_order(),
            )
            .await
        {
            Ok(done) if done.size == close.size => {}
            Ok(done) => warn!(
                coin = close.coin,
                asked = %close.size,
                filled = %done.size,
                "liquidation only partly closed on the venue"
            ),
            Err(error) => warn!(coin = close.coin, %error, "liquidation not closed on the venue"),
        }
    }
}

/// Settles every order the engine sent and has no answer for: a fill the
/// venue made is recorded on the account that asked for it, and an order
/// the venue did not fill is dropped.
pub async fn repair(engine: &EngineHandle, executor: &Executor) {
    // No order is in flight while this holds the turn.
    let _turn = executor.turn().await;
    let Ok(orders) = engine.open_orders().await else {
        return;
    };
    for open in orders {
        let order = open.order;
        let done = match executor.outcome(order, open.sent_at).await {
            Ok(Outcome::Filled(done)) => done,
            Ok(Outcome::Unfilled) => {
                if engine.drop_order(order).await.is_ok() {
                    info!(order, account = %open.account, "order was not filled, dropped");
                }
                continue;
            }
            Ok(Outcome::Unknown) => continue,
            Err(error) => {
                warn!(order, %error, "order could not be looked up");
                continue;
            }
        };
        match engine
            .record_fill(order, done.size, done.px, done.order_id)
            .await
        {
            Ok(_) => info!(
                order,
                account = %open.account,
                market = %open.market,
                size = %done.size,
                px = %done.px,
                "recorded a fill whose answer was lost"
            ),
            // The account was liquidated while the order was out. The fill
            // has no account to go to, and reconciliation shows it.
            Err(EngineError::AccountClosed(account)) => {
                warn!(order, %account, size = %done.size, "fill belongs to a closed account");
                let _ = engine.drop_order(order).await;
            }
            Err(error) => warn!(order, %error, "fill could not be recorded"),
        }
    }
}

/// Runs `repair` at startup and then on a timer, until `shutdown`.
pub async fn run_repairs(
    engine: EngineHandle,
    executor: std::sync::Arc<Executor>,
    shutdown: CancellationToken,
) {
    let mut every = tokio::time::interval(REPAIR_EVERY);
    every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = every.tick() => repair(&engine, &executor).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_order_looked_up_is_found_finished_or_missing() {
        let filled = json!({"status": "order", "order": {
            "order": {"coin": "BTC", "sz": "0.0", "oid": 77, "origSz": "0.0002", "cloid": null},
            "status": "filled", "statusTimestamp": 1
        }});
        assert_eq!(
            parse_order_status(&filled),
            Some(OrderStatus::Done {
                order_id: 77,
                size: dec!(0.0002),
                filled: true,
            })
        );
        let cancelled = json!({"status": "order", "order": {
            "order": {"oid": 78, "origSz": "0.5"}, "status": "canceled"
        }});
        assert_eq!(
            parse_order_status(&cancelled),
            Some(OrderStatus::Done {
                order_id: 78,
                size: dec!(0.5),
                filled: false,
            })
        );
        let open = json!({"status": "order", "order": {
            "order": {"oid": 79, "origSz": "0.5"}, "status": "open"
        }});
        assert_eq!(parse_order_status(&open), Some(OrderStatus::Open));
        assert_eq!(
            parse_order_status(&json!({"status": "unknownOid"})),
            Some(OrderStatus::NotFound)
        );
        assert_eq!(parse_order_status(&json!({"status": "other"})), None);
    }

    #[test]
    fn fills_of_one_order_are_added_up_at_their_average_price() {
        let fills = json!([
            {"oid": 77, "px": "60000.0", "sz": "0.1"},
            {"oid": 12, "px": "1.0", "sz": "5"},
            {"oid": 77, "px": "60100.0", "sz": "0.3"}
        ]);
        assert_eq!(sum_fills(&fills, 77), Some((dec!(0.4), dec!(60075))));
        assert_eq!(sum_fills(&fills, 99), Some((dec!(0), dec!(0))));
        assert_eq!(sum_fills(&json!({"error": "x"}), 77), None);
    }

    #[test]
    fn limit_price_reaches_through_the_book_and_fits_the_venue() {
        // BTC sizes have 5 decimals, so prices have at most 1.
        assert_eq!(
            limit_price(dec!(61487), Side::Long, dec!(0.05), 5),
            Some(dec!(64561))
        );
        assert_eq!(
            limit_price(dec!(61487), Side::Short, dec!(0.05), 5),
            Some(dec!(58413))
        );
        // ETH sizes have 4 decimals. Five figures leave one decimal here.
        assert_eq!(
            limit_price(dec!(1743.85), Side::Long, dec!(0.05), 4),
            Some(dec!(1831))
        );
        // A small price keeps its figures but not more decimals than allowed.
        assert_eq!(
            limit_price(dec!(0.0123456), Side::Long, dec!(0), 2),
            Some(dec!(0.0123))
        );
    }

    #[test]
    fn limit_price_is_never_zero_or_below() {
        assert_eq!(limit_price(dec!(0.00001), Side::Short, dec!(0.05), 5), None);
        assert_eq!(limit_price(dec!(100), Side::Short, dec!(1), 0), None);
    }

    #[test]
    fn a_fill_is_read_from_the_reply() {
        let reply = json!({"status": "ok", "response": {"type": "order", "data": {"statuses": [
            {"filled": {"totalSz": "0.001", "avgPx": "61487.0", "oid": 77}}
        ]}}});
        assert_eq!(
            parse_order_reply(&reply).unwrap(),
            Execution {
                size: dec!(0.001),
                px: dec!(61487.0),
                order_id: 77,
            }
        );
    }

    #[test]
    fn refusals_are_told_apart() {
        let unmatched = json!({"status": "ok", "response": {"type": "order", "data": {"statuses": [
            {"error": "Order could not immediately match against any resting orders. asset=3"}
        ]}}});
        assert!(matches!(
            parse_order_reply(&unmatched),
            Err(ExecutionError::NotFilled)
        ));

        let small = json!({"status": "ok", "response": {"type": "order", "data": {"statuses": [
            {"error": "Order must have minimum value of $10."}
        ]}}});
        assert!(matches!(
            parse_order_reply(&small),
            Err(ExecutionError::Rejected(message)) if message.contains("minimum value")
        ));

        let failed = json!({"status": "err", "response": "User or API Wallet does not exist."});
        assert!(matches!(
            parse_order_reply(&failed),
            Err(ExecutionError::Rejected(message)) if message.contains("does not exist")
        ));

        let odd = json!({"status": "ok", "response": {"type": "order", "data": {"statuses": [
            {"resting": {"oid": 5}}
        ]}}});
        assert!(matches!(
            parse_order_reply(&odd),
            Err(ExecutionError::Reply(_))
        ));
    }

    #[test]
    fn account_positions_are_signed() {
        let reply = json!({
            "marginSummary": {"accountValue": "109.5"},
            "assetPositions": [
                {"position": {"coin": "BTC", "szi": "0.001"}},
                {"position": {"coin": "ETH", "szi": "-0.05"}},
                {"position": {"coin": "SOL", "szi": "0.0"}}
            ]
        });
        let account = parse_account(&reply).unwrap();
        assert_eq!(account.account_value, dec!(109.5));
        assert_eq!(account.positions.len(), 2);
        assert_eq!(account.positions["BTC"], dec!(0.001));
        assert_eq!(account.positions["ETH"], dec!(-0.05));
    }

    #[test]
    fn only_liquidations_on_this_venue_are_closed() {
        use crate::engine::AccountId;
        use crate::risk::MarketId;

        let liquidated = |venue| Event::Liquidated {
            account: AccountId(1),
            market: MarketId::new(venue, "BTC"),
            side: Side::Long,
            size: dec!(0.5),
            px: dec!(60000),
            realized_pnl: dec!(-100),
            fee: dec!(0),
            at: 1,
        };
        assert_eq!(
            Close::from_event(&liquidated(Venue::Hyperliquid)),
            Some(Close {
                coin: "BTC".into(),
                side: Side::Long,
                size: dec!(0.5),
                px: dec!(60000),
            })
        );
        assert_eq!(Close::from_event(&liquidated(Venue::Okx)), None);
    }
}
