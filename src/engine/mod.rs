//! The risk engine: paper accounts, the latest marks, and the rules that
//! connect them.
//!
//! `Engine` is pure. It has no I/O, no async and no clock: every method
//! takes the time as an argument and returns the events it caused. Feeding
//! the same calls in the same order always produces the same events, which
//! is what makes a recorded run replayable. `restore` rebuilds an engine
//! from the events an earlier one emitted. `task` wraps it in a Tokio task.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::book::average_price;
use crate::feed::BookView;
use crate::risk::{
    Account, AccountState, CASH_ASSET, Health, LiquidationPrice, MarketId, Marks, Position,
    PositionError, RiskError, Schedule, Side, Thresholds, Venue, account_health, asset_value,
    liquidation_price, matched_sizes, step,
};

/// The latest order book of each market the engine has been given one for.
type Depths = BTreeMap<MarketId, Arc<BookView>>;

/// The average price of trading `size` against a book: asks for a buy, bids
/// for a sell.
fn book_price(book: &BookView, side: Side, size: Decimal, partial: bool) -> Option<Decimal> {
    let levels = match side {
        Side::Long => &book.asks,
        Side::Short => &book.bids,
    };
    average_price(levels, size, partial)
}

pub mod restore;
pub mod task;

pub use restore::RestoreError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AccountId(pub u64);

impl fmt::Display for AccountId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Something that happened to an account. These are the facts a log has to
/// keep: none of them can be recomputed from the others.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    AccountOpened {
        account: AccountId,
        collateral: Decimal,
        at: u64,
    },
    /// Collateral added to an account: dollars, or a coin kept in kind.
    CollateralDeposited {
        account: AccountId,
        asset: String,
        amount: Decimal,
        at: u64,
    },
    Filled {
        account: AccountId,
        #[serde(flatten)]
        market: MarketId,
        side: Side,
        size: Decimal,
        px: Decimal,
        realized_pnl: Decimal,
        at: u64,
    },
    StateChanged {
        account: AccountId,
        from: AccountState,
        to: AccountState,
        at: u64,
    },
    /// A position closed by liquidation at the mark.
    Liquidated {
        account: AccountId,
        #[serde(flatten)]
        market: MarketId,
        side: Side,
        size: Decimal,
        px: Decimal,
        realized_pnl: Decimal,
        at: u64,
    },
    /// A funding payment on one position. `amount` is the change to the
    /// dollar balance: negative when the account paid.
    Funding {
        account: AccountId,
        #[serde(flatten)]
        market: MarketId,
        rate: Decimal,
        amount: Decimal,
        at: u64,
    },
    /// The account's risk could not be computed. Its state was left alone.
    Fault {
        account: AccountId,
        error: String,
        at: u64,
    },
}

impl Event {
    pub fn account(&self) -> AccountId {
        match self {
            Event::AccountOpened { account, .. }
            | Event::CollateralDeposited { account, .. }
            | Event::Filled { account, .. }
            | Event::StateChanged { account, .. }
            | Event::Liquidated { account, .. }
            | Event::Funding { account, .. }
            | Event::Fault { account, .. } => *account,
        }
    }

    /// Engine time of the event, in milliseconds.
    pub fn at(&self) -> u64 {
        match self {
            Event::AccountOpened { at, .. }
            | Event::CollateralDeposited { at, .. }
            | Event::Filled { at, .. }
            | Event::StateChanged { at, .. }
            | Event::Liquidated { at, .. }
            | Event::Funding { at, .. }
            | Event::Fault { at, .. } => *at,
        }
    }

    /// The event's `type` tag as it appears in JSON.
    pub fn kind(&self) -> &'static str {
        match self {
            Event::AccountOpened { .. } => "account_opened",
            Event::CollateralDeposited { .. } => "collateral_deposited",
            Event::Filled { .. } => "filled",
            Event::StateChanged { .. } => "state_changed",
            Event::Liquidated { .. } => "liquidated",
            Event::Funding { .. } => "funding",
            Event::Fault { .. } => "fault",
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum EngineError {
    #[error("unknown account {0}")]
    UnknownAccount(AccountId),
    #[error("account {0} is closed")]
    AccountClosed(AccountId),
    #[error("collateral {0} must not be negative")]
    NegativeCollateral(Decimal),
    #[error("deposit amount {0} must be above zero")]
    NonPositiveDeposit(Decimal),
    #[error("{0} is not accepted as collateral")]
    UnsupportedAsset(String),
    #[error("{0} has no maintenance schedule")]
    UnsupportedMarket(MarketId),
    #[error("no current mark for {0}")]
    NoMark(MarketId),
    #[error("invalid fill: {0}")]
    InvalidFill(#[from] PositionError),
    #[error("position size {size} is above the largest this market supports, {max}")]
    PositionTooLarge { size: Decimal, max: Decimal },
    #[error("not enough margin for this fill")]
    InsufficientMargin,
    #[error("the {market} book does not hold {size} on that side")]
    InsufficientLiquidity { market: MarketId, size: Decimal },
    #[error("risk calculation failed: {0}")]
    Risk(RiskError),
    #[error("engine is not running")]
    Unavailable,
}

impl From<RiskError> for EngineError {
    fn from(error: RiskError) -> Self {
        match error {
            RiskError::MissingMark(market) => EngineError::NoMark(market),
            RiskError::MissingSchedule(market) => EngineError::UnsupportedMarket(market),
            RiskError::UnsupportedAsset(asset) => EngineError::UnsupportedAsset(asset),
            RiskError::AboveSchedule { size, max } => EngineError::PositionTooLarge { size, max },
            other => EngineError::Risk(other),
        }
    }
}

/// An account as the API reports it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AccountView {
    pub id: AccountId,
    #[serde(flatten)]
    pub state: AccountState,
    /// The dollar balance.
    pub collateral: Decimal,
    /// Coins held as collateral.
    pub assets: Vec<AssetView>,
    /// `None` while any open position or collateral asset has no current
    /// mark.
    pub health: Option<Health>,
    pub positions: Vec<PositionView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AssetView {
    pub asset: String,
    pub amount: Decimal,
    /// The share of the asset's value that does not count.
    pub haircut: Option<Decimal>,
    pub mark: Option<Decimal>,
    /// What the asset counts for in equity: amount times mark, less the
    /// haircut.
    pub value: Option<Decimal>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PositionView {
    #[serde(flatten)]
    pub market: MarketId,
    pub side: Side,
    pub size: Decimal,
    /// The part of the size matched by an opposite position in the same
    /// coin on another venue. It pays the hedge rate, not the venue's.
    pub hedged_size: Decimal,
    pub entry_px: Decimal,
    pub mark: Option<Decimal>,
    pub unrealized_pnl: Option<Decimal>,
    pub liquidation_price: Option<LiquidationPrice>,
    /// The rate the venue will charge at its next funding settlement, and
    /// when that is. A positive rate is paid by longs.
    pub funding: Option<Funding>,
}

/// A funding rate and the time it is charged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Funding {
    /// The share of a position's notional a long pays a short.
    pub rate: Decimal,
    /// Milliseconds since the epoch.
    pub settles_at: u64,
}

struct Entry {
    account: Account,
    state: AccountState,
}

pub struct Engine {
    schedule: Schedule,
    thresholds: Thresholds,
    marks: Marks,
    depths: Depths,
    /// The next funding settlement of each market that has one coming.
    fundings: BTreeMap<MarketId, Funding>,
    accounts: BTreeMap<AccountId, Entry>,
    next_id: u64,
}

impl Engine {
    pub fn new(schedule: Schedule, thresholds: Thresholds) -> Self {
        Self {
            schedule,
            thresholds,
            marks: Marks::new(),
            depths: Depths::new(),
            fundings: BTreeMap::new(),
            accounts: BTreeMap::new(),
            next_id: 1,
        }
    }

    pub fn create_account(
        &mut self,
        collateral: Decimal,
        now: u64,
    ) -> Result<(AccountId, Vec<Event>), EngineError> {
        if collateral < Decimal::ZERO {
            return Err(EngineError::NegativeCollateral(collateral));
        }
        let id = AccountId(self.next_id);
        self.next_id += 1;
        self.accounts.insert(
            id,
            Entry {
                account: Account::new(collateral),
                state: AccountState::Open,
            },
        );
        let opened = Event::AccountOpened {
            account: id,
            collateral,
            at: now,
        };
        Ok((id, vec![opened]))
    }

    /// Adds collateral to an account. `asset` is the dollar asset or a coin
    /// the schedule accepts. A coin is kept in kind and valued at its mark
    /// from then on.
    pub fn deposit(
        &mut self,
        id: AccountId,
        asset: &str,
        amount: Decimal,
        now: u64,
    ) -> Result<Vec<Event>, EngineError> {
        let entry = self
            .accounts
            .get_mut(&id)
            .ok_or(EngineError::UnknownAccount(id))?;
        if matches!(entry.state, AccountState::Closed { .. }) {
            return Err(EngineError::AccountClosed(id));
        }
        if amount <= Decimal::ZERO {
            return Err(EngineError::NonPositiveDeposit(amount));
        }
        if asset != CASH_ASSET {
            self.schedule.collateral(asset)?;
        }
        entry.account.deposit(asset, amount)?;

        let mut events = vec![Event::CollateralDeposited {
            account: id,
            asset: asset.to_string(),
            amount,
            at: now,
        }];
        reassess(
            id,
            entry,
            &self.marks,
            &self.depths,
            &self.schedule,
            &self.thresholds,
            now,
            &mut events,
        );
        Ok(events)
    }

    /// Gives the engine a market's current order book, or takes it away
    /// when `book` is `None`. Nothing is re-evaluated: the book only prices
    /// the fills and liquidations that come after.
    pub fn set_depth(&mut self, market: MarketId, book: Option<Arc<BookView>>) {
        match book {
            Some(book) => self.depths.insert(market, book),
            None => self.depths.remove(&market),
        };
    }

    /// Records the rate a venue will charge at its next funding
    /// settlement, replacing the one held for that market.
    ///
    /// Call `settle_funding` first. A venue announces its next settlement
    /// only after the last one has passed, so replacing a settlement that
    /// is already due would lose it.
    pub fn set_funding(&mut self, market: MarketId, funding: Funding) {
        self.fundings.insert(market, funding);
    }

    /// Charges every funding settlement that has come due.
    ///
    /// Each position in a settling market pays `size * mark * rate` if it
    /// is long and receives it if it is short. A negative rate reverses
    /// that. The payment moves the dollar balance, and the account is
    /// re-evaluated like after any other loss or gain.
    ///
    /// A settlement is charged once. One that comes due while its market
    /// has no mark is dropped, because there is no price to charge it at.
    pub fn settle_funding(&mut self, now: u64) -> Vec<Event> {
        let due: Vec<(MarketId, Funding)> = self
            .fundings
            .iter()
            .filter(|(_, funding)| funding.settles_at <= now)
            .map(|(market, funding)| (market.clone(), *funding))
            .collect();
        let mut events = Vec::new();
        for (market, funding) in due {
            self.fundings.remove(&market);
            let Some(&mark) = self.marks.get(&market) else {
                continue;
            };
            for (&id, entry) in self.accounts.iter_mut() {
                let Some(position) = entry.account.position(&market) else {
                    continue;
                };
                let amount = position
                    .size()
                    .checked_mul(mark)
                    .and_then(|notional| notional.checked_mul(funding.rate))
                    .map(|payment| -(payment * position.side().sign()));
                let balance = amount.and_then(|a| entry.account.collateral.checked_add(a));
                let (Some(amount), Some(balance)) = (amount, balance) else {
                    events.push(Event::Fault {
                        account: id,
                        error: RiskError::Overflow.to_string(),
                        at: now,
                    });
                    continue;
                };
                if amount.is_zero() {
                    continue;
                }
                entry.account.collateral = balance;
                events.push(Event::Funding {
                    account: id,
                    market: market.clone(),
                    rate: funding.rate,
                    amount,
                    at: now,
                });
                reassess(
                    id,
                    entry,
                    &self.marks,
                    &self.depths,
                    &self.schedule,
                    &self.thresholds,
                    now,
                    &mut events,
                );
            }
        }
        events
    }

    /// Fills a paper order.
    ///
    /// With a book for the market, the fill walks it: a buy takes asks and a
    /// sell takes bids, level by level, and the fill price is the average.
    /// An order larger than the side of the book is refused. Without a book
    /// the fill is at the market's mark.
    ///
    /// A fill that leaves the position larger than it was is refused if the
    /// account's equity would be below its initial margin requirement, or
    /// if it would put the account in a margin call or worse. A fill that leaves
    /// it the same size or smaller is always accepted, including one that
    /// flips it to the other side. A refused fill changes nothing.
    pub fn fill(
        &mut self,
        id: AccountId,
        market: MarketId,
        side: Side,
        size: Decimal,
        now: u64,
    ) -> Result<Vec<Event>, EngineError> {
        let (account, px, realized_pnl) = self.priced(id, &market, side, size, None)?;
        self.commit(id, account, market, side, size, px, realized_pnl, now)
    }

    /// Says whether `fill` would accept this order, without changing
    /// anything, and returns the price it would fill at. An order that is
    /// about to be sent to a venue is checked here first.
    pub fn check_fill(
        &self,
        id: AccountId,
        market: &MarketId,
        side: Side,
        size: Decimal,
    ) -> Result<Decimal, EngineError> {
        self.priced(id, market, side, size, None)
            .map(|(_, px, _)| px)
    }

    /// Records a fill that a venue has already made, at the venue's price.
    ///
    /// The margin rules are not applied: the position exists on the venue
    /// whether or not this account can carry it. If it cannot, the
    /// reassessment that follows deals with it like any other loss.
    pub fn record_fill(
        &mut self,
        id: AccountId,
        market: MarketId,
        side: Side,
        size: Decimal,
        px: Decimal,
        now: u64,
    ) -> Result<Vec<Event>, EngineError> {
        let (account, px, realized_pnl) = self.priced(id, &market, side, size, Some(px))?;
        self.commit(id, account, market, side, size, px, realized_pnl, now)
    }

    /// The sum of every account's position in each coin on one venue, longs
    /// positive and shorts negative. It is what a single venue account
    /// holding all of them would show.
    pub fn net_positions(&self, venue: Venue) -> BTreeMap<String, Decimal> {
        let mut net = BTreeMap::new();
        for entry in self.accounts.values() {
            for (market, position) in entry.account.positions() {
                if market.venue == venue {
                    *net.entry(market.coin.clone()).or_insert(Decimal::ZERO) +=
                        position.size() * position.side().sign();
                }
            }
        }
        net.retain(|_, size| !size.is_zero());
        net
    }

    /// Works out the account a fill would leave behind, its price and the
    /// profit it realizes. With `at` the fill is one that already happened
    /// at that price, and nothing can refuse it but a bad input. Without it
    /// the fill is priced off the book and must pass the margin rules.
    fn priced(
        &self,
        id: AccountId,
        market: &MarketId,
        side: Side,
        size: Decimal,
        at: Option<Decimal>,
    ) -> Result<(Account, Decimal, Decimal), EngineError> {
        let entry = self
            .accounts
            .get(&id)
            .ok_or(EngineError::UnknownAccount(id))?;
        if matches!(entry.state, AccountState::Closed { .. }) {
            return Err(EngineError::AccountClosed(id));
        }
        self.schedule.table(market)?;
        let px = match at {
            Some(px) => px,
            None => {
                let mark = *self
                    .marks
                    .get(market)
                    .ok_or_else(|| EngineError::NoMark(market.clone()))?;
                Position::new(side, size, mark)?;
                match self.depths.get(market) {
                    Some(book) => book_price(book, side, size, false).ok_or_else(|| {
                        EngineError::InsufficientLiquidity {
                            market: market.clone(),
                            size,
                        }
                    })?,
                    None => mark,
                }
            }
        };
        let fill = Position::new(side, size, px)?;

        // Work on a copy, and only keep it if the result is acceptable.
        let mut account = entry.account.clone();
        let before = account.position(market).copied();
        let realized_pnl = account.apply_fill(market.clone(), fill)?;
        let grows = match (before, account.position(market)) {
            (_, None) => false,
            (None, Some(_)) => true,
            (Some(before), Some(after)) => {
                // A flip to a smaller position on the other side lowers
                // the margin requirement, so it counts as a reduction.
                after.size() > before.size()
            }
        };
        if grows && at.is_none() {
            let health = account_health(&account, &self.marks, &self.schedule)?;
            if health.equity < health.initial_required
                || health.is_liquidatable()
                || self.thresholds.in_margin_call(&health)?
            {
                return Err(EngineError::InsufficientMargin);
            }
        }
        Ok((account, px, realized_pnl))
    }

    #[allow(clippy::too_many_arguments)]
    fn commit(
        &mut self,
        id: AccountId,
        account: Account,
        market: MarketId,
        side: Side,
        size: Decimal,
        px: Decimal,
        realized_pnl: Decimal,
        now: u64,
    ) -> Result<Vec<Event>, EngineError> {
        let entry = self
            .accounts
            .get_mut(&id)
            .ok_or(EngineError::UnknownAccount(id))?;
        entry.account = account;

        let mut events = vec![Event::Filled {
            account: id,
            market,
            side,
            size,
            px,
            realized_pnl,
            at: now,
        }];
        reassess(
            id,
            entry,
            &self.marks,
            &self.depths,
            &self.schedule,
            &self.thresholds,
            now,
            &mut events,
        );
        Ok(events)
    }

    /// Records a new mark for a market, or its loss when `mark` is `None`,
    /// and re-evaluates every account that holds a position there or holds
    /// collateral priced by it.
    pub fn set_mark(&mut self, market: MarketId, mark: Option<Decimal>, now: u64) -> Vec<Event> {
        match mark {
            Some(px) => self.marks.insert(market.clone(), px),
            None => self.marks.remove(&market),
        };

        let mut events = Vec::new();
        for (id, entry) in &mut self.accounts {
            let priced_here = entry.account.assets().any(|(asset, _)| {
                self.schedule
                    .collateral(asset)
                    .is_ok_and(|rule| rule.price() == &market)
            });
            if priced_here || entry.account.position(&market).is_some() {
                reassess(
                    *id,
                    entry,
                    &self.marks,
                    &self.depths,
                    &self.schedule,
                    &self.thresholds,
                    now,
                    &mut events,
                );
            }
        }
        events
    }

    pub fn view(&self, id: AccountId) -> Option<AccountView> {
        let entry = self.accounts.get(&id)?;
        let account = &entry.account;
        let matched = matched_sizes(account, &self.schedule).unwrap_or_default();
        let positions = account
            .positions()
            .map(|(market, position)| {
                let mark = self.marks.get(market).copied();
                PositionView {
                    market: market.clone(),
                    side: position.side(),
                    size: position.size(),
                    hedged_size: matched.get(market).copied().unwrap_or(Decimal::ZERO),
                    entry_px: position.entry_px(),
                    mark,
                    unrealized_pnl: mark.and_then(|px| position.unrealized_pnl(px).ok()),
                    liquidation_price: liquidation_price(
                        account,
                        market,
                        &self.marks,
                        &self.schedule,
                    )
                    .ok(),
                    funding: self.fundings.get(market).copied(),
                }
            })
            .collect();
        let assets = account
            .assets()
            .map(|(asset, amount)| {
                let rule = self.schedule.collateral(asset).ok();
                AssetView {
                    asset: asset.to_string(),
                    amount,
                    haircut: rule.map(|rule| rule.haircut()),
                    mark: rule.and_then(|rule| self.marks.get(rule.price()).copied()),
                    value: asset_value(asset, amount, &self.marks, &self.schedule).ok(),
                }
            })
            .collect();
        Some(AccountView {
            id,
            state: entry.state,
            collateral: account.collateral,
            assets,
            health: account_health(account, &self.marks, &self.schedule).ok(),
            positions,
        })
    }
}

/// Brings one account's state in line with the current marks, liquidating
/// it if it has fallen below maintenance margin.
#[allow(clippy::too_many_arguments)]
fn reassess(
    id: AccountId,
    entry: &mut Entry,
    marks: &Marks,
    depths: &Depths,
    schedule: &Schedule,
    thresholds: &Thresholds,
    now: u64,
    events: &mut Vec<Event>,
) {
    match try_reassess(id, entry, marks, depths, schedule, thresholds, now, events) {
        // With a mark missing there is no health to act on. The account
        // keeps its state until every mark is back.
        Ok(()) | Err(RiskError::MissingMark(_)) => {}
        Err(error) => events.push(Event::Fault {
            account: id,
            error: error.to_string(),
            at: now,
        }),
    }
}

#[allow(clippy::too_many_arguments)]
fn try_reassess(
    id: AccountId,
    entry: &mut Entry,
    marks: &Marks,
    depths: &Depths,
    schedule: &Schedule,
    thresholds: &Thresholds,
    now: u64,
    events: &mut Vec<Event>,
) -> Result<(), RiskError> {
    let advance = |entry: &mut Entry, events: &mut Vec<Event>| -> Result<(), RiskError> {
        let health = account_health(&entry.account, marks, schedule)?;
        let has_positions = entry.account.has_positions();
        let (next, transition) = step(entry.state, &health, has_positions, thresholds, now)?;
        entry.state = next;
        if let Some(t) = transition {
            events.push(Event::StateChanged {
                account: id,
                from: t.from,
                to: t.to,
                at: t.at,
            });
        }
        Ok(())
    };

    advance(entry, events)?;
    if matches!(entry.state, AccountState::Liquidating { .. }) {
        // A long is closed by selling into the bids and a short by buying
        // from the asks. Size the book cannot fill is priced at its last
        // level. A market with no book closes at its mark.
        let exit = |market: &MarketId, position: &Position| {
            let mark = crate::risk::health::mark_for(marks, market)?;
            let closing = match position.side() {
                Side::Long => Side::Short,
                Side::Short => Side::Long,
            };
            Ok(depths
                .get(market)
                .and_then(|book| book_price(book, closing, position.size(), true))
                .unwrap_or(mark))
        };
        for closed in entry.account.close_all_at(exit)? {
            events.push(Event::Liquidated {
                account: id,
                market: closed.market,
                side: closed.position.side(),
                size: closed.position.size(),
                px: closed.px,
                realized_pnl: closed.realized_pnl,
                at: now,
            });
        }
        advance(entry, events)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::risk::{TierTable, Venue};
    use rust_decimal::dec;

    fn market(coin: &str) -> MarketId {
        MarketId::new(Venue::Hyperliquid, coin)
    }

    /// BTC at a flat 1.25% maintenance rate, margin call below 1.5x.
    fn engine() -> Engine {
        let mut schedule = Schedule::new();
        schedule.insert(market("BTC"), TierTable::flat(dec!(0.0125)).unwrap());
        schedule.insert(market("ETH"), TierTable::flat(dec!(0.02)).unwrap());
        Engine::new(schedule, Thresholds::new(dec!(1.5)).unwrap())
    }

    /// One account with 3,000 of collateral, long 1 BTC from 60,000.
    fn long_btc() -> (Engine, AccountId) {
        let mut engine = engine();
        engine.set_mark(market("BTC"), Some(dec!(60_000)), 0);
        let (id, _) = engine.create_account(dec!(3_000), 1).unwrap();
        engine
            .fill(id, market("BTC"), Side::Long, dec!(1), 2)
            .unwrap();
        (engine, id)
    }

    fn okx_btc() -> MarketId {
        MarketId::new(Venue::Okx, "BTC")
    }

    /// The engine above plus OKX BTC with its real size brackets.
    fn two_venue_engine() -> Engine {
        let mut schedule = Schedule::new();
        schedule.insert(market("BTC"), TierTable::flat(dec!(0.0125)).unwrap());
        schedule.insert(
            okx_btc(),
            crate::venue::okx::maintenance_table("BTC").unwrap(),
        );
        let mut engine = Engine::new(schedule, Thresholds::new(dec!(1.5)).unwrap());
        engine.set_mark(market("BTC"), Some(dec!(60_000)), 0);
        engine.set_mark(okx_btc(), Some(dec!(60_000)), 0);
        engine
    }

    #[test]
    fn one_venue_moving_liquidates_positions_on_both() {
        let mut engine = two_venue_engine();
        let (id, _) = engine.create_account(dec!(22_000), 1).unwrap();
        engine
            .fill(id, market("BTC"), Side::Long, dec!(10), 2)
            .unwrap();
        engine
            .fill(id, okx_btc(), Side::Short, dec!(10), 3)
            .unwrap();
        // 600,000 at 1.25% plus 600,000 at 0.4%.
        let health = engine.view(id).unwrap().health.unwrap();
        assert_eq!(health.maintenance_required, dec!(9_900));

        // Only the short's venue moves. The long gains nothing to offset it.
        let events = engine.set_mark(okx_btc(), Some(dec!(61_300)), 4);
        let closed: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                Event::Liquidated { market, px, .. } => Some((market.clone(), *px)),
                _ => None,
            })
            .collect();
        assert_eq!(
            closed,
            [(market("BTC"), dec!(60_000)), (okx_btc(), dec!(61_300))]
        );
        let view = engine.view(id).unwrap();
        assert!(matches!(view.state, AccountState::Closed { .. }));
        assert_eq!(view.collateral, dec!(9_000));
    }

    /// The two-venue engine, accepting BTC as collateral at a 10% haircut
    /// and pricing it at the Hyperliquid mark.
    fn coin_engine() -> Engine {
        let mut engine = two_venue_engine();
        engine.schedule.insert_collateral(
            "BTC",
            crate::risk::CollateralRule::new(market("BTC"), dec!(0.10)).unwrap(),
        );
        engine
    }

    #[test]
    fn coin_collateral_backs_positions_on_both_venues() {
        let mut engine = coin_engine();
        let (id, _) = engine.create_account(dec!(0), 1).unwrap();
        // No dollars, so no fill is possible yet.
        assert_eq!(
            engine.fill(id, okx_btc(), Side::Long, dec!(1), 2),
            Err(EngineError::InsufficientMargin)
        );

        let events = engine.deposit(id, "BTC", dec!(0.5), 3).unwrap();
        assert_eq!(
            events,
            [Event::CollateralDeposited {
                account: id,
                asset: "BTC".into(),
                amount: dec!(0.5),
                at: 3
            }]
        );
        engine
            .fill(id, market("BTC"), Side::Long, dec!(1), 4)
            .unwrap();
        engine.fill(id, okx_btc(), Side::Short, dec!(1), 5).unwrap();

        let view = engine.view(id).unwrap();
        assert_eq!(view.collateral, dec!(0));
        assert_eq!(
            view.assets,
            [AssetView {
                asset: "BTC".into(),
                amount: dec!(0.5),
                haircut: Some(dec!(0.10)),
                mark: Some(dec!(60_000)),
                value: Some(dec!(27_000)),
            }]
        );
        let health = view.health.unwrap();
        // 0.5 BTC at 60,000 less 10%.
        assert_eq!(health.collateral_value, dec!(27_000));
        assert_eq!(health.equity, dec!(27_000));
        // 60,000 at 1.25% plus 60,000 at 0.4%.
        assert_eq!(health.maintenance_required, dec!(990));
    }

    #[test]
    fn a_long_backed_by_the_same_coin_liquidates_sooner() {
        let liquidation_price =
            |engine: &Engine, id| match engine.view(id).unwrap().positions[0].liquidation_price {
                Some(LiquidationPrice::At(px)) => px,
                other => panic!("expected a price, got {other:?}"),
            };

        // 2,700 of dollars against a long of 1 BTC.
        let mut dollars = coin_engine();
        let (a, _) = dollars.create_account(dec!(2_700), 1).unwrap();
        dollars
            .fill(a, market("BTC"), Side::Long, dec!(1), 2)
            .unwrap();

        // The same 2,700 of equity held as 0.05 BTC.
        let mut coins = coin_engine();
        let (b, _) = coins.create_account(dec!(0), 1).unwrap();
        coins.deposit(b, "BTC", dec!(0.05), 2).unwrap();
        coins
            .fill(b, market("BTC"), Side::Long, dec!(1), 3)
            .unwrap();
        assert_eq!(coins.view(b).unwrap().health.unwrap().equity, dec!(2_700));

        // A falling price takes the collateral down with the position.
        assert!(liquidation_price(&coins, b) > liquidation_price(&dollars, a));

        // 58,100 is below the coin account's price and above the other's.
        let px = dec!(58_100);
        assert!(liquidation_price(&coins, b) > px && px > liquidation_price(&dollars, a));
        let events = coins.set_mark(market("BTC"), Some(px), 10);
        assert!(
            events
                .iter()
                .any(|event| matches!(event, Event::Liquidated { .. }))
        );
        let events = dollars.set_mark(market("BTC"), Some(px), 10);
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, Event::Liquidated { .. }))
        );

        // Liquidation closes the position. The coin stays in the account.
        let view = coins.view(b).unwrap();
        assert!(matches!(view.state, AccountState::Closed { .. }));
        assert_eq!(view.collateral, dec!(-1_900));
        assert_eq!(view.assets[0].amount, dec!(0.05));
    }

    #[test]
    fn a_short_covered_by_the_same_coin_is_never_liquidated_by_its_price() {
        let mut engine = coin_engine();
        let (id, _) = engine.create_account(dec!(0), 1).unwrap();
        engine.deposit(id, "BTC", dec!(2), 2).unwrap();
        engine
            .fill(id, market("BTC"), Side::Short, dec!(1), 3)
            .unwrap();
        assert_eq!(
            engine.view(id).unwrap().positions[0].liquidation_price,
            Some(LiquidationPrice::Never)
        );
        // The price triples. The short loses 120,000, the coin gains 216,000.
        assert!(
            engine
                .set_mark(market("BTC"), Some(dec!(180_000)), 4)
                .is_empty()
        );
        assert_eq!(engine.view(id).unwrap().state, AccountState::Open);
    }

    #[test]
    fn collateral_price_moves_reach_accounts_with_positions_elsewhere() {
        let mut engine = coin_engine();
        let (id, _) = engine.create_account(dec!(0), 1).unwrap();
        engine.deposit(id, "BTC", dec!(0.05), 2).unwrap();
        // The only position is on OKX. The collateral is priced on Hyperliquid.
        engine.fill(id, okx_btc(), Side::Long, dec!(4), 3).unwrap();
        // 2,700 of equity against 960 of maintenance.
        assert_eq!(engine.view(id).unwrap().state, AccountState::Open);

        // Hyperliquid falls. The OKX mark has not moved, so the position has
        // no loss, but the collateral is now worth 900.
        let events = engine.set_mark(market("BTC"), Some(dec!(20_000)), 4);
        assert!(
            events
                .iter()
                .any(|event| matches!(event, Event::Liquidated { .. }))
        );

        // With the collateral's mark gone, health is unknown.
        let (id, _) = engine.create_account(dec!(0), 5).unwrap();
        engine.deposit(id, "BTC", dec!(1), 6).unwrap();
        engine.set_mark(market("BTC"), None, 7);
        let view = engine.view(id).unwrap();
        assert_eq!(view.health, None);
        assert_eq!(view.assets[0].value, None);
    }

    #[test]
    fn deposits_are_checked_and_a_dollar_deposit_adds_to_the_balance() {
        let mut engine = coin_engine();
        let (id, _) = engine.create_account(dec!(100), 1).unwrap();
        assert_eq!(
            engine.deposit(id, "DOGE", dec!(1), 2),
            Err(EngineError::UnsupportedAsset("DOGE".into()))
        );
        assert_eq!(
            engine.deposit(id, "BTC", dec!(0), 2),
            Err(EngineError::NonPositiveDeposit(dec!(0)))
        );
        assert_eq!(
            engine.deposit(AccountId(99), "BTC", dec!(1), 2),
            Err(EngineError::UnknownAccount(AccountId(99)))
        );
        engine.deposit(id, "USDC", dec!(50), 3).unwrap();
        let view = engine.view(id).unwrap();
        assert_eq!(view.collateral, dec!(150));
        assert!(view.assets.is_empty());
    }

    #[test]
    fn a_deposit_can_end_a_margin_call() {
        let (mut engine, id) = long_btc();
        engine.schedule.insert_collateral(
            "BTC",
            crate::risk::CollateralRule::new(market("BTC"), dec!(0.10)).unwrap(),
        );
        engine.set_mark(market("BTC"), Some(dec!(58_000)), 10);
        let events = engine.deposit(id, "BTC", dec!(0.1), 11).unwrap();
        assert_eq!(
            events[1],
            Event::StateChanged {
                account: id,
                from: AccountState::MarginCall { since: 10 },
                to: AccountState::Open,
                at: 11
            }
        );
    }

    #[test]
    fn a_hedge_across_venues_pays_the_hedge_rate() {
        let hedged = |hedge_rate| {
            let mut engine = two_venue_engine();
            engine.schedule.set_hedge_rate(hedge_rate).unwrap();
            let (id, _) = engine.create_account(dec!(22_000), 1).unwrap();
            engine
                .fill(id, market("BTC"), Side::Long, dec!(10), 2)
                .unwrap();
            engine
                .fill(id, okx_btc(), Side::Short, dec!(10), 3)
                .unwrap();
            (engine, id)
        };

        let (mut gross, a) = hedged(None);
        let (mut net, b) = hedged(Some(dec!(0.0025)));
        // 600,000 at 1.25% plus 600,000 at 0.4%, against 0.25% on each leg.
        let health = |engine: &Engine, id| engine.view(id).unwrap().health.unwrap();
        assert_eq!(health(&gross, a).maintenance_required, dec!(9_900));
        assert_eq!(health(&net, b).maintenance_required, dec!(3_000));
        assert_eq!(health(&net, b).initial_required, dec!(6_000));
        let view = net.view(b).unwrap();
        assert_eq!(view.positions[0].hedged_size, dec!(10));
        assert_eq!(view.positions[1].hedged_size, dec!(10));
        assert_eq!(gross.view(a).unwrap().positions[0].hedged_size, dec!(0));

        // The two venues' marks drift 1,300 apart. Both accounts lose
        // 13,000. Only the gross one is below its requirement.
        let liquidated = |events: Vec<Event>| {
            events
                .iter()
                .any(|event| matches!(event, Event::Liquidated { .. }))
        };
        assert!(liquidated(gross.set_mark(okx_btc(), Some(dec!(61_300)), 4)));
        assert!(!liquidated(net.set_mark(okx_btc(), Some(dec!(61_300)), 4)));
        // The gap reaches 1,900 and the hedge rate no longer covers it.
        assert!(liquidated(net.set_mark(okx_btc(), Some(dec!(61_900)), 5)));
    }

    #[test]
    fn the_unhedged_part_of_a_position_pays_its_venue_table() {
        let mut engine = two_venue_engine();
        engine.schedule.set_hedge_rate(Some(dec!(0.0025))).unwrap();
        let (id, _) = engine.create_account(dec!(50_000), 1).unwrap();
        engine
            .fill(id, market("BTC"), Side::Long, dec!(10), 2)
            .unwrap();
        engine.fill(id, okx_btc(), Side::Short, dec!(4), 3).unwrap();
        let view = engine.view(id).unwrap();
        assert_eq!(view.positions[0].hedged_size, dec!(4));
        // Matched: 240,000 at 0.25% on each leg. Outright: 360,000 at 1.25%.
        assert_eq!(view.health.unwrap().maintenance_required, dec!(5_700));
    }

    #[test]
    fn a_fill_needs_initial_margin_at_max_leverage() {
        let (mut engine, id) = long_btc();
        // 3,000 of equity. Initial margin is 2.5%, so 120,000 is the most
        // it carries: 2 BTC. A margin call would not start until 2.66 BTC.
        assert_eq!(
            engine.fill(id, market("BTC"), Side::Long, dec!(1.01), 3),
            Err(EngineError::InsufficientMargin)
        );
        engine
            .fill(id, market("BTC"), Side::Long, dec!(1), 4)
            .unwrap();
        let health = engine.view(id).unwrap().health.unwrap();
        assert_eq!(health.initial_required, dec!(3_000));
        assert_eq!(health.maintenance_required, dec!(1_500));
    }

    fn book(bids: &[(Decimal, Decimal)], asks: &[(Decimal, Decimal)]) -> Arc<BookView> {
        let levels = |side: &[(Decimal, Decimal)]| {
            side.iter()
                .map(|&(px, sz)| crate::book::Level { px, sz })
                .collect()
        };
        Arc::new(BookView {
            venue: Venue::Hyperliquid,
            coin: "BTC".into(),
            time: 0,
            bids: levels(bids),
            asks: levels(asks),
            mid: None,
        })
    }

    #[test]
    fn fills_and_liquidations_walk_the_book() {
        let mut engine = engine();
        engine.set_mark(market("BTC"), Some(dec!(60_000)), 0);
        engine.set_depth(
            market("BTC"),
            Some(book(
                &[(dec!(59_990), dec!(0.5)), (dec!(59_980), dec!(1))],
                &[(dec!(60_010), dec!(0.5)), (dec!(60_020), dec!(1))],
            )),
        );
        let (id, _) = engine.create_account(dec!(3_000), 1).unwrap();

        // The asks hold 1.5.
        assert_eq!(
            engine.fill(id, market("BTC"), Side::Long, dec!(2), 2),
            Err(EngineError::InsufficientLiquidity {
                market: market("BTC"),
                size: dec!(2)
            })
        );
        // 0.5 at 60,010 and 0.5 at 60,020.
        let events = engine
            .fill(id, market("BTC"), Side::Long, dec!(1), 3)
            .unwrap();
        assert!(matches!(events[0], Event::Filled { px, .. } if px == dec!(60_015)));
        let view = engine.view(id).unwrap();
        assert_eq!(view.positions[0].entry_px, dec!(60_015));
        // The cost of crossing the spread shows as a loss at the mark.
        assert_eq!(view.positions[0].unrealized_pnl, Some(dec!(-15)));

        // The bids thin out, and then the mark falls through maintenance.
        // The long is sold into what is there: 0.4 at 57,600, and the 0.6
        // the book cannot take at that same last level.
        engine.set_depth(
            market("BTC"),
            Some(book(
                &[(dec!(57_600), dec!(0.4))],
                &[(dec!(57_700), dec!(1))],
            )),
        );
        let events = engine.set_mark(market("BTC"), Some(dec!(57_700)), 10);
        let closed: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                Event::Liquidated {
                    px, realized_pnl, ..
                } => Some((*px, *realized_pnl)),
                _ => None,
            })
            .collect();
        assert_eq!(closed, [(dec!(57_600), dec!(-2_415))]);
        assert_eq!(engine.view(id).unwrap().collateral, dec!(585));

        // With the book taken away, fills go back to the mark.
        engine.set_depth(market("BTC"), None);
        let (id, _) = engine.create_account(dec!(3_000), 11).unwrap();
        let events = engine
            .fill(id, market("BTC"), Side::Long, dec!(1), 12)
            .unwrap();
        assert!(matches!(events[0], Event::Filled { px, .. } if px == dec!(57_700)));
    }

    #[test]
    fn position_past_the_last_size_bracket_is_refused() {
        let mut engine = two_venue_engine();
        let (id, _) = engine.create_account(dec!(1_000_000_000), 1).unwrap();
        assert_eq!(
            engine.fill(id, okx_btc(), Side::Long, dec!(2_000.01), 2),
            Err(EngineError::PositionTooLarge {
                size: dec!(2_000.01),
                max: dec!(2_000)
            })
        );
        assert!(engine.view(id).unwrap().positions.is_empty());
        engine
            .fill(id, okx_btc(), Side::Long, dec!(2_000), 3)
            .unwrap();
    }

    #[test]
    fn fill_opens_a_position_at_the_mark() {
        let (engine, id) = long_btc();
        let view = engine.view(id).unwrap();
        assert_eq!(view.state, AccountState::Open);
        assert_eq!(view.positions.len(), 1);
        assert_eq!(view.positions[0].entry_px, dec!(60_000));
        assert_eq!(view.positions[0].unrealized_pnl, Some(dec!(0)));
        let health = view.health.unwrap();
        assert_eq!(health.equity, dec!(3_000));
        assert_eq!(health.maintenance_required, dec!(750));
    }

    #[test]
    fn falling_mark_walks_the_account_to_liquidation() {
        let (mut engine, id) = long_btc();

        // Equity 1,000 against maintenance 725: inside the 1.5x band.
        let events = engine.set_mark(market("BTC"), Some(dec!(58_000)), 10);
        assert_eq!(
            events,
            [Event::StateChanged {
                account: id,
                from: AccountState::Open,
                to: AccountState::MarginCall { since: 10 },
                at: 10
            }]
        );

        // Equity 700 against maintenance 721.25: liquidated at the mark.
        let events = engine.set_mark(market("BTC"), Some(dec!(57_700)), 20);
        assert_eq!(
            events,
            [
                Event::StateChanged {
                    account: id,
                    from: AccountState::MarginCall { since: 10 },
                    to: AccountState::Liquidating { since: 20 },
                    at: 20
                },
                Event::Liquidated {
                    account: id,
                    market: market("BTC"),
                    side: Side::Long,
                    size: dec!(1),
                    px: dec!(57_700),
                    realized_pnl: dec!(-2_300),
                    at: 20
                },
                Event::StateChanged {
                    account: id,
                    from: AccountState::Liquidating { since: 20 },
                    to: AccountState::Closed { at: 20 },
                    at: 20
                },
            ]
        );

        let view = engine.view(id).unwrap();
        assert_eq!(view.collateral, dec!(700));
        assert!(view.positions.is_empty());
        assert_eq!(
            engine.fill(id, market("BTC"), Side::Long, dec!(1), 30),
            Err(EngineError::AccountClosed(id))
        );
    }

    #[test]
    fn fill_that_would_start_in_a_margin_call_is_refused() {
        let (mut engine, id) = long_btc();
        let before = engine.view(id);
        // 3 BTC needs 2,250 of maintenance, and 3,000 is below 1.5x that.
        assert_eq!(
            engine.fill(id, market("BTC"), Side::Long, dec!(2), 3),
            Err(EngineError::InsufficientMargin)
        );
        assert_eq!(engine.view(id), before);
    }

    #[test]
    fn reducing_fill_is_accepted_in_a_margin_call_and_can_end_it() {
        let (mut engine, id) = long_btc();
        engine.set_mark(market("BTC"), Some(dec!(58_000)), 10);

        let events = engine
            .fill(id, market("BTC"), Side::Short, dec!(0.5), 11)
            .unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(
            events[1],
            Event::StateChanged {
                account: id,
                from: AccountState::MarginCall { since: 10 },
                to: AccountState::Open,
                at: 11
            }
        );
        assert_eq!(engine.view(id).unwrap().collateral, dec!(2_000));
    }

    #[test]
    fn flip_to_a_smaller_position_counts_as_a_reduction() {
        let (mut engine, id) = long_btc();
        engine.set_mark(market("BTC"), Some(dec!(58_000)), 10);
        // Long 1 becomes short 0.9: smaller, so allowed in a margin call.
        engine
            .fill(id, market("BTC"), Side::Short, dec!(1.9), 11)
            .unwrap();
        let view = engine.view(id).unwrap();
        assert_eq!(view.positions[0].side, Side::Short);
        assert_eq!(view.positions[0].size, dec!(0.9));
        // Long 1 to short 1.5 is larger, and equity cannot carry it.
        let (mut engine, id) = long_btc();
        engine.set_mark(market("BTC"), Some(dec!(58_000)), 10);
        assert_eq!(
            engine.fill(id, market("BTC"), Side::Short, dec!(2.5), 11),
            Err(EngineError::InsufficientMargin)
        );
    }

    #[test]
    fn lost_mark_hides_health_and_freezes_the_state() {
        let (mut engine, id) = long_btc();
        assert_eq!(engine.set_mark(market("BTC"), None, 10), []);

        let view = engine.view(id).unwrap();
        assert_eq!(view.health, None);
        assert_eq!(view.positions[0].mark, None);
        assert!(view.positions[0].liquidation_price.is_some());
        assert_eq!(view.state, AccountState::Open);
        assert_eq!(
            engine.fill(id, market("BTC"), Side::Short, dec!(1), 11),
            Err(EngineError::NoMark(market("BTC")))
        );
    }

    #[test]
    fn fills_need_a_known_account_a_schedule_and_a_valid_size() {
        let (mut engine, id) = long_btc();
        assert_eq!(
            engine.fill(AccountId(99), market("BTC"), Side::Long, dec!(1), 3),
            Err(EngineError::UnknownAccount(AccountId(99)))
        );
        assert_eq!(
            engine.fill(id, market("DOGE"), Side::Long, dec!(1), 3),
            Err(EngineError::UnsupportedMarket(market("DOGE")))
        );
        assert_eq!(
            engine.fill(id, market("BTC"), Side::Long, dec!(0), 3),
            Err(EngineError::InvalidFill(PositionError::NonPositiveSize(
                dec!(0)
            )))
        );
        assert_eq!(
            engine.create_account(dec!(-1), 3),
            Err(EngineError::NegativeCollateral(dec!(-1)))
        );
    }

    #[test]
    fn a_loss_on_one_market_liquidates_positions_on_both() {
        let mut engine = engine();
        engine.set_mark(market("BTC"), Some(dec!(60_000)), 0);
        engine.set_mark(market("ETH"), Some(dec!(3_000)), 0);
        let (id, _) = engine.create_account(dec!(5_000), 1).unwrap();
        engine
            .fill(id, market("BTC"), Side::Long, dec!(1), 2)
            .unwrap();
        engine
            .fill(id, market("ETH"), Side::Long, dec!(10), 3)
            .unwrap();

        let events = engine.set_mark(market("ETH"), Some(dec!(2_620)), 10);
        let liquidated: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                Event::Liquidated { market, .. } => Some(market.coin.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(liquidated, ["BTC", "ETH"]);
        assert_eq!(
            engine.view(id).unwrap().state,
            AccountState::Closed { at: 10 }
        );
    }

    #[test]
    fn the_same_inputs_replay_to_the_same_events() {
        let run = || {
            let mut engine = engine();
            let mut log = engine.set_mark(market("BTC"), Some(dec!(60_000)), 0);
            let (id, opened) = engine.create_account(dec!(3_000), 1).unwrap();
            log.extend(opened);
            log.extend(
                engine
                    .fill(id, market("BTC"), Side::Long, dec!(1), 2)
                    .unwrap(),
            );
            for (at, px) in [(3, dec!(59_000)), (4, dec!(58_000)), (5, dec!(57_000))] {
                log.extend(engine.set_mark(market("BTC"), Some(px), at));
            }
            (log, engine.view(id))
        };
        assert_eq!(run(), run());
        assert_eq!(run().0.len(), 6);
    }

    #[test]
    fn deposit_events_serialize_as_tagged_json() {
        let event = Event::CollateralDeposited {
            account: AccountId(1),
            asset: "BTC".into(),
            amount: dec!(0.5),
            at: 7,
        };
        assert_eq!(
            serde_json::to_value(&event).unwrap(),
            serde_json::json!({
                "type": "collateral_deposited", "account": 1, "asset": "BTC",
                "amount": "0.5", "at": 7
            })
        );
    }

    #[test]
    fn events_serialize_as_tagged_json() {
        let event = Event::Filled {
            account: AccountId(1),
            market: market("BTC"),
            side: Side::Long,
            size: dec!(0.5),
            px: dec!(60_000),
            realized_pnl: dec!(0),
            at: 7,
        };
        assert_eq!(
            serde_json::to_value(&event).unwrap(),
            serde_json::json!({
                "type": "filled", "account": 1, "venue": "hyperliquid", "coin": "BTC",
                "side": "long", "size": "0.5", "px": "60000", "realized_pnl": "0", "at": 7
            })
        );
    }
    fn funding(rate: Decimal, settles_at: u64) -> Funding {
        Funding { rate, settles_at }
    }

    #[test]
    fn a_long_pays_positive_funding_and_a_short_receives_it() {
        let (mut engine, long) = long_btc();
        let (short, _) = engine.create_account(dec!(3_000), 3).unwrap();
        engine
            .fill(short, market("BTC"), Side::Short, dec!(0.5), 4)
            .unwrap();
        engine.set_funding(market("BTC"), funding(dec!(0.0001), 100));

        // Nothing is charged before the settlement time.
        assert!(engine.settle_funding(99).is_empty());
        assert_eq!(
            engine.view(long).unwrap().positions[0].funding,
            Some(funding(dec!(0.0001), 100))
        );

        // 1 BTC at 60,000 pays 6. Half a BTC short receives 3.
        let events = engine.settle_funding(100);
        assert_eq!(
            events,
            vec![
                Event::Funding {
                    account: long,
                    market: market("BTC"),
                    rate: dec!(0.0001),
                    amount: dec!(-6),
                    at: 100,
                },
                Event::Funding {
                    account: short,
                    market: market("BTC"),
                    rate: dec!(0.0001),
                    amount: dec!(3),
                    at: 100,
                },
            ]
        );
        assert_eq!(engine.view(long).unwrap().collateral, dec!(2_994));
        assert_eq!(engine.view(short).unwrap().collateral, dec!(3_003));

        // A settlement is charged once.
        assert!(engine.settle_funding(200).is_empty());
        assert_eq!(engine.view(long).unwrap().positions[0].funding, None);
    }

    #[test]
    fn negative_funding_pays_the_long() {
        let (mut engine, id) = long_btc();
        engine.set_funding(market("BTC"), funding(dec!(-0.0002), 10));
        engine.settle_funding(10);
        assert_eq!(engine.view(id).unwrap().collateral, dec!(3_012));
    }

    #[test]
    fn funding_uses_the_mark_at_settlement() {
        let (mut engine, id) = long_btc();
        engine.set_funding(market("BTC"), funding(dec!(0.0001), 10));
        engine.set_mark(market("BTC"), Some(dec!(61_000)), 5);
        engine.settle_funding(10);
        assert_eq!(engine.view(id).unwrap().collateral, dec!(2_993.9));
    }

    #[test]
    fn funding_without_a_mark_is_dropped() {
        let (mut engine, id) = long_btc();
        engine.set_funding(market("BTC"), funding(dec!(0.0001), 10));
        engine.set_mark(market("BTC"), None, 5);
        assert!(engine.settle_funding(10).is_empty());
        engine.set_mark(market("BTC"), Some(dec!(60_000)), 11);
        assert!(engine.settle_funding(12).is_empty());
        assert_eq!(engine.view(id).unwrap().collateral, dec!(3_000));
    }

    #[test]
    fn funding_can_liquidate_an_account() {
        // Maintenance on 1 BTC at 60,000 is 750. A payment of 2,400 leaves
        // 600 of equity.
        let (mut engine, id) = long_btc();
        engine.set_funding(market("BTC"), funding(dec!(0.04), 10));
        let events = engine.settle_funding(10);
        assert!(matches!(events[0], Event::Funding { amount, .. } if amount == dec!(-2_400)));
        assert!(events.iter().any(|e| matches!(e, Event::Liquidated { .. })));
        assert!(matches!(
            engine.view(id).unwrap().state,
            AccountState::Closed { .. }
        ));
    }

    #[test]
    fn a_hedged_pair_at_one_rate_and_mark_nets_to_zero() {
        let mut engine = two_venue_engine();
        engine.set_mark(market("BTC"), Some(dec!(60_000)), 0);
        engine.set_mark(okx_btc(), Some(dec!(60_000)), 0);
        let (id, _) = engine.create_account(dec!(10_000), 1).unwrap();
        engine
            .fill(id, market("BTC"), Side::Long, dec!(1), 2)
            .unwrap();
        engine.fill(id, okx_btc(), Side::Short, dec!(1), 3).unwrap();
        engine.set_funding(market("BTC"), funding(dec!(0.0001), 10));
        engine.set_funding(okx_btc(), funding(dec!(0.0001), 10));
        assert_eq!(engine.settle_funding(10).len(), 2);
        assert_eq!(engine.view(id).unwrap().collateral, dec!(10_000));
    }
}
