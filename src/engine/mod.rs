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

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::risk::{
    Account, AccountState, Health, LiquidationPrice, MarketId, Marks, Position, PositionError,
    RiskError, Schedule, Side, Thresholds, account_health, liquidation_price, step,
};

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
    /// The account's risk could not be computed. Its state was left alone.
    Fault {
        account: AccountId,
        error: String,
        at: u64,
    },
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum EngineError {
    #[error("unknown account {0}")]
    UnknownAccount(AccountId),
    #[error("account {0} is closed")]
    AccountClosed(AccountId),
    #[error("collateral {0} must not be negative")]
    NegativeCollateral(Decimal),
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
    pub collateral: Decimal,
    /// `None` while any open position has no current mark.
    pub health: Option<Health>,
    pub positions: Vec<PositionView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PositionView {
    #[serde(flatten)]
    pub market: MarketId,
    pub side: Side,
    pub size: Decimal,
    pub entry_px: Decimal,
    pub mark: Option<Decimal>,
    pub unrealized_pnl: Option<Decimal>,
    pub liquidation_price: Option<LiquidationPrice>,
}

struct Entry {
    account: Account,
    state: AccountState,
}

pub struct Engine {
    schedule: Schedule,
    thresholds: Thresholds,
    marks: Marks,
    accounts: BTreeMap<AccountId, Entry>,
    next_id: u64,
}

impl Engine {
    pub fn new(schedule: Schedule, thresholds: Thresholds) -> Self {
        Self {
            schedule,
            thresholds,
            marks: Marks::new(),
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

    /// Fills a paper order at the market's current mark.
    ///
    /// A fill that leaves the position larger than it was is refused if it
    /// would put the account in a margin call or worse. A fill that leaves
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
        let entry = self
            .accounts
            .get_mut(&id)
            .ok_or(EngineError::UnknownAccount(id))?;
        if matches!(entry.state, AccountState::Closed { .. }) {
            return Err(EngineError::AccountClosed(id));
        }
        self.schedule.table(&market)?;
        let px = *self
            .marks
            .get(&market)
            .ok_or_else(|| EngineError::NoMark(market.clone()))?;
        let fill = Position::new(side, size, px)?;

        // Work on a copy, and only keep it if the result is acceptable.
        let mut account = entry.account.clone();
        let before = account.position(&market).copied();
        let realized_pnl = account.apply_fill(market.clone(), fill)?;
        let grows = match (before, account.position(&market)) {
            (_, None) => false,
            (None, Some(_)) => true,
            (Some(before), Some(after)) => {
                // A flip to a smaller position on the other side lowers
                // the margin requirement, so it counts as a reduction.
                after.size() > before.size()
            }
        };
        if grows {
            let health = account_health(&account, &self.marks, &self.schedule)?;
            if health.is_liquidatable() || self.thresholds.in_margin_call(&health)? {
                return Err(EngineError::InsufficientMargin);
            }
        }
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
            &self.schedule,
            &self.thresholds,
            now,
            &mut events,
        );
        Ok(events)
    }

    /// Records a new mark for a market, or its loss when `mark` is `None`,
    /// and re-evaluates every account that holds a position there.
    pub fn set_mark(&mut self, market: MarketId, mark: Option<Decimal>, now: u64) -> Vec<Event> {
        match mark {
            Some(px) => self.marks.insert(market.clone(), px),
            None => self.marks.remove(&market),
        };

        let mut events = Vec::new();
        for (id, entry) in &mut self.accounts {
            if entry.account.position(&market).is_some() {
                reassess(
                    *id,
                    entry,
                    &self.marks,
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
        let positions = account
            .positions()
            .map(|(market, position)| {
                let mark = self.marks.get(market).copied();
                PositionView {
                    market: market.clone(),
                    side: position.side(),
                    size: position.size(),
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
                }
            })
            .collect();
        Some(AccountView {
            id,
            state: entry.state,
            collateral: account.collateral,
            health: account_health(account, &self.marks, &self.schedule).ok(),
            positions,
        })
    }
}

/// Brings one account's state in line with the current marks, liquidating
/// it if it has fallen below maintenance margin.
fn reassess(
    id: AccountId,
    entry: &mut Entry,
    marks: &Marks,
    schedule: &Schedule,
    thresholds: &Thresholds,
    now: u64,
    events: &mut Vec<Event>,
) {
    match try_reassess(id, entry, marks, schedule, thresholds, now, events) {
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

fn try_reassess(
    id: AccountId,
    entry: &mut Entry,
    marks: &Marks,
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
        for closed in entry.account.close_all(marks)? {
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
        let (id, _) = engine.create_account(dec!(20_000), 1).unwrap();
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
        let events = engine.set_mark(okx_btc(), Some(dec!(61_200)), 4);
        let closed: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                Event::Liquidated { market, px, .. } => Some((market.clone(), *px)),
                _ => None,
            })
            .collect();
        assert_eq!(
            closed,
            [(market("BTC"), dec!(60_000)), (okx_btc(), dec!(61_200))]
        );
        let view = engine.view(id).unwrap();
        assert!(matches!(view.state, AccountState::Closed { .. }));
        assert_eq!(view.collateral, dec!(8_000));
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
}
