//! Rebuilding an engine from its own event log.
//!
//! The log is the record of what happened to every account, in order.
//! Folding it from the start gives back every account's collateral,
//! positions and state. Collateral held in coins comes back as amounts.
//! What it is worth is not in the log, because that depends on the marks. Marks are not in the log. They are live data, and a
//! restored engine gets them from the feeds like a new one does.
//!
//! Every event is checked against the state it is applied to. A log that
//! does not describe a possible history is rejected, not repaired.

use rust_decimal::Decimal;
use thiserror::Error;

use super::{AccountId, Engine, Entry, Event};
use crate::risk::{
    Account, AccountState, MarketId, Position, PositionError, RiskError, Schedule, Thresholds,
};

/// Why a log could not be replayed. `index` counts events from zero.
#[derive(Debug, Error, PartialEq, Eq)]
#[error("event {index}: {reason}")]
pub struct RestoreError {
    pub index: usize,
    pub reason: Reason,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum Reason {
    #[error("account {0} was opened twice")]
    DuplicateAccount(AccountId),
    #[error("account {0} was never opened")]
    UnknownAccount(AccountId),
    #[error("account {0} is closed")]
    AccountClosed(AccountId),
    #[error("fill realizes {replayed}, but the log recorded {logged}")]
    RealizedPnl { logged: Decimal, replayed: Decimal },
    #[error("state change starts from {logged:?}, but the account is {replayed:?}")]
    State {
        logged: AccountState,
        replayed: AccountState,
    },
    #[error("deposit of {0} is not above zero")]
    Deposit(Decimal),
    #[error("liquidation of {0} does not match the position held")]
    Liquidation(MarketId),
    #[error(transparent)]
    Position(#[from] PositionError),
    #[error(transparent)]
    Risk(#[from] RiskError),
}

impl Engine {
    /// Rebuilds an engine from events in the order they were emitted.
    ///
    /// The result has every account the log describes and no marks. A log
    /// cut off at any point is still a valid log: the engine finishes
    /// whatever was in progress when marks arrive.
    pub fn restore<'a>(
        schedule: Schedule,
        thresholds: Thresholds,
        events: impl IntoIterator<Item = &'a Event>,
    ) -> Result<Self, RestoreError> {
        let mut engine = Engine::new(schedule, thresholds);
        for (index, event) in events.into_iter().enumerate() {
            engine
                .apply(event)
                .map_err(|reason| RestoreError { index, reason })?;
        }
        Ok(engine)
    }

    fn apply(&mut self, event: &Event) -> Result<(), Reason> {
        match event {
            Event::AccountOpened {
                account,
                collateral,
                ..
            } => {
                if self.accounts.contains_key(account) {
                    return Err(Reason::DuplicateAccount(*account));
                }
                self.accounts.insert(
                    *account,
                    Entry {
                        account: Account::new(*collateral),
                        state: AccountState::Open,
                    },
                );
                self.next_id = self.next_id.max(account.0 + 1);
            }
            Event::CollateralDeposited {
                account,
                asset,
                amount,
                ..
            } => {
                if *amount <= Decimal::ZERO {
                    return Err(Reason::Deposit(*amount));
                }
                self.open_entry(*account)?.account.deposit(asset, *amount)?;
            }
            Event::Filled {
                account,
                market,
                side,
                size,
                px,
                realized_pnl,
                ..
            } => {
                let entry = self.open_entry(*account)?;
                let fill = Position::new(*side, *size, *px)?;
                let replayed = entry.account.apply_fill(market.clone(), fill)?;
                if replayed != *realized_pnl {
                    return Err(Reason::RealizedPnl {
                        logged: *realized_pnl,
                        replayed,
                    });
                }
            }
            Event::StateChanged {
                account, from, to, ..
            } => {
                let entry = self.open_entry(*account)?;
                if entry.state != *from {
                    return Err(Reason::State {
                        logged: *from,
                        replayed: entry.state,
                    });
                }
                entry.state = *to;
            }
            Event::Liquidated {
                account,
                market,
                side,
                size,
                realized_pnl,
                ..
            } => {
                let entry = self.open_entry(*account)?;
                let held = entry
                    .account
                    .position(market)
                    .is_some_and(|held| held.side() == *side && held.size() == *size);
                if !held {
                    return Err(Reason::Liquidation(market.clone()));
                }
                entry.account.remove_position(market);
                entry.account.collateral = entry
                    .account
                    .collateral
                    .checked_add(*realized_pnl)
                    .ok_or(RiskError::Overflow)?;
            }
            Event::Funding {
                account, amount, ..
            } => {
                let entry = self.open_entry(*account)?;
                entry.account.collateral = entry
                    .account
                    .collateral
                    .checked_add(*amount)
                    .ok_or(RiskError::Overflow)?;
            }
            // A fault changed nothing when it happened.
            Event::Fault { account, .. } => {
                self.entry(*account)?;
            }
        }
        Ok(())
    }

    fn entry(&mut self, id: AccountId) -> Result<&mut Entry, Reason> {
        self.accounts.get_mut(&id).ok_or(Reason::UnknownAccount(id))
    }

    /// An account that can still change.
    fn open_entry(&mut self, id: AccountId) -> Result<&mut Entry, Reason> {
        let entry = self.entry(id)?;
        if matches!(entry.state, AccountState::Closed { .. }) {
            return Err(Reason::AccountClosed(id));
        }
        Ok(entry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::risk::{Side, TierTable, Venue};
    use rust_decimal::dec;

    fn btc() -> MarketId {
        MarketId::new(Venue::Hyperliquid, "BTC")
    }

    fn parts() -> (Schedule, Thresholds) {
        let mut schedule = Schedule::new();
        schedule.insert(btc(), TierTable::flat(dec!(0.0125)).unwrap());
        (schedule, Thresholds::new(dec!(1.5)).unwrap())
    }

    /// An account that is opened, filled, and liquidated by a mark move.
    fn liquidated_run() -> (Engine, Vec<Event>) {
        let (schedule, thresholds) = parts();
        let mut engine = Engine::new(schedule, thresholds);
        engine.set_mark(btc(), Some(dec!(60_000)), 0);
        let (id, mut events) = engine.create_account(dec!(3_000), 1).unwrap();
        events.extend(engine.fill(id, btc(), Side::Long, dec!(1), 2).unwrap());
        events.extend(engine.set_mark(btc(), Some(dec!(57_700)), 3));
        (engine, events)
    }

    fn restore(events: &[Event]) -> Result<Engine, RestoreError> {
        let (schedule, thresholds) = parts();
        Engine::restore(schedule, thresholds, events)
    }

    #[test]
    fn restored_engine_has_the_same_accounts() {
        let (mut original, events) = liquidated_run();
        assert_eq!(events.len(), 5);
        let mut restored = restore(&events).unwrap();

        // Marks are not restored. Give both the same one and compare.
        original.set_mark(btc(), Some(dec!(58_000)), 4);
        assert!(restored.set_mark(btc(), Some(dec!(58_000)), 4).is_empty());
        let id = AccountId(1);
        assert_eq!(restored.view(id), original.view(id));
        assert!(matches!(
            restored.view(id).unwrap().state,
            AccountState::Closed { at: 3 }
        ));

        // Ids continue after the highest one in the log.
        let (next, _) = restored.create_account(dec!(1), 5).unwrap();
        assert_eq!(next, AccountId(2));
    }

    #[test]
    fn log_cut_off_mid_liquidation_is_finished_by_the_next_mark() {
        let (_, events) = liquidated_run();
        // Keep the move to Liquidating and drop what followed it.
        let mut restored = restore(&events[..3]).unwrap();
        let events = restored.set_mark(btc(), Some(dec!(57_700)), 9);
        assert!(matches!(events[0], Event::Liquidated { .. }));
        assert!(matches!(
            restored.view(AccountId(1)).unwrap().state,
            AccountState::Closed { at: 9 }
        ));
    }

    #[test]
    fn impossible_logs_are_rejected() {
        let (_, events) = liquidated_run();

        let reason = |events: &[Event]| restore(events).err().map(|e| (e.index, e.reason));
        assert_eq!(
            reason(&events[1..]),
            Some((0, Reason::UnknownAccount(AccountId(1))))
        );
        let twice = [events[0].clone(), events[0].clone()];
        assert_eq!(
            reason(&twice),
            Some((1, Reason::DuplicateAccount(AccountId(1))))
        );
        // A fill after the account closed.
        let mut after_close = events.clone();
        after_close.push(events[1].clone());
        assert_eq!(
            reason(&after_close),
            Some((5, Reason::AccountClosed(AccountId(1))))
        );
        // The liquidation without the fill that opened the position.
        let no_fill = [events[0].clone(), events[3].clone()];
        assert_eq!(reason(&no_fill), Some((1, Reason::Liquidation(btc()))));
        // A state change from a state the account is not in.
        let wrong_state = [events[0].clone(), events[4].clone()];
        assert!(matches!(
            reason(&wrong_state),
            Some((1, Reason::State { .. }))
        ));
    }

    #[test]
    fn deposits_are_restored_and_a_log_from_before_them_still_loads() {
        let (mut schedule, thresholds) = parts();
        schedule.insert_collateral(
            "BTC",
            crate::risk::CollateralRule::new(btc(), dec!(0.10)).unwrap(),
        );
        let mut original = Engine::new(schedule.clone(), thresholds);
        original.set_mark(btc(), Some(dec!(60_000)), 0);
        let (id, mut events) = original.create_account(dec!(0), 1).unwrap();
        events.extend(original.deposit(id, "BTC", dec!(0.5), 2).unwrap());
        events.extend(original.deposit(id, "USDC", dec!(250), 3).unwrap());
        events.extend(original.fill(id, btc(), Side::Long, dec!(1), 4).unwrap());

        let mut restored = Engine::restore(schedule, thresholds, &events).unwrap();
        assert!(restored.set_mark(btc(), Some(dec!(60_000)), 5).is_empty());
        assert_eq!(restored.view(id), original.view(id));
        assert_eq!(restored.view(id).unwrap().assets[0].amount, dec!(0.5));

        let mut bad = events.clone();
        bad[1] = Event::CollateralDeposited {
            account: id,
            asset: "BTC".into(),
            amount: dec!(-1),
            at: 2,
        };
        let (schedule, thresholds) = parts();
        assert_eq!(
            Engine::restore(schedule, thresholds, &bad)
                .err()
                .map(|e| (e.index, e.reason)),
            Some((1, Reason::Deposit(dec!(-1))))
        );

        // An event written before deposits existed, exactly as stored.
        let old = r#"{"type":"account_opened","account":1,"collateral":"100000","at":1}"#;
        let old: Event = serde_json::from_str(old).unwrap();
        let (schedule, thresholds) = parts();
        let restored = Engine::restore(schedule, thresholds, [&old]).unwrap();
        let view = restored.view(AccountId(1)).unwrap();
        assert_eq!(view.collateral, dec!(100_000));
        assert!(view.assets.is_empty());
    }

    #[test]
    fn tampered_realized_pnl_is_rejected() {
        let (schedule, thresholds) = parts();
        let mut engine = Engine::new(schedule, thresholds);
        engine.set_mark(btc(), Some(dec!(60_000)), 0);
        let (id, mut events) = engine.create_account(dec!(30_000), 1).unwrap();
        events.extend(engine.fill(id, btc(), Side::Long, dec!(1), 2).unwrap());
        engine.set_mark(btc(), Some(dec!(61_000)), 3);
        events.extend(engine.fill(id, btc(), Side::Short, dec!(1), 4).unwrap());
        assert!(restore(&events).is_ok());

        let Some(Event::Filled { realized_pnl, .. }) = events.last_mut() else {
            panic!("expected a fill");
        };
        *realized_pnl = dec!(5_000);
        assert_eq!(
            restore(&events).err().map(|e| e.reason),
            Some(Reason::RealizedPnl {
                logged: dec!(5_000),
                replayed: dec!(1_000)
            })
        );
    }

    #[test]
    fn events_survive_a_round_trip_through_json() {
        let (_, events) = liquidated_run();
        for event in &events {
            let json = serde_json::to_value(event).unwrap();
            assert_eq!(json["type"], event.kind());
            assert_eq!(json["account"], event.account().0);
            assert_eq!(json["at"], event.at());
        }
        let json = serde_json::to_string(&events).unwrap();
        let back: Vec<Event> = serde_json::from_str(&json).unwrap();
        assert_eq!(back, events);
    }

    #[test]
    fn funding_payments_are_restored() {
        let (schedule, thresholds) = parts();
        let mut engine = Engine::new(schedule, thresholds);
        engine.set_mark(btc(), Some(dec!(60_000)), 0);
        let (id, mut events) = engine.create_account(dec!(30_000), 1).unwrap();
        events.extend(engine.fill(id, btc(), Side::Long, dec!(1), 2).unwrap());
        engine.set_funding(
            btc(),
            crate::engine::Funding {
                rate: dec!(0.0001),
                settles_at: 3,
            },
        );
        events.extend(engine.settle_funding(3));
        assert_eq!(events.last().unwrap().kind(), "funding");

        let restored = restore(&events).unwrap();
        assert_eq!(restored.view(id).unwrap().collateral, dec!(29_994));
    }
}
