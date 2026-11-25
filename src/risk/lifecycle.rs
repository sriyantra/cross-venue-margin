//! Account lifecycle as an explicit state machine.
//!
//! The state is decided by account health, so it lives on the account, not
//! on a position. `step` is the only way to move between states. It takes
//! the time as an argument, so a recorded run replays to the same states.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::num::mul;
use super::{Health, RiskError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum AccountState {
    Open,
    /// Equity is still above maintenance margin but inside the warning band.
    MarginCall {
        since: u64,
    },
    /// Equity is below maintenance margin and positions remain.
    Liquidating {
        since: u64,
    },
    /// Liquidation closed every position. This state is final.
    Closed {
        at: u64,
    },
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ThresholdError {
    #[error("margin call multiple {0} must be above 1")]
    MultipleTooLow(Decimal),
}

/// Where the warning band starts. There is no default: the caller chooses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Thresholds {
    margin_call_multiple: Decimal,
}

impl Thresholds {
    /// A margin call starts when equity falls below
    /// `maintenance_required * margin_call_multiple`. The multiple must be
    /// above 1, or the call would start after liquidation does.
    pub fn new(margin_call_multiple: Decimal) -> Result<Self, ThresholdError> {
        if margin_call_multiple <= Decimal::ONE {
            return Err(ThresholdError::MultipleTooLow(margin_call_multiple));
        }
        Ok(Self {
            margin_call_multiple,
        })
    }

    pub fn margin_call_multiple(&self) -> Decimal {
        self.margin_call_multiple
    }

    /// Whether equity is below the level where a margin call starts.
    pub fn in_margin_call(&self, health: &Health) -> Result<bool, RiskError> {
        let band = mul(health.maintenance_required, self.margin_call_multiple)?;
        Ok(health.equity < band)
    }
}

/// A change of state, for the event log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Transition {
    pub from: AccountState,
    pub to: AccountState,
    pub at: u64,
}

/// Advances the state machine by one health reading.
///
/// Returns the new state, and a `Transition` only when the kind of state
/// changed. Staying in a state keeps its original timestamp.
pub fn step(
    state: AccountState,
    health: &Health,
    has_positions: bool,
    thresholds: &Thresholds,
    now: u64,
) -> Result<(AccountState, Option<Transition>), RiskError> {
    let next = match state {
        AccountState::Closed { .. } => state,
        AccountState::Liquidating { .. } if !has_positions => AccountState::Closed { at: now },
        _ if !has_positions => AccountState::Open,
        _ if health.is_liquidatable() => match state {
            AccountState::Liquidating { .. } => state,
            _ => AccountState::Liquidating { since: now },
        },
        _ if thresholds.in_margin_call(health)? => match state {
            AccountState::MarginCall { .. } => state,
            _ => AccountState::MarginCall { since: now },
        },
        _ => AccountState::Open,
    };

    let transition = (next != state).then_some(Transition {
        from: state,
        to: next,
        at: now,
    });
    Ok((next, transition))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::dec;

    fn health(equity: Decimal, maintenance_required: Decimal) -> Health {
        Health {
            equity,
            collateral_value: dec!(0),
            unrealized_pnl: dec!(0),
            notional: dec!(0),
            maintenance_required,
            margin_available: equity - maintenance_required,
        }
    }

    fn thresholds() -> Thresholds {
        Thresholds::new(dec!(1.5)).unwrap()
    }

    fn next(state: AccountState, equity: Decimal, now: u64) -> AccountState {
        step(state, &health(equity, dec!(100)), true, &thresholds(), now)
            .unwrap()
            .0
    }

    #[test]
    fn multiple_must_be_above_one() {
        assert_eq!(
            Thresholds::new(dec!(1)),
            Err(ThresholdError::MultipleTooLow(dec!(1)))
        );
    }

    #[test]
    fn walks_down_through_the_bands() {
        // Maintenance is 100, so the margin call band is 100 to 150.
        assert_eq!(next(AccountState::Open, dec!(150), 1), AccountState::Open);
        assert_eq!(
            next(AccountState::Open, dec!(149.99), 2),
            AccountState::MarginCall { since: 2 }
        );
        assert_eq!(
            next(AccountState::MarginCall { since: 2 }, dec!(100), 3),
            AccountState::MarginCall { since: 2 }
        );
        assert_eq!(
            next(AccountState::MarginCall { since: 2 }, dec!(99.99), 4),
            AccountState::Liquidating { since: 4 }
        );
    }

    #[test]
    fn open_can_go_straight_to_liquidating() {
        assert_eq!(
            next(AccountState::Open, dec!(50), 7),
            AccountState::Liquidating { since: 7 }
        );
    }

    #[test]
    fn recovers_when_equity_returns() {
        assert_eq!(
            next(AccountState::MarginCall { since: 2 }, dec!(200), 5),
            AccountState::Open
        );
        assert_eq!(
            next(AccountState::Liquidating { since: 4 }, dec!(120), 6),
            AccountState::MarginCall { since: 6 }
        );
    }

    #[test]
    fn liquidating_with_no_positions_left_is_closed_for_good() {
        let flat = health(dec!(10), dec!(0));
        let (closed, transition) = step(
            AccountState::Liquidating { since: 4 },
            &flat,
            false,
            &thresholds(),
            9,
        )
        .unwrap();
        assert_eq!(closed, AccountState::Closed { at: 9 });
        assert_eq!(
            transition,
            Some(Transition {
                from: AccountState::Liquidating { since: 4 },
                to: closed,
                at: 9
            })
        );

        let healthy = health(dec!(1_000), dec!(100));
        assert_eq!(
            step(closed, &healthy, true, &thresholds(), 10).unwrap(),
            (closed, None)
        );
    }

    #[test]
    fn transition_is_reported_only_on_a_change() {
        let reading = health(dec!(120), dec!(100));
        let (state, first) = step(AccountState::Open, &reading, true, &thresholds(), 1).unwrap();
        let (again, second) = step(state, &reading, true, &thresholds(), 2).unwrap();
        assert!(first.is_some());
        assert_eq!(again, state);
        assert_eq!(second, None);
    }

    #[test]
    fn account_without_positions_stays_open() {
        let broke = health(dec!(-5), dec!(0));
        assert_eq!(
            step(AccountState::Open, &broke, false, &thresholds(), 1).unwrap(),
            (AccountState::Open, None)
        );
    }
}
