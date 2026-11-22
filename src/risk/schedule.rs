//! Maintenance margin schedules.
//!
//! A schedule is data, not code. Each market has a table of notional
//! brackets, and the maintenance margin for a position is
//! `notional * rate - deduction` using the bracket its notional falls in.

use std::collections::BTreeMap;

use rust_decimal::Decimal;
use thiserror::Error;

use super::num::{add, div, mul, sub};
use super::{MarketId, RiskError};

/// One notional bracket. It applies to positions whose notional is above
/// `lower_bound`, up to the next tier's `lower_bound`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tier {
    pub lower_bound: Decimal,
    pub rate: Decimal,
    pub deduction: Decimal,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ScheduleError {
    #[error("a tier table needs at least one tier")]
    Empty,
    #[error("the first tier must start at zero notional, got {0}")]
    FirstBoundNotZero(Decimal),
    #[error("tier lower bounds must be strictly increasing")]
    BoundsNotIncreasing,
    #[error("maintenance rate {0} must be above 0 and below 1")]
    RateOutOfRange(Decimal),
    #[error("deduction {0} must not be negative")]
    NegativeDeduction(Decimal),
    #[error("the tier starting at {0} would require negative margin")]
    NegativeMaintenance(Decimal),
    #[error("max leverage {0} must be at least 1")]
    LeverageTooLow(Decimal),
    #[error("max leverage must not rise as notional grows")]
    LeverageIncreasing,
    #[error("tier values overflow")]
    Overflow,
}

/// The validated tier table for one market.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierTable {
    tiers: Vec<Tier>,
}

impl TierTable {
    /// Builds a table from explicit tiers, for venues that publish the rate
    /// and deduction of every bracket.
    pub fn new(tiers: Vec<Tier>) -> Result<Self, ScheduleError> {
        let first = tiers.first().ok_or(ScheduleError::Empty)?;
        if !first.lower_bound.is_zero() {
            return Err(ScheduleError::FirstBoundNotZero(first.lower_bound));
        }
        if tiers
            .windows(2)
            .any(|w| w[0].lower_bound >= w[1].lower_bound)
        {
            return Err(ScheduleError::BoundsNotIncreasing);
        }
        for tier in &tiers {
            if tier.rate <= Decimal::ZERO || tier.rate >= Decimal::ONE {
                return Err(ScheduleError::RateOutOfRange(tier.rate));
            }
            if tier.deduction < Decimal::ZERO {
                return Err(ScheduleError::NegativeDeduction(tier.deduction));
            }
            // Margin grows with notional inside a tier, so checking the
            // tier's lower bound covers the whole tier.
            let at_bound = mul(tier.lower_bound, tier.rate).map_err(|_| ScheduleError::Overflow)?;
            if at_bound < tier.deduction {
                return Err(ScheduleError::NegativeMaintenance(tier.lower_bound));
            }
        }
        Ok(Self { tiers })
    }

    /// A single rate at every notional.
    pub fn flat(rate: Decimal) -> Result<Self, ScheduleError> {
        Self::new(vec![Tier {
            lower_bound: Decimal::ZERO,
            rate,
            deduction: Decimal::ZERO,
        }])
    }

    /// Builds a table from `(lower_bound, max_leverage)` brackets using
    /// Hyperliquid's documented rule: the maintenance rate is half the
    /// initial margin rate at the bracket's max leverage, and each deduction
    /// is chosen so the margin does not jump at a bracket boundary.
    pub fn from_max_leverage(brackets: &[(Decimal, Decimal)]) -> Result<Self, ScheduleError> {
        let overflow = |_: RiskError| ScheduleError::Overflow;
        let mut tiers: Vec<Tier> = Vec::with_capacity(brackets.len());
        for &(lower_bound, max_leverage) in brackets {
            if max_leverage < Decimal::ONE {
                return Err(ScheduleError::LeverageTooLow(max_leverage));
            }
            let rate = div(
                Decimal::ONE,
                mul(Decimal::TWO, max_leverage).map_err(overflow)?,
            )
            .map_err(overflow)?;
            let deduction = match tiers.last() {
                None => Decimal::ZERO,
                Some(prev) => {
                    if rate < prev.rate {
                        return Err(ScheduleError::LeverageIncreasing);
                    }
                    let step = mul(lower_bound, sub(rate, prev.rate).map_err(overflow)?)
                        .map_err(overflow)?;
                    add(prev.deduction, step).map_err(overflow)?
                }
            };
            tiers.push(Tier {
                lower_bound,
                rate,
                deduction,
            });
        }
        Self::new(tiers)
    }

    pub fn tiers(&self) -> &[Tier] {
        &self.tiers
    }

    /// The tier a notional falls in. A notional exactly on a boundary
    /// belongs to the lower tier.
    pub fn tier_for(&self, notional: Decimal) -> &Tier {
        self.tiers
            .iter()
            .rev()
            .find(|tier| notional > tier.lower_bound)
            .unwrap_or(&self.tiers[0])
    }

    /// Maintenance margin required for a position of this notional.
    pub fn maintenance(&self, notional: Decimal) -> Result<Decimal, RiskError> {
        let tier = self.tier_for(notional);
        sub(mul(notional, tier.rate)?, tier.deduction)
    }
}

/// The tier table of every market the engine knows.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Schedule {
    tables: BTreeMap<MarketId, TierTable>,
}

impl Schedule {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, market: MarketId, table: TierTable) -> Option<TierTable> {
        self.tables.insert(market, table)
    }

    pub fn table(&self, market: &MarketId) -> Result<&TierTable, RiskError> {
        self.tables
            .get(market)
            .ok_or_else(|| RiskError::MissingSchedule(market.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::risk::Venue;
    use rust_decimal::dec;

    /// Hyperliquid's mainnet BTC brackets as documented on 2026-11-22.
    fn btc() -> TierTable {
        TierTable::from_max_leverage(&[(dec!(0), dec!(40)), (dec!(150_000_000), dec!(20))]).unwrap()
    }

    #[test]
    fn leverage_brackets_become_rates_and_deductions() {
        assert_eq!(
            btc().tiers(),
            [
                Tier {
                    lower_bound: dec!(0),
                    rate: dec!(0.0125),
                    deduction: dec!(0)
                },
                Tier {
                    lower_bound: dec!(150_000_000),
                    rate: dec!(0.025),
                    deduction: dec!(1_875_000)
                },
            ]
        );
    }

    #[test]
    fn maintenance_uses_the_tier_of_the_notional() {
        let table = btc();
        assert_eq!(table.maintenance(dec!(0)), Ok(dec!(0)));
        assert_eq!(table.maintenance(dec!(60_000)), Ok(dec!(750)));
        assert_eq!(table.maintenance(dec!(300_000_000)), Ok(dec!(5_625_000)));
    }

    #[test]
    fn maintenance_does_not_jump_at_a_boundary() {
        let table = btc();
        let at = table.maintenance(dec!(150_000_000)).unwrap();
        let above = table.maintenance(dec!(150_000_000.01)).unwrap();
        assert_eq!(at, dec!(1_875_000));
        assert_eq!(above - at, dec!(0.00025));
    }

    #[test]
    fn rejects_malformed_tables() {
        let tier = |lower_bound, rate| Tier {
            lower_bound,
            rate,
            deduction: dec!(0),
        };
        assert_eq!(TierTable::new(vec![]), Err(ScheduleError::Empty));
        assert_eq!(
            TierTable::new(vec![tier(dec!(5), dec!(0.01))]),
            Err(ScheduleError::FirstBoundNotZero(dec!(5)))
        );
        assert_eq!(
            TierTable::new(vec![tier(dec!(0), dec!(0.01)), tier(dec!(0), dec!(0.02))]),
            Err(ScheduleError::BoundsNotIncreasing)
        );
        assert_eq!(
            TierTable::new(vec![Tier {
                lower_bound: dec!(0),
                rate: dec!(0.01),
                deduction: dec!(100),
            }]),
            Err(ScheduleError::NegativeMaintenance(dec!(0)))
        );
        assert_eq!(
            TierTable::flat(dec!(1)),
            Err(ScheduleError::RateOutOfRange(dec!(1)))
        );
        assert_eq!(
            TierTable::from_max_leverage(&[(dec!(0), dec!(20)), (dec!(100), dec!(40))]),
            Err(ScheduleError::LeverageIncreasing)
        );
        assert_eq!(
            TierTable::from_max_leverage(&[(dec!(0), dec!(0.5))]),
            Err(ScheduleError::LeverageTooLow(dec!(0.5)))
        );
    }

    #[test]
    fn unknown_market_is_an_error() {
        let market = MarketId::new(Venue::Hyperliquid, "BTC");
        assert_eq!(
            Schedule::new().table(&market),
            Err(RiskError::MissingSchedule(market))
        );
    }
}
