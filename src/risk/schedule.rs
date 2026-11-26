//! Maintenance margin schedules.
//!
//! A schedule is data, not code. Each market has one table, and venues
//! differ in what kind:
//!
//! - `TierTable` is keyed by notional. The margin is
//!   `notional * rate - deduction` using the bracket the notional falls in,
//!   so the bracket changes as the price moves.
//! - `SizeTable` is keyed by position size. The margin is `notional * rate`
//!   with the rate of the bracket the size falls in, applied to the whole
//!   position. The bracket only changes when the position does.

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
    #[error("maintenance rate must not fall as size grows")]
    RateDecreasing,
    #[error("size bound {0} must be above zero")]
    NonPositiveBound(Decimal),
    #[error("tier values overflow")]
    Overflow,
    #[error("initial margin rate {0} must be at least the maintenance rate and at most 1")]
    InitialRateOutOfRange(Decimal),
    #[error("haircut {0} must be at least 0 and below 1")]
    HaircutOutOfRange(Decimal),
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

    /// Initial margin for a position of this notional at the bracket's max
    /// leverage. The maintenance rate is half the initial rate, so this is
    /// the notional at twice the bracket's maintenance rate.
    pub fn initial(&self, notional: Decimal) -> Result<Decimal, RiskError> {
        let tier = self.tier_for(notional);
        mul(notional, mul(tier.rate, Decimal::TWO)?)
    }
}

/// One size bracket: positions up to and including `max_size` pay `rate`
/// on their whole notional.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SizeTier {
    pub max_size: Decimal,
    pub rate: Decimal,
    /// Initial margin rate: one over the bracket's max leverage.
    pub initial_rate: Decimal,
}

/// A validated table keyed by position size, in units of the coin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SizeTable {
    tiers: Vec<SizeTier>,
}

impl SizeTable {
    /// Builds a table from `(max_size, rate)` brackets in increasing order.
    /// The last bracket's `max_size` is the largest position the table
    /// covers. The initial margin rate of each bracket is set to twice its
    /// maintenance rate.
    pub fn new(brackets: &[(Decimal, Decimal)]) -> Result<Self, ScheduleError> {
        let with_initial: Vec<(Decimal, Decimal, Decimal)> = brackets
            .iter()
            .map(|&(max_size, rate)| (max_size, rate, rate + rate))
            .collect();
        Self::with_initial_rates(&with_initial)
    }

    /// Builds a table from `(max_size, rate, initial_rate)` brackets, for a
    /// venue that publishes both rates.
    pub fn with_initial_rates(
        brackets: &[(Decimal, Decimal, Decimal)],
    ) -> Result<Self, ScheduleError> {
        let mut tiers: Vec<SizeTier> = Vec::with_capacity(brackets.len());
        for &(max_size, rate, initial_rate) in brackets {
            if max_size <= Decimal::ZERO {
                return Err(ScheduleError::NonPositiveBound(max_size));
            }
            if rate <= Decimal::ZERO || rate >= Decimal::ONE {
                return Err(ScheduleError::RateOutOfRange(rate));
            }
            if initial_rate < rate || initial_rate > Decimal::ONE {
                return Err(ScheduleError::InitialRateOutOfRange(initial_rate));
            }
            if let Some(prev) = tiers.last() {
                if max_size <= prev.max_size {
                    return Err(ScheduleError::BoundsNotIncreasing);
                }
                if rate < prev.rate {
                    return Err(ScheduleError::RateDecreasing);
                }
            }
            tiers.push(SizeTier {
                max_size,
                rate,
                initial_rate,
            });
        }
        if tiers.is_empty() {
            return Err(ScheduleError::Empty);
        }
        Ok(Self { tiers })
    }

    pub fn tiers(&self) -> &[SizeTier] {
        &self.tiers
    }

    /// The rate for a position of this size. A size exactly on a bound
    /// belongs to the lower bracket. A size past the last bracket has no
    /// rate.
    pub fn rate_for(&self, size: Decimal) -> Result<Decimal, RiskError> {
        self.tier_for(size).map(|tier| tier.rate)
    }

    /// The initial margin rate for a position of this size.
    pub fn initial_rate_for(&self, size: Decimal) -> Result<Decimal, RiskError> {
        self.tier_for(size).map(|tier| tier.initial_rate)
    }

    fn tier_for(&self, size: Decimal) -> Result<&SizeTier, RiskError> {
        self.tiers
            .iter()
            .find(|tier| size <= tier.max_size)
            .ok_or_else(|| RiskError::AboveSchedule {
                size,
                max: self.tiers[self.tiers.len() - 1].max_size,
            })
    }
}

/// A market's maintenance table, in whichever form its venue defines it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarginTable {
    ByNotional(TierTable),
    BySize(SizeTable),
}

impl MarginTable {
    /// Maintenance margin for a position of this size and notional.
    pub fn maintenance(&self, size: Decimal, notional: Decimal) -> Result<Decimal, RiskError> {
        match self {
            MarginTable::ByNotional(table) => table.maintenance(notional),
            MarginTable::BySize(table) => mul(notional, table.rate_for(size)?),
        }
    }

    /// Initial margin for a position of this size and notional at the
    /// venue's max leverage for it.
    pub fn initial(&self, size: Decimal, notional: Decimal) -> Result<Decimal, RiskError> {
        match self {
            MarginTable::ByNotional(table) => table.initial(notional),
            MarginTable::BySize(table) => mul(notional, table.initial_rate_for(size)?),
        }
    }
}

impl From<TierTable> for MarginTable {
    fn from(table: TierTable) -> Self {
        MarginTable::ByNotional(table)
    }
}

impl From<SizeTable> for MarginTable {
    fn from(table: SizeTable) -> Self {
        MarginTable::BySize(table)
    }
}

/// How one collateral asset counts toward equity.
///
/// The asset is valued at the mark of `price`, and only `1 - haircut` of
/// that value counts. The haircut is the cushion against the asset losing
/// value before it could be sold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollateralRule {
    price: MarketId,
    haircut: Decimal,
}

impl CollateralRule {
    pub fn new(price: MarketId, haircut: Decimal) -> Result<Self, ScheduleError> {
        if haircut < Decimal::ZERO || haircut >= Decimal::ONE {
            return Err(ScheduleError::HaircutOutOfRange(haircut));
        }
        Ok(Self { price, haircut })
    }

    /// The market whose mark prices the asset.
    pub fn price(&self) -> &MarketId {
        &self.price
    }

    pub fn haircut(&self) -> Decimal {
        self.haircut
    }

    /// The share of the asset's value that counts: `1 - haircut`.
    pub fn weight(&self) -> Decimal {
        Decimal::ONE - self.haircut
    }
}

/// The maintenance table of every market the engine knows, and the rule
/// for every asset it accepts as collateral.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Schedule {
    tables: BTreeMap<MarketId, MarginTable>,
    collateral: BTreeMap<String, CollateralRule>,
    hedge_rate: Option<Decimal>,
}

impl Schedule {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(
        &mut self,
        market: MarketId,
        table: impl Into<MarginTable>,
    ) -> Option<MarginTable> {
        self.tables.insert(market, table.into())
    }

    pub fn table(&self, market: &MarketId) -> Result<&MarginTable, RiskError> {
        self.tables
            .get(market)
            .ok_or_else(|| RiskError::MissingSchedule(market.clone()))
    }

    /// Sets the maintenance rate for the matched part of a long and a short
    /// in the same coin on different venues. `None` turns netting off.
    pub fn set_hedge_rate(&mut self, rate: Option<Decimal>) -> Result<(), ScheduleError> {
        if let Some(rate) = rate
            && (rate <= Decimal::ZERO || rate >= Decimal::ONE)
        {
            return Err(ScheduleError::RateOutOfRange(rate));
        }
        self.hedge_rate = rate;
        Ok(())
    }

    pub fn hedge_rate(&self) -> Option<Decimal> {
        self.hedge_rate
    }

    pub fn insert_collateral(
        &mut self,
        asset: impl Into<String>,
        rule: CollateralRule,
    ) -> Option<CollateralRule> {
        self.collateral.insert(asset.into(), rule)
    }

    pub fn collateral(&self, asset: &str) -> Result<&CollateralRule, RiskError> {
        self.collateral
            .get(asset)
            .ok_or_else(|| RiskError::UnsupportedAsset(asset.to_string()))
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

    fn by_size() -> SizeTable {
        SizeTable::new(&[(dec!(10), dec!(0.004)), (dec!(50), dec!(0.005))]).unwrap()
    }

    #[test]
    fn size_table_applies_one_rate_to_the_whole_position() {
        let table = MarginTable::from(by_size());
        // 10 coins at 60,000 is still in the first bracket.
        assert_eq!(table.maintenance(dec!(10), dec!(600_000)), Ok(dec!(2_400)));
        // One step over, the whole position pays the second rate.
        assert_eq!(
            table.maintenance(dec!(10.01), dec!(600_600)),
            Ok(dec!(3_003))
        );
        // The bracket follows size, not notional.
        assert_eq!(
            table.maintenance(dec!(1), dec!(9_000_000)),
            Ok(dec!(36_000))
        );
    }

    #[test]
    fn size_past_the_last_bracket_has_no_rate() {
        assert_eq!(by_size().rate_for(dec!(50)), Ok(dec!(0.005)));
        assert_eq!(
            by_size().rate_for(dec!(50.01)),
            Err(RiskError::AboveSchedule {
                size: dec!(50.01),
                max: dec!(50)
            })
        );
    }

    #[test]
    fn invalid_size_tables_are_rejected() {
        assert_eq!(SizeTable::new(&[]), Err(ScheduleError::Empty));
        assert_eq!(
            SizeTable::new(&[(dec!(0), dec!(0.01))]),
            Err(ScheduleError::NonPositiveBound(dec!(0)))
        );
        assert_eq!(
            SizeTable::new(&[(dec!(10), dec!(0.01)), (dec!(10), dec!(0.02))]),
            Err(ScheduleError::BoundsNotIncreasing)
        );
        assert_eq!(
            SizeTable::new(&[(dec!(10), dec!(0.02)), (dec!(20), dec!(0.01))]),
            Err(ScheduleError::RateDecreasing)
        );
        assert_eq!(
            SizeTable::new(&[(dec!(10), dec!(1))]),
            Err(ScheduleError::RateOutOfRange(dec!(1)))
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
