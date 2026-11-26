//! Risk engine core.
//!
//! This module is pure: no I/O, no async, no clock. It takes an account, a
//! set of mark prices and a maintenance schedule, and returns numbers. The
//! margin model starts gross: every position pays its own maintenance margin,
//! and positions offset each other only through profit and loss in account
//! equity. With a hedge rate in the schedule, a long and a short in the same
//! coin on different venues are netted and pay that rate instead.
//! Collateral can be dollars or coins. A coin counts at its mark price less
//! a haircut, so the account's equity moves with the price of what it holds.

use rust_decimal::Decimal;
use thiserror::Error;

pub mod health;
pub mod lifecycle;
pub mod liquidation;
pub mod netting;
mod num;
pub mod schedule;
pub mod types;

pub use health::{Health, account_health, asset_value};
pub use lifecycle::{AccountState, ThresholdError, Thresholds, Transition, step};
pub use liquidation::{LiquidationPrice, liquidation_price};
pub use netting::{Requirement, matched_sizes, requirement};
pub use schedule::{
    CollateralRule, MarginTable, Schedule, ScheduleError, SizeTable, SizeTier, Tier, TierTable,
};
pub use types::{
    Account, CASH_ASSET, Closed, MarketId, Marks, Position, PositionError, Side, Venue,
};

/// Why a risk number could not be computed. Each of these means there is no
/// trustworthy answer, so the caller must not fall back to an old one.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum RiskError {
    #[error("no mark price for {0}")]
    MissingMark(MarketId),
    #[error("no maintenance schedule for {0}")]
    MissingSchedule(MarketId),
    #[error("mark price {mark} for {market} is not positive")]
    InvalidMark { market: MarketId, mark: Decimal },
    #[error("{0} is not accepted as collateral")]
    UnsupportedAsset(String),
    #[error("no open position in {0}")]
    NoPosition(MarketId),
    #[error("position size {size} is above the largest size the schedule covers, {max}")]
    AboveSchedule { size: Decimal, max: Decimal },
    #[error("result is too small to represent")]
    Underflow,
    #[error("arithmetic overflow")]
    Overflow,
}
