//! Risk engine core.
//!
//! This module is pure: no I/O, no async, no clock. It takes an account, a
//! set of mark prices and a maintenance schedule, and returns numbers. The
//! margin model is gross: every position pays its own maintenance margin, and
//! positions offset each other only through profit and loss in account equity.

use rust_decimal::Decimal;
use thiserror::Error;

pub mod health;
pub mod liquidation;
mod num;
pub mod schedule;
pub mod types;

pub use health::{Health, account_health};
pub use liquidation::{LiquidationPrice, liquidation_price};
pub use schedule::{Schedule, ScheduleError, Tier, TierTable};
pub use types::{Account, MarketId, Marks, Position, PositionError, Side, Venue};

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
    #[error("no open position in {0}")]
    NoPosition(MarketId),
    #[error("arithmetic overflow")]
    Overflow,
}
