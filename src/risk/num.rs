//! Checked decimal arithmetic. `Decimal`'s operators panic on overflow, and a
//! risk engine must report a bad input instead of crashing on it.

use rust_decimal::Decimal;

use super::RiskError;

pub(crate) fn add(a: Decimal, b: Decimal) -> Result<Decimal, RiskError> {
    a.checked_add(b).ok_or(RiskError::Overflow)
}

pub(crate) fn sub(a: Decimal, b: Decimal) -> Result<Decimal, RiskError> {
    a.checked_sub(b).ok_or(RiskError::Overflow)
}

pub(crate) fn mul(a: Decimal, b: Decimal) -> Result<Decimal, RiskError> {
    a.checked_mul(b).ok_or(RiskError::Overflow)
}

/// Fails on overflow and on division by zero.
pub(crate) fn div(a: Decimal, b: Decimal) -> Result<Decimal, RiskError> {
    a.checked_div(b).ok_or(RiskError::Overflow)
}
