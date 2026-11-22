//! Accounts, positions and the keys that identify a market.

use std::collections::BTreeMap;
use std::fmt;

use rust_decimal::Decimal;
use thiserror::Error;

use super::RiskError;
use super::num::{mul, sub};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Venue {
    Hyperliquid,
}

impl fmt::Display for Venue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Venue::Hyperliquid => f.write_str("hyperliquid"),
        }
    }
}

/// One market on one venue. The same coin on two venues is two markets.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MarketId {
    pub venue: Venue,
    pub coin: String,
}

impl MarketId {
    pub fn new(venue: Venue, coin: impl Into<String>) -> Self {
        Self {
            venue,
            coin: coin.into(),
        }
    }
}

impl fmt::Display for MarketId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.venue, self.coin)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Long,
    Short,
}

impl Side {
    /// `1` for long, `-1` for short.
    pub fn sign(self) -> Decimal {
        match self {
            Side::Long => Decimal::ONE,
            Side::Short => Decimal::NEGATIVE_ONE,
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PositionError {
    #[error("position size {0} is not positive")]
    NonPositiveSize(Decimal),
    #[error("entry price {0} is not positive")]
    NonPositiveEntry(Decimal),
}

/// An open position. The fields are private so a position with a zero or
/// negative size or entry price cannot exist.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Position {
    side: Side,
    size: Decimal,
    entry_px: Decimal,
}

impl Position {
    pub fn new(side: Side, size: Decimal, entry_px: Decimal) -> Result<Self, PositionError> {
        if size <= Decimal::ZERO {
            return Err(PositionError::NonPositiveSize(size));
        }
        if entry_px <= Decimal::ZERO {
            return Err(PositionError::NonPositiveEntry(entry_px));
        }
        Ok(Self {
            side,
            size,
            entry_px,
        })
    }

    pub fn side(&self) -> Side {
        self.side
    }

    /// Size in units of the coin. Always positive.
    pub fn size(&self) -> Decimal {
        self.size
    }

    pub fn entry_px(&self) -> Decimal {
        self.entry_px
    }

    /// Position value at `mark`. Always positive.
    pub fn notional(&self, mark: Decimal) -> Result<Decimal, RiskError> {
        mul(self.size, mark)
    }

    /// Profit or loss if the position were closed at `mark`.
    pub fn unrealized_pnl(&self, mark: Decimal) -> Result<Decimal, RiskError> {
        let move_px = sub(mark, self.entry_px)?;
        mul(mul(self.size, move_px)?, self.side.sign())
    }
}

/// One collateral balance backing positions on any number of venues.
///
/// Positions live in a `BTreeMap`, so they are always visited in the same
/// order. Results cannot depend on insertion order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Account {
    pub collateral: Decimal,
    positions: BTreeMap<MarketId, Position>,
}

impl Account {
    pub fn new(collateral: Decimal) -> Self {
        Self {
            collateral,
            positions: BTreeMap::new(),
        }
    }

    /// Sets the position for a market and returns the one it replaced.
    pub fn set_position(&mut self, market: MarketId, position: Position) -> Option<Position> {
        self.positions.insert(market, position)
    }

    pub fn remove_position(&mut self, market: &MarketId) -> Option<Position> {
        self.positions.remove(market)
    }

    pub fn position(&self, market: &MarketId) -> Option<&Position> {
        self.positions.get(market)
    }

    pub fn positions(&self) -> impl Iterator<Item = (&MarketId, &Position)> {
        self.positions.iter()
    }
}

/// The current mark price of each market, as published by that market's venue.
pub type Marks = BTreeMap<MarketId, Decimal>;

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::dec;

    #[test]
    fn rejects_non_positive_size_and_entry() {
        assert_eq!(
            Position::new(Side::Long, dec!(0), dec!(100)),
            Err(PositionError::NonPositiveSize(dec!(0)))
        );
        assert_eq!(
            Position::new(Side::Short, dec!(1), dec!(-1)),
            Err(PositionError::NonPositiveEntry(dec!(-1)))
        );
    }

    #[test]
    fn pnl_has_the_sign_of_the_side() {
        let long = Position::new(Side::Long, dec!(2), dec!(100)).unwrap();
        let short = Position::new(Side::Short, dec!(2), dec!(100)).unwrap();
        assert_eq!(long.unrealized_pnl(dec!(110)), Ok(dec!(20)));
        assert_eq!(short.unrealized_pnl(dec!(110)), Ok(dec!(-20)));
        assert_eq!(long.notional(dec!(110)), Ok(dec!(220)));
        assert_eq!(short.notional(dec!(110)), Ok(dec!(220)));
    }

    #[test]
    fn overflow_is_an_error_not_a_panic() {
        let huge = Position::new(Side::Long, Decimal::MAX, dec!(1)).unwrap();
        assert_eq!(huge.notional(dec!(2)), Err(RiskError::Overflow));
    }

    #[test]
    fn market_id_orders_by_venue_then_coin() {
        let btc = MarketId::new(Venue::Hyperliquid, "BTC");
        let eth = MarketId::new(Venue::Hyperliquid, "ETH");
        assert!(btc < eth);
        assert_eq!(btc.to_string(), "hyperliquid:BTC");
    }
}
