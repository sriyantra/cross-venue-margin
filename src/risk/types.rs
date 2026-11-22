//! Accounts, positions and the keys that identify a market.

use std::collections::BTreeMap;
use std::fmt;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::RiskError;
use super::health::mark_for;
use super::num::{add, div, mul, sub};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Venue {
    Hyperliquid,
    Okx,
}

impl fmt::Display for Venue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Venue::Hyperliquid => f.write_str("hyperliquid"),
            Venue::Okx => f.write_str("okx"),
        }
    }
}

/// One market on one venue. The same coin on two venues is two markets.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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
///
/// A fill has the same shape (a side, a size and a price), so fills are
/// passed around as `Position` values too.
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

    pub fn has_positions(&self) -> bool {
        !self.positions.is_empty()
    }

    /// Applies a fill and returns the profit or loss it realized.
    ///
    /// A fill on the same side grows the position and averages the entry
    /// price. A fill on the other side closes that much of the position at
    /// the fill price, moves the result into collateral, and opens whatever
    /// is left over as a new position on the fill's side. On an error the
    /// account is unchanged.
    pub fn apply_fill(&mut self, market: MarketId, fill: Position) -> Result<Decimal, RiskError> {
        let Some(held) = self.positions.get(&market).copied() else {
            self.positions.insert(market, fill);
            return Ok(Decimal::ZERO);
        };

        if held.side == fill.side {
            let size = add(held.size, fill.size)?;
            let cost = add(
                mul(held.size, held.entry_px)?,
                mul(fill.size, fill.entry_px)?,
            )?;
            let entry_px = div(cost, size)?;
            // Sizes near the limit of the decimal type can round the cost
            // to zero. A position with no entry price must not exist.
            if entry_px <= Decimal::ZERO {
                return Err(RiskError::Underflow);
            }
            let grown = Position {
                side: held.side,
                size,
                entry_px,
            };
            self.positions.insert(market, grown);
            return Ok(Decimal::ZERO);
        }

        let closed = held.size.min(fill.size);
        let realized = Position {
            size: closed,
            ..held
        }
        .unrealized_pnl(fill.entry_px)?;
        let collateral = add(self.collateral, realized)?;
        let remaining = sub(held.size, fill.size)?;
        if remaining > Decimal::ZERO {
            let reduced = Position {
                size: remaining,
                ..held
            };
            self.positions.insert(market, reduced);
        } else if remaining < Decimal::ZERO {
            let flipped = Position {
                size: -remaining,
                ..fill
            };
            self.positions.insert(market, flipped);
        } else {
            self.positions.remove(&market);
        }
        self.collateral = collateral;
        Ok(realized)
    }

    /// Closes every position at its market's mark and moves the result into
    /// collateral. On an error the account is unchanged.
    pub fn close_all(&mut self, marks: &Marks) -> Result<Vec<Closed>, RiskError> {
        let mut closed = Vec::with_capacity(self.positions.len());
        let mut collateral = self.collateral;
        for (market, position) in &self.positions {
            let px = mark_for(marks, market)?;
            let realized_pnl = position.unrealized_pnl(px)?;
            collateral = add(collateral, realized_pnl)?;
            closed.push(Closed {
                market: market.clone(),
                position: *position,
                px,
                realized_pnl,
            });
        }
        self.positions.clear();
        self.collateral = collateral;
        Ok(closed)
    }
}

/// One position closed by `Account::close_all`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Closed {
    pub market: MarketId,
    pub position: Position,
    pub px: Decimal,
    pub realized_pnl: Decimal,
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

    fn btc() -> MarketId {
        MarketId::new(Venue::Hyperliquid, "BTC")
    }

    fn pos(side: Side, size: Decimal, px: Decimal) -> Position {
        Position::new(side, size, px).unwrap()
    }

    #[test]
    fn same_side_fill_averages_the_entry() {
        let mut account = Account::new(dec!(1_000));
        account
            .apply_fill(btc(), pos(Side::Long, dec!(1), dec!(100)))
            .unwrap();
        let realized = account
            .apply_fill(btc(), pos(Side::Long, dec!(3), dec!(120)))
            .unwrap();
        assert_eq!(realized, dec!(0));
        assert_eq!(
            account.position(&btc()),
            Some(&pos(Side::Long, dec!(4), dec!(115)))
        );
        assert_eq!(account.collateral, dec!(1_000));
    }

    #[test]
    fn fill_too_small_to_price_is_an_error() {
        let dust = Decimal::new(1, 28);
        let mut account = Account::new(dec!(1_000));
        account
            .apply_fill(btc(), pos(Side::Long, dust, dec!(0.5)))
            .unwrap();
        let untouched = account.clone();
        assert_eq!(
            account.apply_fill(btc(), pos(Side::Long, dust, dec!(0.5))),
            Err(RiskError::Underflow)
        );
        assert_eq!(account, untouched);
    }

    #[test]
    fn opposite_fill_reduces_and_realizes() {
        let mut account = Account::new(dec!(1_000));
        account
            .apply_fill(btc(), pos(Side::Long, dec!(4), dec!(100)))
            .unwrap();
        let realized = account
            .apply_fill(btc(), pos(Side::Short, dec!(1), dec!(90)))
            .unwrap();
        assert_eq!(realized, dec!(-10));
        assert_eq!(account.collateral, dec!(990));
        assert_eq!(
            account.position(&btc()),
            Some(&pos(Side::Long, dec!(3), dec!(100)))
        );
    }

    #[test]
    fn opposite_fill_of_equal_size_closes() {
        let mut account = Account::new(dec!(1_000));
        account
            .apply_fill(btc(), pos(Side::Short, dec!(2), dec!(100)))
            .unwrap();
        let realized = account
            .apply_fill(btc(), pos(Side::Long, dec!(2), dec!(95)))
            .unwrap();
        assert_eq!(realized, dec!(10));
        assert!(!account.has_positions());
    }

    #[test]
    fn larger_opposite_fill_flips_the_position() {
        let mut account = Account::new(dec!(1_000));
        account
            .apply_fill(btc(), pos(Side::Long, dec!(1), dec!(100)))
            .unwrap();
        let realized = account
            .apply_fill(btc(), pos(Side::Short, dec!(3), dec!(110)))
            .unwrap();
        assert_eq!(realized, dec!(10));
        assert_eq!(account.collateral, dec!(1_010));
        assert_eq!(
            account.position(&btc()),
            Some(&pos(Side::Short, dec!(2), dec!(110)))
        );
    }

    #[test]
    fn close_all_realizes_every_position_or_none() {
        let eth = MarketId::new(Venue::Hyperliquid, "ETH");
        let mut account = Account::new(dec!(1_000));
        account.set_position(btc(), pos(Side::Long, dec!(1), dec!(100)));
        account.set_position(eth.clone(), pos(Side::Short, dec!(2), dec!(50)));

        let untouched = account.clone();
        let only_btc = Marks::from([(btc(), dec!(90))]);
        assert_eq!(
            account.close_all(&only_btc),
            Err(RiskError::MissingMark(eth.clone()))
        );
        assert_eq!(account, untouched);

        let marks = Marks::from([(btc(), dec!(90)), (eth, dec!(40))]);
        let closed = account.close_all(&marks).unwrap();
        assert_eq!(closed.len(), 2);
        assert_eq!(account.collateral, dec!(1_010));
        assert!(!account.has_positions());
    }

    #[test]
    fn market_id_orders_by_venue_then_coin() {
        let btc = MarketId::new(Venue::Hyperliquid, "BTC");
        let eth = MarketId::new(Venue::Hyperliquid, "ETH");
        assert!(btc < eth);
        assert_eq!(btc.to_string(), "hyperliquid:BTC");
    }
}
