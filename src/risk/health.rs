//! Account health: equity against the maintenance margin it must cover.

use rust_decimal::Decimal;

use super::num::{add, sub};
use super::{Account, MarketId, Marks, RiskError, Schedule};

/// An account's risk numbers at one set of mark prices.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Health {
    /// Collateral plus unrealized profit and loss.
    pub equity: Decimal,
    pub unrealized_pnl: Decimal,
    /// Sum of position values. Long and short both count as positive.
    pub notional: Decimal,
    /// Sum of every position's maintenance margin.
    pub maintenance_required: Decimal,
    /// Equity minus maintenance margin. Negative means liquidatable.
    pub margin_available: Decimal,
}

impl Health {
    /// The account can be liquidated when equity is below the maintenance
    /// margin. Equity exactly equal to it is still safe.
    pub fn is_liquidatable(&self) -> bool {
        self.margin_available < Decimal::ZERO
    }
}

/// Looks up a market's mark and rejects one that cannot be a price.
pub(crate) fn mark_for(marks: &Marks, market: &MarketId) -> Result<Decimal, RiskError> {
    let mark = *marks
        .get(market)
        .ok_or_else(|| RiskError::MissingMark(market.clone()))?;
    if mark <= Decimal::ZERO {
        return Err(RiskError::InvalidMark {
            market: market.clone(),
            mark,
        });
    }
    Ok(mark)
}

/// Computes account health.
///
/// Fails if any open position has no mark or no schedule. A number built
/// from a partial set of prices would be wrong, so there is no partial
/// result.
pub fn account_health(
    account: &Account,
    marks: &Marks,
    schedule: &Schedule,
) -> Result<Health, RiskError> {
    let mut unrealized_pnl = Decimal::ZERO;
    let mut notional = Decimal::ZERO;
    let mut maintenance_required = Decimal::ZERO;

    for (market, position) in account.positions() {
        let mark = mark_for(marks, market)?;
        let position_notional = position.notional(mark)?;
        let maintenance = schedule.table(market)?.maintenance(position_notional)?;

        unrealized_pnl = add(unrealized_pnl, position.unrealized_pnl(mark)?)?;
        notional = add(notional, position_notional)?;
        maintenance_required = add(maintenance_required, maintenance)?;
    }

    let equity = add(account.collateral, unrealized_pnl)?;
    Ok(Health {
        equity,
        unrealized_pnl,
        notional,
        maintenance_required,
        margin_available: sub(equity, maintenance_required)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::risk::{Position, Side, TierTable, Venue};
    use rust_decimal::dec;

    fn market(coin: &str) -> MarketId {
        MarketId::new(Venue::Hyperliquid, coin)
    }

    fn schedule() -> Schedule {
        let mut schedule = Schedule::new();
        schedule.insert(market("BTC"), TierTable::flat(dec!(0.0125)).unwrap());
        schedule.insert(market("ETH"), TierTable::flat(dec!(0.02)).unwrap());
        schedule
    }

    fn account() -> Account {
        let mut account = Account::new(dec!(5_000));
        account.set_position(
            market("BTC"),
            Position::new(Side::Long, dec!(1), dec!(60_000)).unwrap(),
        );
        account.set_position(
            market("ETH"),
            Position::new(Side::Short, dec!(10), dec!(3_000)).unwrap(),
        );
        account
    }

    #[test]
    fn empty_account_is_all_collateral() {
        let health =
            account_health(&Account::new(dec!(100)), &Marks::new(), &Schedule::new()).unwrap();
        assert_eq!(health.equity, dec!(100));
        assert_eq!(health.maintenance_required, dec!(0));
        assert_eq!(health.margin_available, dec!(100));
        assert!(!health.is_liquidatable());
    }

    #[test]
    fn sums_pnl_and_maintenance_over_positions() {
        let marks = Marks::from([(market("BTC"), dec!(61_000)), (market("ETH"), dec!(3_100))]);
        let health = account_health(&account(), &marks, &schedule()).unwrap();
        // BTC long: +1,000. ETH short: -1,000.
        assert_eq!(health.unrealized_pnl, dec!(0));
        assert_eq!(health.equity, dec!(5_000));
        assert_eq!(health.notional, dec!(92_000));
        // 61,000 * 1.25% + 31,000 * 2%.
        assert_eq!(health.maintenance_required, dec!(1_382.5));
        assert_eq!(health.margin_available, dec!(3_617.5));
    }

    #[test]
    fn equity_equal_to_maintenance_is_not_liquidatable() {
        let mut account = Account::new(dec!(750));
        account.set_position(
            market("BTC"),
            Position::new(Side::Long, dec!(1), dec!(60_000)).unwrap(),
        );
        let at = Marks::from([(market("BTC"), dec!(60_000))]);
        let below = Marks::from([(market("BTC"), dec!(59_999))]);
        assert!(
            !account_health(&account, &at, &schedule())
                .unwrap()
                .is_liquidatable()
        );
        assert!(
            account_health(&account, &below, &schedule())
                .unwrap()
                .is_liquidatable()
        );
    }

    #[test]
    fn missing_or_invalid_mark_is_an_error() {
        let only_btc = Marks::from([(market("BTC"), dec!(60_000))]);
        assert_eq!(
            account_health(&account(), &only_btc, &schedule()),
            Err(RiskError::MissingMark(market("ETH")))
        );
        let zero = Marks::from([(market("BTC"), dec!(0)), (market("ETH"), dec!(3_000))]);
        assert_eq!(
            account_health(&account(), &zero, &schedule()),
            Err(RiskError::InvalidMark {
                market: market("BTC"),
                mark: dec!(0)
            })
        );
    }

    #[test]
    fn missing_schedule_is_an_error() {
        let marks = Marks::from([(market("BTC"), dec!(60_000)), (market("ETH"), dec!(3_000))]);
        assert_eq!(
            account_health(&account(), &marks, &Schedule::new()),
            Err(RiskError::MissingSchedule(market("BTC")))
        );
    }
}
