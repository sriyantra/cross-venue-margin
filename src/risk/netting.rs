//! Netting of hedged positions across venues.
//!
//! A long in a coin on one venue and a short in the same coin on another
//! mostly cancel: what one loses on a price move the other gains. With a
//! hedge rate set in the schedule, the matched part of both legs pays that
//! rate instead of each venue's full requirement. What is left to lose on a
//! matched pair is the gap between the two venues' marks, and the hedge
//! rate is the charge for that gap. The unmatched part of a position pays
//! its venue's requirement as before.
//!
//! Without a hedge rate nothing is matched and margin is gross.

use std::collections::BTreeMap;

use rust_decimal::Decimal;

use super::num::{add, mul, sub};
use super::{Account, MarketId, Position, RiskError, Schedule, Side};

/// Margin required for one position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Requirement {
    /// Below this much equity the account is liquidated.
    pub maintenance: Decimal,
    /// A fill that grows a position needs this much equity.
    pub initial: Decimal,
}

/// How much of each position is matched by positions on the other side in
/// the same coin. Positions that are not matched at all are left out.
///
/// The smaller side of a coin is matched in full. The larger side is
/// matched in market order until the smaller side is used up.
pub fn matched_sizes(
    account: &Account,
    schedule: &Schedule,
) -> Result<BTreeMap<MarketId, Decimal>, RiskError> {
    let mut matched = BTreeMap::new();
    if schedule.hedge_rate().is_none() {
        return Ok(matched);
    }

    // Long and short size per coin.
    let mut totals: BTreeMap<&str, (Decimal, Decimal)> = BTreeMap::new();
    for (market, position) in account.positions() {
        let (long, short) = totals.entry(market.coin.as_str()).or_default();
        match position.side() {
            Side::Long => *long = add(*long, position.size())?,
            Side::Short => *short = add(*short, position.size())?,
        }
    }

    // What is still to be matched on each side: the smaller total on both.
    for (long, short) in totals.values_mut() {
        let pair = (*long).min(*short);
        (*long, *short) = (pair, pair);
    }
    for (market, position) in account.positions() {
        let Some((long, short)) = totals.get_mut(market.coin.as_str()) else {
            continue;
        };
        let left = match position.side() {
            Side::Long => long,
            Side::Short => short,
        };
        let take = position.size().min(*left);
        if take > Decimal::ZERO {
            *left = sub(*left, take)?;
            matched.insert(market.clone(), take);
        }
    }
    Ok(matched)
}

/// The margin one position needs at `mark`, given how much of it is matched.
pub fn requirement(
    schedule: &Schedule,
    market: &MarketId,
    position: &Position,
    matched: Decimal,
    mark: Decimal,
) -> Result<Requirement, RiskError> {
    let table = schedule.table(market)?;
    let size = position.size();
    let notional = position.notional(mark)?;
    // Computed even when it is replaced below, so a position the table does
    // not cover is refused whether or not it is hedged.
    let gross = Requirement {
        maintenance: table.maintenance(size, notional)?,
        initial: table.initial(size, notional)?,
    };
    let Some(rate) = schedule.hedge_rate().filter(|_| matched > Decimal::ZERO) else {
        return Ok(gross);
    };

    let hedged = mul(mul(matched, mark)?, rate)?;
    let outright = sub(size, matched)?;
    let mut net = Requirement {
        maintenance: hedged,
        initial: mul(hedged, Decimal::TWO)?,
    };
    if outright > Decimal::ZERO {
        let outright_notional = mul(outright, mark)?;
        net.maintenance = add(
            net.maintenance,
            table.maintenance(outright, outright_notional)?,
        )?;
        net.initial = add(net.initial, table.initial(outright, outright_notional)?)?;
    }
    Ok(net)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::risk::{SizeTable, TierTable, Venue};
    use rust_decimal::dec;

    fn hl() -> MarketId {
        MarketId::new(Venue::Hyperliquid, "BTC")
    }

    fn okx() -> MarketId {
        MarketId::new(Venue::Okx, "BTC")
    }

    fn schedule(hedge_rate: Option<Decimal>) -> Schedule {
        let mut schedule = Schedule::new();
        schedule.insert(hl(), TierTable::flat(dec!(0.0125)).unwrap());
        schedule.insert(
            okx(),
            SizeTable::new(&[(dec!(10), dec!(0.004)), (dec!(100), dec!(0.01))]).unwrap(),
        );
        schedule.insert(
            MarketId::new(Venue::Hyperliquid, "ETH"),
            TierTable::flat(dec!(0.02)).unwrap(),
        );
        schedule.set_hedge_rate(hedge_rate).unwrap();
        schedule
    }

    fn account(long: Decimal, short: Decimal) -> Account {
        let mut account = Account::new(dec!(0));
        account.set_position(hl(), Position::new(Side::Long, long, dec!(60_000)).unwrap());
        account.set_position(
            okx(),
            Position::new(Side::Short, short, dec!(60_000)).unwrap(),
        );
        account
    }

    #[test]
    fn nothing_is_matched_without_a_hedge_rate() {
        let schedule = schedule(None);
        assert!(
            matched_sizes(&account(dec!(1), dec!(1)), &schedule)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn the_smaller_side_is_matched_in_full() {
        let schedule = schedule(Some(dec!(0.0025)));
        let matched = matched_sizes(&account(dec!(3), dec!(1)), &schedule).unwrap();
        assert_eq!(matched, BTreeMap::from([(hl(), dec!(1)), (okx(), dec!(1))]));
    }

    #[test]
    fn positions_on_the_same_side_or_in_other_coins_are_not_matched() {
        let schedule = schedule(Some(dec!(0.0025)));
        let mut account = Account::new(dec!(0));
        account.set_position(
            hl(),
            Position::new(Side::Long, dec!(1), dec!(60_000)).unwrap(),
        );
        account.set_position(
            okx(),
            Position::new(Side::Long, dec!(1), dec!(60_000)).unwrap(),
        );
        account.set_position(
            MarketId::new(Venue::Hyperliquid, "ETH"),
            Position::new(Side::Short, dec!(20), dec!(3_000)).unwrap(),
        );
        assert!(matched_sizes(&account, &schedule).unwrap().is_empty());
    }

    #[test]
    fn matched_size_pays_the_hedge_rate_and_the_rest_pays_the_table() {
        let schedule = schedule(Some(dec!(0.0025)));
        let long = Position::new(Side::Long, dec!(3), dec!(60_000)).unwrap();

        let gross = requirement(&schedule, &hl(), &long, dec!(0), dec!(60_000)).unwrap();
        assert_eq!(gross.maintenance, dec!(2_250));
        assert_eq!(gross.initial, dec!(4_500));

        // 1 matched: 60,000 at 0.25%. 2 outright: 120,000 at 1.25%.
        let net = requirement(&schedule, &hl(), &long, dec!(1), dec!(60_000)).unwrap();
        assert_eq!(net.maintenance, dec!(150) + dec!(1_500));
        assert_eq!(net.initial, dec!(300) + dec!(3_000));

        let full = requirement(&schedule, &hl(), &long, dec!(3), dec!(60_000)).unwrap();
        assert_eq!(full.maintenance, dec!(450));
    }

    #[test]
    fn a_hedged_position_past_the_table_is_still_refused() {
        let schedule = schedule(Some(dec!(0.0025)));
        let short = Position::new(Side::Short, dec!(101), dec!(60_000)).unwrap();
        assert_eq!(
            requirement(&schedule, &okx(), &short, dec!(101), dec!(60_000)),
            Err(RiskError::AboveSchedule {
                size: dec!(101),
                max: dec!(100)
            })
        );
    }
}
