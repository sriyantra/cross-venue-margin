//! Liquidation price of one position inside a cross-margined account.

use rust_decimal::Decimal;
use serde::Serialize;

use super::health::mark_for;
use super::num::{add, div, mul, sub};
use super::{Account, MarketId, Marks, RiskError, Schedule, Side, Tier};

/// Where one market's mark must go for the account to reach its maintenance
/// margin, with every other mark held where it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "type", content = "price", rename_all = "snake_case")]
pub enum LiquidationPrice {
    /// Margin available is zero at this price and negative beyond it: below
    /// it for a long, above it for a short.
    At(Decimal),
    /// No price of this market can make the account liquidatable. Only a
    /// long can be in this state, because its loss stops at a price of zero.
    Never,
    /// The account is liquidatable at every price of this market. Only a
    /// short can be in this state.
    AtAnyPrice,
}

/// Solves `equity(px) = maintenance(px)` for the mark of `market`.
///
/// In one tier the margin available is a straight line in the price:
/// `base + deduction + px * size * (sign - rate)`. The rate and deduction
/// change from tier to tier, so the answer is found by walking the tiers in
/// the direction the price has to move.
pub fn liquidation_price(
    account: &Account,
    market: &MarketId,
    marks: &Marks,
    schedule: &Schedule,
) -> Result<LiquidationPrice, RiskError> {
    let position = account
        .position(market)
        .ok_or_else(|| RiskError::NoPosition(market.clone()))?;
    let tiers = schedule.table(market)?.tiers();

    // Everything that does not move with this market's price: collateral,
    // the other positions' PnL and maintenance, and this position's cost.
    let mut base = account.collateral;
    for (other, held) in account.positions() {
        if other == market {
            continue;
        }
        let mark = mark_for(marks, other)?;
        let maintenance = schedule.table(other)?.maintenance(held.notional(mark)?)?;
        base = sub(add(base, held.unrealized_pnl(mark)?)?, maintenance)?;
    }
    let size = position.size();
    let sign = position.side().sign();
    base = sub(base, mul(mul(sign, size)?, position.entry_px())?)?;

    let slope = |tier: &Tier| mul(size, sub(sign, tier.rate)?);
    let margin_at = |tier: &Tier, px: Decimal| -> Result<Decimal, RiskError> {
        add(add(base, tier.deduction)?, mul(px, slope(tier)?)?)
    };
    let root = |tier: &Tier| -> Result<LiquidationPrice, RiskError> {
        let px = div(-add(base, tier.deduction)?, slope(tier)?)?;
        Ok(LiquidationPrice::At(px))
    };
    // The price at which this position's notional reaches a tier's bound.
    let bound_px = |tier: &Tier| div(tier.lower_bound, size);

    match position.side() {
        // Margin rises with the price inside every tier. Walk down from the
        // top tier and stop at the first place it goes negative.
        Side::Long => {
            let mut i = tiers.len() - 1;
            loop {
                let tier = &tiers[i];
                if let Some(above) = tiers.get(i + 1) {
                    let top = bound_px(above)?;
                    if margin_at(tier, top)? < Decimal::ZERO {
                        return Ok(LiquidationPrice::At(top));
                    }
                }
                if i == 0 {
                    return if margin_at(tier, Decimal::ZERO)? < Decimal::ZERO {
                        root(tier)
                    } else {
                        Ok(LiquidationPrice::Never)
                    };
                }
                if margin_at(tier, bound_px(tier)?)? < Decimal::ZERO {
                    return root(tier);
                }
                i -= 1;
            }
        }
        // Margin falls as the price rises. Walk up from the first tier.
        Side::Short => {
            let mut i = 0;
            loop {
                let tier = &tiers[i];
                if i == 0 {
                    if margin_at(tier, Decimal::ZERO)? <= Decimal::ZERO {
                        return Ok(LiquidationPrice::AtAnyPrice);
                    }
                } else {
                    // The requirement can step up at a bound. If that step
                    // alone takes the margin negative, the bound is the price.
                    let bottom = bound_px(tier)?;
                    if margin_at(tier, bottom)? < Decimal::ZERO {
                        return Ok(LiquidationPrice::At(bottom));
                    }
                }
                match tiers.get(i + 1) {
                    Some(above) if margin_at(tier, bound_px(above)?)? >= Decimal::ZERO => i += 1,
                    _ => return root(tier),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::risk::{Position, TierTable, Venue, account_health};
    use rust_decimal::dec;

    fn market(coin: &str) -> MarketId {
        MarketId::new(Venue::Hyperliquid, coin)
    }

    fn btc_tiers() -> TierTable {
        TierTable::from_max_leverage(&[(dec!(0), dec!(40)), (dec!(150_000_000), dec!(20))]).unwrap()
    }

    fn schedule() -> Schedule {
        let mut schedule = Schedule::new();
        schedule.insert(market("BTC"), btc_tiers());
        schedule.insert(market("ETH"), TierTable::flat(dec!(0.02)).unwrap());
        schedule
    }

    fn one_position(collateral: Decimal, side: Side, size: Decimal, entry: Decimal) -> Account {
        let mut account = Account::new(collateral);
        account.set_position(market("BTC"), Position::new(side, size, entry).unwrap());
        account
    }

    fn price(account: &Account, marks: &Marks, schedule: &Schedule) -> Decimal {
        match liquidation_price(account, &market("BTC"), marks, schedule).unwrap() {
            LiquidationPrice::At(px) => px,
            other => panic!("expected a price, got {other:?}"),
        }
    }

    fn margin_at(account: &Account, btc: Decimal, schedule: &Schedule) -> Decimal {
        let marks = Marks::from([(market("BTC"), btc), (market("ETH"), dec!(3_000))]);
        account_health(account, &marks, schedule)
            .unwrap()
            .margin_available
    }

    #[test]
    fn long_matches_the_venue_formula() {
        // Hyperliquid documents
        //   liq = price - side * margin_available / size / (1 - l * side)
        // with l the maintenance rate. At a mark of 60,000 with 3,000 of
        // collateral, margin_available is 3,000 - 750 = 2,250.
        let account = one_position(dec!(3_000), Side::Long, dec!(1), dec!(60_000));
        let expected = dec!(60_000) - dec!(2_250) / (dec!(1) - dec!(0.0125));
        assert_eq!(price(&account, &Marks::new(), &schedule()), expected);
    }

    #[test]
    fn short_matches_the_venue_formula() {
        let account = one_position(dec!(3_000), Side::Short, dec!(1), dec!(60_000));
        let expected = dec!(60_000) + dec!(2_250) / (dec!(1) + dec!(0.0125));
        assert_eq!(price(&account, &Marks::new(), &schedule()), expected);
    }

    #[test]
    fn other_positions_move_the_price() {
        let mut account = one_position(dec!(3_000), Side::Long, dec!(1), dec!(60_000));
        let alone = price(&account, &Marks::new(), &schedule());

        account.set_position(
            market("ETH"),
            Position::new(Side::Long, dec!(10), dec!(3_000)).unwrap(),
        );
        // ETH is down 100 per coin: 1,000 of loss plus 580 of maintenance.
        let marks = Marks::from([(market("ETH"), dec!(2_900))]);
        let with_eth = price(&account, &marks, &schedule());
        assert_eq!(with_eth - alone, dec!(1_580) / dec!(0.9875));
    }

    #[test]
    fn fully_funded_long_is_never_liquidated() {
        let account = one_position(dec!(60_000), Side::Long, dec!(1), dec!(60_000));
        assert_eq!(
            liquidation_price(&account, &market("BTC"), &Marks::new(), &schedule()),
            Ok(LiquidationPrice::Never)
        );
    }

    #[test]
    fn short_with_no_equity_is_liquidatable_at_any_price() {
        let account = one_position(dec!(-60_000), Side::Short, dec!(1), dec!(60_000));
        assert_eq!(
            liquidation_price(&account, &market("BTC"), &Marks::new(), &schedule()),
            Ok(LiquidationPrice::AtAnyPrice)
        );
    }

    #[test]
    fn large_position_uses_the_tier_at_the_liquidation_price() {
        // 5,000 BTC stays above the 150M bracket all the way down to the
        // liquidation price, so the second tier's rate and deduction apply.
        let schedule = schedule();
        let account = one_position(dec!(20_000_000), Side::Long, dec!(5_000), dec!(60_000));
        let px = price(&account, &Marks::new(), &schedule);
        assert_eq!(px, dec!(278_125_000) / dec!(4_875));
        assert!(px * dec!(5_000) > dec!(150_000_000));
        assert!(margin_at(&account, px, &schedule).abs() < dec!(0.000001));
    }

    #[test]
    fn long_falls_through_to_the_lower_tier() {
        // Still solvent when notional drops to the 150M bound at 30,000, so
        // the first tier decides the price.
        let schedule = schedule();
        let account = one_position(dec!(160_000_000), Side::Long, dec!(5_000), dec!(60_000));
        let px = price(&account, &Marks::new(), &schedule);
        assert!(px * dec!(5_000) < dec!(150_000_000));
        assert!(margin_at(&account, px, &schedule).abs() < dec!(0.000001));
    }

    #[test]
    fn short_is_liquidated_by_a_step_in_the_requirement() {
        // A table with no deductions steps from 1% to 5% at 100,000 of
        // notional. This short is safe at the bound and underwater just past
        // it, so the bound is the liquidation price.
        let stepped = TierTable::new(vec![
            Tier {
                lower_bound: dec!(0),
                rate: dec!(0.01),
                deduction: dec!(0),
            },
            Tier {
                lower_bound: dec!(100_000),
                rate: dec!(0.05),
                deduction: dec!(0),
            },
        ])
        .unwrap();
        let mut schedule = Schedule::new();
        schedule.insert(market("BTC"), stepped);

        let account = one_position(dec!(12_000), Side::Short, dec!(10), dec!(9_000));
        assert_eq!(price(&account, &Marks::new(), &schedule), dec!(10_000));
        assert_eq!(margin_at(&account, dec!(10_000), &schedule), dec!(1_000));
        assert!(margin_at(&account, dec!(10_000.01), &schedule) < dec!(0));
    }

    #[test]
    fn errors_name_what_is_missing() {
        let account = one_position(dec!(3_000), Side::Long, dec!(1), dec!(60_000));
        assert_eq!(
            liquidation_price(&account, &market("ETH"), &Marks::new(), &schedule()),
            Err(RiskError::NoPosition(market("ETH")))
        );

        let mut two = account.clone();
        two.set_position(
            market("ETH"),
            Position::new(Side::Long, dec!(1), dec!(3_000)).unwrap(),
        );
        assert_eq!(
            liquidation_price(&two, &market("BTC"), &Marks::new(), &schedule()),
            Err(RiskError::MissingMark(market("ETH")))
        );
    }
}
