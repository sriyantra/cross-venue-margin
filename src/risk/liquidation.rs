//! Liquidation price of one position inside a cross-margined account.

use rust_decimal::Decimal;
use serde::Serialize;

use super::health::{asset_value, mark_for};
use super::netting::{matched_sizes, requirement};
use super::num::{add, div, mul, sub};
use super::{Account, MarginTable, MarketId, Marks, RiskError, Schedule, Side, Tier};

/// Where one market's mark must go for the account to reach its maintenance
/// margin, with every other mark held where it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "type", content = "price", rename_all = "snake_case")]
pub enum LiquidationPrice {
    /// The account is safe at this price and liquidatable beyond it: below
    /// it for a long, above it for a short. Margin available is zero here,
    /// except where a table's requirement steps up at a tier bound, in which
    /// case this is the bound and margin goes negative just past it.
    At(Decimal),
    /// No price of this market can make the account liquidatable. A long is
    /// in this state when its loss at a price of zero is covered. A short is
    /// in this state when the account holds at least as much of the same
    /// coin as collateral, so the collateral gains what the short loses.
    Never,
    /// The account is liquidatable at every price of this market. Only a
    /// short can be in this state.
    AtAnyPrice,
    /// The account is safe above this price and liquidatable below it,
    /// although the position is a short. This happens when collateral in
    /// the same coin outweighs the short and the dollar balance is far
    /// enough below zero that the coin's value is what keeps the account
    /// solvent.
    Below(Decimal),
}

/// Solves `equity(px) = maintenance(px)` for the mark of `market`.
///
/// In one tier the margin available is a straight line in the price:
/// `base + deduction + px * (size * sign - outright * rate - hedged *
/// hedge_rate + held)`. `hedged` is the part of the position matched by an
/// opposite position on another venue, `outright` is the rest, and `held` is
/// the haircut amount of collateral priced by this same market. In a table
/// keyed by notional the rate and deduction change from tier to tier, so
/// the answer is found by walking the tiers in the direction the price has
/// to move. In a table keyed by size the position stays in one tier at
/// every price, so there is a single line to solve.
///
/// Collateral priced by another market is held at its current value, like
/// every other mark. That includes the same coin on another venue.
///
/// A short with a dollar balance far below zero and only part of its size
/// held as collateral can be liquidatable both below one price and above
/// another. The upper one is reported.
pub fn liquidation_price(
    account: &Account,
    market: &MarketId,
    marks: &Marks,
    schedule: &Schedule,
) -> Result<LiquidationPrice, RiskError> {
    let position = account
        .position(market)
        .ok_or_else(|| RiskError::NoPosition(market.clone()))?;
    let size = position.size();
    let matched = matched_sizes(account, schedule)?;
    let hedged = matched.get(market).copied().unwrap_or(Decimal::ZERO);
    // The part of the position that pays its venue's table. The hedged part
    // pays the hedge rate at every price.
    let outright = sub(size, hedged)?;
    let hedge_rate = schedule.hedge_rate().unwrap_or(Decimal::ZERO);
    let one_line = |rate| {
        [Tier {
            lower_bound: Decimal::ZERO,
            rate,
            deduction: Decimal::ZERO,
        }]
    };
    let fixed;
    let tiers = match schedule.table(market)? {
        MarginTable::ByNotional(table) if outright > Decimal::ZERO => table.tiers(),
        MarginTable::ByNotional(_) => {
            fixed = one_line(Decimal::ZERO);
            &fixed[..]
        }
        MarginTable::BySize(table) => {
            // Asked for the whole size first, so a position past the table
            // is an error whether or not it is hedged.
            table.rate_for(size)?;
            fixed = one_line(if outright > Decimal::ZERO {
                table.rate_for(outright)?
            } else {
                Decimal::ZERO
            });
            &fixed[..]
        }
    };

    // Everything that does not move with this market's price: the dollar
    // balance, collateral priced elsewhere, the other positions' PnL and
    // maintenance, and this position's cost.
    let mut base = account.collateral;
    let mut held = Decimal::ZERO;
    for (asset, amount) in account.assets() {
        let rule = schedule.collateral(asset)?;
        if rule.price() == market {
            held = add(held, mul(amount, rule.weight())?)?;
        } else {
            base = add(base, asset_value(asset, amount, marks, schedule)?)?;
        }
    }
    for (other, held) in account.positions() {
        if other == market {
            continue;
        }
        let mark = mark_for(marks, other)?;
        let other_hedged = matched.get(other).copied().unwrap_or(Decimal::ZERO);
        let maintenance = requirement(schedule, other, held, other_hedged, mark)?.maintenance;
        base = sub(add(base, held.unrealized_pnl(mark)?)?, maintenance)?;
    }
    let sign = position.side().sign();
    base = sub(base, mul(mul(sign, size)?, position.entry_px())?)?;

    let slope = |tier: &Tier| {
        let required = add(mul(outright, tier.rate)?, mul(hedged, hedge_rate)?)?;
        add(sub(mul(size, sign)?, required)?, held)
    };
    let margin_at = |tier: &Tier, px: Decimal| -> Result<Decimal, RiskError> {
        add(add(base, tier.deduction)?, mul(px, slope(tier)?)?)
    };
    let root = |tier: &Tier| -> Result<LiquidationPrice, RiskError> {
        let px = div(-add(base, tier.deduction)?, slope(tier)?)?;
        Ok(LiquidationPrice::At(px))
    };
    // The price at which the outright part's notional reaches a tier's bound.
    // `None` means the position is too small to reach that bound at any
    // price the decimal type can hold.
    let bound_px = |tier: &Tier| tier.lower_bound.checked_div(outright);

    match position.side() {
        // Margin rises with the price inside every tier. Walk down from the
        // top tier and stop at the first place it goes negative.
        Side::Long => {
            let mut i = tiers.len() - 1;
            loop {
                let tier = &tiers[i];
                if let Some(top) = tiers.get(i + 1).and_then(bound_px)
                    && margin_at(tier, top)? < Decimal::ZERO
                {
                    return Ok(LiquidationPrice::At(top));
                }
                if i == 0 {
                    return if margin_at(tier, Decimal::ZERO)? < Decimal::ZERO {
                        root(tier)
                    } else {
                        Ok(LiquidationPrice::Never)
                    };
                }
                if let Some(bottom) = bound_px(tier)
                    && margin_at(tier, bottom)? < Decimal::ZERO
                {
                    return root(tier);
                }
                i -= 1;
            }
        }
        // Without collateral in this coin, margin falls as the price rises.
        // Walk up from the first tier. Collateral in this coin can make the
        // margin rise with the price in the lower tiers, so each tier's
        // slope is checked instead of assumed.
        Side::Short => {
            let mut i = 0;
            let mut lower = None;
            if margin_at(&tiers[0], Decimal::ZERO)? < Decimal::ZERO
                && slope(&tiers[0])? > Decimal::ZERO
            {
                // Liquidatable at a price of zero, with margin rising. Climb
                // to the price where the account becomes safe.
                loop {
                    let tier = &tiers[i];
                    let rise = slope(tier)?;
                    if rise <= Decimal::ZERO {
                        return Ok(LiquidationPrice::AtAnyPrice);
                    }
                    let px = div(-add(base, tier.deduction)?, rise)?;
                    match tiers.get(i + 1).and_then(bound_px) {
                        Some(top) if px >= top => i += 1,
                        _ => {
                            lower = Some(px);
                            break;
                        }
                    }
                }
            }
            let mut walked = false;
            loop {
                let tier = &tiers[i];
                if walked {
                    // The requirement can step up at a bound. If that step
                    // alone takes the margin negative, the bound is the price.
                    // This tier was only entered because its bound is a
                    // reachable price.
                    let bottom = bound_px(tier).ok_or(RiskError::Overflow)?;
                    if margin_at(tier, bottom)? < Decimal::ZERO {
                        return Ok(LiquidationPrice::At(bottom));
                    }
                } else if lower.is_none() {
                    let at_zero = margin_at(tier, Decimal::ZERO)?;
                    if at_zero < Decimal::ZERO
                        || (at_zero.is_zero() && slope(tier)? < Decimal::ZERO)
                    {
                        return Ok(LiquidationPrice::AtAnyPrice);
                    }
                }
                match tiers.get(i + 1).and_then(bound_px) {
                    Some(top) if margin_at(tier, top)? >= Decimal::ZERO => {
                        i += 1;
                        walked = true;
                    }
                    _ if slope(tier)? < Decimal::ZERO => return root(tier),
                    // Margin does not fall in the last tier the price can
                    // reach, so there is no upper price.
                    _ => {
                        return Ok(lower.map_or(LiquidationPrice::Never, LiquidationPrice::Below));
                    }
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

    /// The schedule above, accepting BTC as collateral at a 10% haircut.
    fn coin_schedule() -> Schedule {
        let mut schedule = schedule();
        schedule.insert_collateral(
            "BTC",
            crate::risk::CollateralRule::new(market("BTC"), dec!(0.10)).unwrap(),
        );
        schedule
    }

    fn with_coin(mut account: Account, amount: Decimal) -> Account {
        account.deposit("BTC", amount).unwrap();
        account
    }

    #[test]
    fn collateral_in_the_same_coin_moves_with_the_price() {
        let schedule = coin_schedule();
        let get = |account: &Account| {
            liquidation_price(account, &market("BTC"), &Marks::new(), &schedule).unwrap()
        };

        // Long 1 with 0.05 BTC: margin is -60,000 + px * (0.9875 + 0.045).
        let long = with_coin(
            one_position(dec!(0), Side::Long, dec!(1), dec!(60_000)),
            dec!(0.05),
        );
        assert_eq!(
            get(&long),
            LiquidationPrice::At(dec!(60_000) / dec!(1.0325))
        );

        // Short 1 with 0.5 BTC: margin is 63,000 + px * (0.45 - 1.0125).
        let part = with_coin(
            one_position(dec!(3_000), Side::Short, dec!(1), dec!(60_000)),
            dec!(0.5),
        );
        assert_eq!(get(&part), LiquidationPrice::At(dec!(112_000)));

        // Short 1 with 2 BTC: the coin gains more than the short loses.
        let covered = with_coin(
            one_position(dec!(0), Side::Short, dec!(1), dec!(60_000)),
            dec!(2),
        );
        assert_eq!(get(&covered), LiquidationPrice::Never);

        // The same, 100,000 in debt: margin is -40,000 + px * (2.7 - 1.0125),
        // so the account needs the coin to be worth enough.
        let in_debt = with_coin(
            one_position(dec!(-100_000), Side::Short, dec!(1), dec!(60_000)),
            dec!(3),
        );
        assert_eq!(
            get(&in_debt),
            LiquidationPrice::Below(dec!(40_000) / dec!(1.6875))
        );
    }

    #[test]
    fn collateral_priced_by_another_market_is_held_at_its_value() {
        let mut schedule = schedule();
        schedule.insert_collateral(
            "ETH",
            crate::risk::CollateralRule::new(market("ETH"), dec!(0.15)).unwrap(),
        );
        let mut account = one_position(dec!(0), Side::Long, dec!(1), dec!(60_000));
        account.deposit("ETH", dec!(1)).unwrap();
        // 1 ETH at 3,000 less 15% is 2,550 of fixed collateral.
        let marks = Marks::from([(market("ETH"), dec!(3_000))]);
        let dollars = one_position(dec!(2_550), Side::Long, dec!(1), dec!(60_000));
        assert_eq!(
            price(&account, &marks, &schedule),
            price(&dollars, &marks, &schedule)
        );
        // Without the ETH mark the collateral has no value to use.
        assert_eq!(
            liquidation_price(&account, &market("BTC"), &Marks::new(), &schedule),
            Err(RiskError::MissingMark(market("ETH")))
        );
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
    fn dust_position_cannot_reach_the_upper_tier() {
        // 150M of notional is past the largest decimal for a position this
        // small, so only the first tier can apply.
        let dust = Decimal::new(1, 22);
        let funded = one_position(dec!(1), Side::Long, dust, dec!(60_000));
        assert_eq!(
            liquidation_price(&funded, &market("BTC"), &Marks::new(), &schedule()),
            Ok(LiquidationPrice::Never)
        );
        let short = one_position(dec!(0.000000000000000001), Side::Short, dust, dec!(60_000));
        assert!(matches!(
            liquidation_price(&short, &market("BTC"), &Marks::new(), &schedule()),
            Ok(LiquidationPrice::At(_))
        ));
    }

    #[test]
    fn size_keyed_table_solves_one_line_at_the_position_tier() {
        use crate::risk::SizeTable;
        let okx = MarketId::new(Venue::Okx, "BTC");
        let mut schedule = schedule();
        schedule.insert(
            okx.clone(),
            SizeTable::new(&[(dec!(10), dec!(0.004)), (dec!(50), dec!(0.005))]).unwrap(),
        );
        // 20 coins sits in the 0.5% bracket at every price.
        let mut long = Account::new(dec!(100_000));
        long.set_position(
            okx.clone(),
            Position::new(Side::Long, dec!(20), dec!(60_000)).unwrap(),
        );
        let LiquidationPrice::At(px) =
            liquidation_price(&long, &okx, &Marks::new(), &schedule).unwrap()
        else {
            panic!("expected a price");
        };
        // 100,000 + 20 * (px - 60,000) = 20 * px * 0.005
        assert_eq!(px.round_dp(6), dec!(55_276.381910));
        let at = Marks::from([(okx.clone(), px)]);
        let health = account_health(&long, &at, &schedule).unwrap();
        assert!(health.margin_available.abs() < dec!(0.000001));

        let mut short = Account::new(dec!(100_000));
        short.set_position(
            okx.clone(),
            Position::new(Side::Short, dec!(20), dec!(60_000)).unwrap(),
        );
        // 100,000 + 20 * (60,000 - px) = 20 * px * 0.005
        let LiquidationPrice::At(px) =
            liquidation_price(&short, &okx, &Marks::new(), &schedule).unwrap()
        else {
            panic!("expected a price");
        };
        assert_eq!(px.round_dp(6), dec!(64_676.616915));
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
