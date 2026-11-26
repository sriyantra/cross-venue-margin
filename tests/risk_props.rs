//! Property tests for the risk core. Each test states a rule that must hold
//! for every account, and proptest searches for an account that breaks it.

use cross_venue_margin::risk::{
    Account, AccountState, CollateralRule, Health, LiquidationPrice, MarketId, Marks, Position,
    Schedule, Side, SizeTable, Thresholds, TierTable, Venue, account_health, liquidation_price,
    step,
};
use proptest::prelude::*;
use rust_decimal::{Decimal, dec};

const COINS: [&str; 3] = ["BTC", "ETH", "SOL"];
const TOLERANCE: Decimal = dec!(0.000001);

/// SOL sits on the second venue, so accounts span both.
fn market(coin: &str) -> MarketId {
    let venue = if coin == "SOL" {
        Venue::Okx
    } else {
        Venue::Hyperliquid
    };
    MarketId::new(venue, coin)
}

/// Tables whose bounds sit inside the notionals the generators produce, so
/// positions land in every tier. Max leverage of 15 gives a rate that does
/// not terminate in decimal. SOL uses a table keyed by size, whose last
/// bracket is wide enough for repeated fills to stack.
fn schedule() -> Schedule {
    let table = |brackets: &[(Decimal, Decimal)]| TierTable::from_max_leverage(brackets).unwrap();
    let mut schedule = Schedule::new();
    schedule.insert(
        market("BTC"),
        table(&[
            (dec!(0), dec!(40)),
            (dec!(50_000), dec!(20)),
            (dec!(500_000), dec!(10)),
        ]),
    );
    schedule.insert(
        market("ETH"),
        table(&[(dec!(0), dec!(25)), (dec!(100_000), dec!(15))]),
    );
    schedule.insert(
        market("SOL"),
        SizeTable::new(&[
            (dec!(1), dec!(0.004)),
            (dec!(10), dec!(0.01)),
            (dec!(100), dec!(0.03)),
            (dec!(100_000), dec!(0.05)),
        ])
        .unwrap(),
    );
    for (coin, haircut) in [("BTC", dec!(0.10)), ("ETH", dec!(0.15))] {
        schedule.insert_collateral(coin, CollateralRule::new(market(coin), haircut).unwrap());
    }
    schedule
}

/// 0.01 to 100,000.00
fn price() -> impl Strategy<Value = Decimal> {
    (1i64..=10_000_000).prop_map(|cents| Decimal::new(cents, 2))
}

/// 0.0001 to 100.0000
fn size() -> impl Strategy<Value = Decimal> {
    (1i64..=1_000_000).prop_map(|units| Decimal::new(units, 4))
}

fn side() -> impl Strategy<Value = Side> {
    prop_oneof![Just(Side::Long), Just(Side::Short)]
}

fn position() -> impl Strategy<Value = Position> {
    (side(), size(), price())
        .prop_map(|(side, size, entry)| Position::new(side, size, entry).unwrap())
}

/// An account with one to three positions, and a mark for every coin.
/// Collateral is 0% to 60% of the positions' cost, so accounts are
/// leveraged and most positions have a real liquidation price.
fn account_and_marks() -> impl Strategy<Value = (Account, Marks)> {
    let collateral = 0i64..=6_000;
    let positions = proptest::collection::vec(proptest::option::of(position()), COINS.len())
        .prop_filter("at least one position", |slots| {
            slots.iter().any(Option::is_some)
        });
    let marks = proptest::collection::vec(price(), COINS.len());
    (collateral, positions, marks).prop_map(|(collateral_bps, positions, marks)| {
        let cost: Decimal = positions
            .iter()
            .flatten()
            .map(|position| position.size() * position.entry_px())
            .sum();
        let collateral = (cost * Decimal::new(collateral_bps, 4)).round_dp(2);
        let mut account = Account::new(collateral);
        for (coin, slot) in COINS.iter().zip(positions) {
            if let Some(position) = slot {
                account.set_position(market(coin), position);
            }
        }
        let marks = COINS.iter().map(|coin| market(coin)).zip(marks).collect();
        (account, marks)
    })
}

/// The accounts above, with BTC and ETH also held as collateral in amounts
/// on the scale of the positions, and a dollar balance that can be below
/// zero. Coins can outweigh a short in the same coin or fall short of it.
fn account_with_coins() -> impl Strategy<Value = (Account, Marks)> {
    let coins = proptest::collection::vec(proptest::option::of(size()), 2);
    let cash = -15_000i64..=3_000;
    (account_and_marks(), coins, cash).prop_map(|((plain, marks), coins, cash_bps)| {
        let cost: Decimal = plain
            .positions()
            .map(|(_, position)| position.size() * position.entry_px())
            .sum();
        let mut account = Account::new((cost * Decimal::new(cash_bps, 4)).round_dp(2));
        for (market, position) in plain.positions() {
            account.set_position(market.clone(), *position);
        }
        for (coin, amount) in ["BTC", "ETH"].iter().zip(coins) {
            if let Some(amount) = amount {
                account.deposit(coin, amount).unwrap();
            }
        }
        (account, marks)
    })
}

fn okx_btc() -> MarketId {
    MarketId::new(Venue::Okx, "BTC")
}

/// The schedule above with BTC also listed on the second venue, and a long
/// and a short in the same coin on different venues netted at 0.25%.
fn hedged_schedule() -> Schedule {
    let mut schedule = schedule();
    schedule.insert(
        okx_btc(),
        SizeTable::new(&[
            (dec!(1), dec!(0.004)),
            (dec!(10), dec!(0.01)),
            (dec!(100_000), dec!(0.03)),
        ])
        .unwrap(),
    );
    schedule.set_hedge_rate(Some(dec!(0.0025))).unwrap();
    schedule
}

/// An account with BTC on both venues, usually on opposite sides, sometimes
/// with ETH beside it and coins as collateral.
fn hedged_account() -> impl Strategy<Value = (Account, Marks)> {
    let legs = (side(), size(), price(), any::<bool>(), size(), price());
    let eth = proptest::option::of(position());
    let coin = proptest::option::of(size());
    let cash = -5_000i64..=3_000;
    let marks = proptest::collection::vec(price(), 3);
    (legs, eth, coin, cash, marks).prop_map(|(legs, eth, coin, cash_bps, marks)| {
        let (side, a_size, a_entry, same_side, b_size, b_entry) = legs;
        let other = match (side, same_side) {
            (Side::Long, false) | (Side::Short, true) => Side::Short,
            _ => Side::Long,
        };
        let first = Position::new(side, a_size, a_entry).unwrap();
        let second = Position::new(other, b_size, b_entry).unwrap();
        let cost = a_size * a_entry + b_size * b_entry;
        let mut account = Account::new((cost * Decimal::new(cash_bps, 4)).round_dp(2));
        account.set_position(market("BTC"), first);
        account.set_position(okx_btc(), second);
        if let Some(eth) = eth {
            account.set_position(market("ETH"), eth);
        }
        if let Some(amount) = coin {
            account.deposit("BTC", amount).unwrap();
        }
        let marks = [market("BTC"), okx_btc(), market("ETH")]
            .into_iter()
            .zip(marks)
            .collect();
        (account, marks)
    })
}

fn margin_with(account: &Account, marks: &Marks, target: &MarketId, px: Decimal) -> Decimal {
    let mut moved = marks.clone();
    moved.insert(target.clone(), px);
    account_health(account, &moved, &schedule())
        .unwrap()
        .margin_available
}

proptest! {
    /// Gross margin: the account's numbers are the sums of its positions'
    /// numbers taken one at a time.
    #[test]
    fn health_is_additive_over_positions((account, marks) in account_and_marks()) {
        let schedule = schedule();
        let whole = account_health(&account, &marks, &schedule).unwrap();

        let (mut pnl, mut maintenance, mut notional) = (dec!(0), dec!(0), dec!(0));
        for (market, position) in account.positions() {
            let mut single = Account::new(dec!(0));
            single.set_position(market.clone(), *position);
            let part = account_health(&single, &marks, &schedule).unwrap();
            pnl += part.unrealized_pnl;
            maintenance += part.maintenance_required;
            notional += part.notional;
        }

        prop_assert_eq!(whole.unrealized_pnl, pnl);
        prop_assert_eq!(whole.maintenance_required, maintenance);
        prop_assert_eq!(whole.notional, notional);
        prop_assert_eq!(whole.equity, account.collateral + pnl);
        prop_assert_eq!(whole.margin_available, whole.equity - maintenance);
        prop_assert_eq!(whole.is_liquidatable(), whole.equity < maintenance);
    }

    /// The liquidation price is the price where margin available is zero,
    /// and the account is safe on one side of it and liquidatable on the other.
    #[test]
    fn liquidation_price_is_where_margin_runs_out((account, marks) in account_and_marks()) {
        let schedule = schedule();
        let tiny = dec!(0.00000001);

        for (target, position) in account.positions() {
            match liquidation_price(&account, target, &marks, &schedule).unwrap() {
                LiquidationPrice::At(px) => {
                    prop_assert!(px > dec!(0));
                    let at = margin_with(&account, &marks, target, px);
                    prop_assert!(at.abs() < TOLERANCE, "margin at {px} is {at}");

                    // Far enough from zero for a 1% move to outweigh rounding.
                    if px >= dec!(0.01) {
                        let above = margin_with(&account, &marks, target, px * dec!(1.01));
                        let below = margin_with(&account, &marks, target, px * dec!(0.99));
                        match position.side() {
                            Side::Long => prop_assert!(above > dec!(0) && below < dec!(0)),
                            Side::Short => prop_assert!(above < dec!(0) && below > dec!(0)),
                        }
                    }
                }
                LiquidationPrice::Never => {
                    prop_assert_eq!(position.side(), Side::Long);
                    prop_assert!(margin_with(&account, &marks, target, tiny) >= dec!(0));
                }
                LiquidationPrice::AtAnyPrice => {
                    prop_assert_eq!(position.side(), Side::Short);
                    prop_assert!(margin_with(&account, &marks, target, tiny) < dec!(0));
                }
                LiquidationPrice::Below(px) => {
                    prop_assert!(false, "no coin collateral, but got below {px}");
                }
            }
        }
    }

    /// Coins held as collateral count at their mark less the haircut, and
    /// nothing else about the account's numbers changes.
    #[test]
    fn coin_collateral_adds_its_haircut_value((account, marks) in account_with_coins()) {
        let schedule = schedule();
        let health = account_health(&account, &marks, &schedule).unwrap();

        let mut value = dec!(0);
        for (asset, amount) in account.assets() {
            let haircut = if asset == "BTC" { dec!(0.10) } else { dec!(0.15) };
            value += amount * marks[&market(asset)] * (dec!(1) - haircut);
        }
        prop_assert_eq!(health.collateral_value, value);
        prop_assert_eq!(health.equity, account.collateral + value + health.unrealized_pnl);
        prop_assert_eq!(health.margin_available, health.equity - health.maintenance_required);
    }

    /// With coins as collateral the liquidation price is still where margin
    /// available is zero, including when the coin is the one the position
    /// is in and its value moves with the same price.
    #[test]
    fn liquidation_price_holds_with_coin_collateral((account, marks) in account_with_coins()) {
        let schedule = schedule();
        let tiny = dec!(0.00000001);
        let huge = dec!(10_000_000_000);

        for (target, position) in account.positions() {
            let margin = |px: Decimal| margin_with(&account, &marks, target, px);
            match liquidation_price(&account, target, &marks, &schedule).unwrap() {
                LiquidationPrice::At(px) => {
                    prop_assert!(px > dec!(0));
                    prop_assert!(margin(px).abs() < TOLERANCE, "margin at {px} is {}", margin(px));
                    if px >= dec!(0.01) {
                        match position.side() {
                            Side::Long => prop_assert!(
                                margin(px * dec!(1.01)) > dec!(0) && margin(px * dec!(0.99)) < dec!(0)
                            ),
                            Side::Short => prop_assert!(margin(px * dec!(1.01)) < dec!(0)),
                        }
                    }
                }
                LiquidationPrice::Below(px) => {
                    prop_assert_eq!(position.side(), Side::Short);
                    prop_assert!(px > dec!(0));
                    prop_assert!(margin(px).abs() < TOLERANCE, "margin at {px} is {}", margin(px));
                    prop_assert!(margin(huge) >= dec!(0));
                    if px >= dec!(0.01) {
                        prop_assert!(
                            margin(px * dec!(1.01)) > dec!(0) && margin(px * dec!(0.99)) < dec!(0)
                        );
                    }
                }
                LiquidationPrice::Never => {
                    prop_assert!(margin(tiny) >= dec!(0));
                    if position.side() == Side::Short {
                        prop_assert!(margin(huge) >= dec!(0));
                    }
                }
                LiquidationPrice::AtAnyPrice => {
                    prop_assert_eq!(position.side(), Side::Short);
                    prop_assert!(margin(tiny) < dec!(0) && margin(huge) < dec!(0));
                }
            }
        }
    }

    /// Netting only ever lowers the requirement, changes nothing but the
    /// requirement, and changes nothing at all when no long faces a short in
    /// the same coin.
    #[test]
    fn netting_lowers_margin_and_only_for_hedged_pairs((account, marks) in hedged_account()) {
        let net_schedule = hedged_schedule();
        let mut gross_schedule = net_schedule.clone();
        gross_schedule.set_hedge_rate(None).unwrap();
        let net = account_health(&account, &marks, &net_schedule).unwrap();
        let gross = account_health(&account, &marks, &gross_schedule).unwrap();

        prop_assert_eq!(net.equity, gross.equity);
        prop_assert_eq!(net.notional, gross.notional);
        prop_assert!(net.maintenance_required <= gross.maintenance_required);
        prop_assert!(net.initial_required <= gross.initial_required);
        prop_assert!(net.initial_required >= net.maintenance_required);

        let sides: Vec<Side> = [market("BTC"), okx_btc()]
            .iter()
            .map(|m| account.position(m).unwrap().side())
            .collect();
        if sides[0] == sides[1] {
            prop_assert_eq!(net, gross);
        } else {
            prop_assert!(net.maintenance_required < gross.maintenance_required);
        }
    }

    /// With hedged legs on two venues the liquidation price of each leg is
    /// still where margin available is zero.
    #[test]
    fn liquidation_price_holds_with_hedged_legs((account, marks) in hedged_account()) {
        let schedule = hedged_schedule();
        let tiny = dec!(0.00000001);
        let huge = dec!(10_000_000_000);

        for (target, position) in account.positions() {
            let margin = |px: Decimal| {
                let mut moved = marks.clone();
                moved.insert(target.clone(), px);
                account_health(&account, &moved, &schedule).unwrap().margin_available
            };
            match liquidation_price(&account, target, &marks, &schedule).unwrap() {
                LiquidationPrice::At(px) => {
                    prop_assert!(px > dec!(0));
                    prop_assert!(margin(px).abs() < TOLERANCE, "margin at {px} is {}", margin(px));
                    if px >= dec!(0.01) {
                        match position.side() {
                            Side::Long => prop_assert!(
                                margin(px * dec!(1.01)) > dec!(0) && margin(px * dec!(0.99)) < dec!(0)
                            ),
                            Side::Short => prop_assert!(margin(px * dec!(1.01)) < dec!(0)),
                        }
                    }
                }
                LiquidationPrice::Below(px) => {
                    prop_assert_eq!(position.side(), Side::Short);
                    prop_assert!(margin(px).abs() < TOLERANCE, "margin at {px} is {}", margin(px));
                    prop_assert!(margin(huge) >= dec!(0));
                }
                LiquidationPrice::Never => {
                    prop_assert!(margin(tiny) >= dec!(0));
                    if position.side() == Side::Short {
                        prop_assert!(margin(huge) >= dec!(0));
                    }
                }
                LiquidationPrice::AtAnyPrice => {
                    prop_assert_eq!(position.side(), Side::Short);
                    prop_assert!(margin(tiny) < dec!(0) && margin(huge) < dec!(0));
                }
            }
        }
    }

    /// A bigger position never needs less maintenance margin, and the
    /// requirement does not jump when a position crosses into the next tier.
    #[test]
    fn maintenance_grows_smoothly_with_notional(
        a in 0i64..=200_000_000,
        b in 0i64..=200_000_000,
    ) {
        let schedule = schedule();
        let (low, high) = (Decimal::new(a.min(b), 2), Decimal::new(a.max(b), 2));
        for coin in COINS {
            let table = schedule.table(&market(coin)).unwrap();
            // Size is held at one coin, so a size-keyed table stays in one
            // bracket and a notional-keyed table ignores it.
            let m_low = table.maintenance(Decimal::ONE, low).unwrap();
            let m_high = table.maintenance(Decimal::ONE, high).unwrap();
            prop_assert!(m_low <= m_high);
            // No rate here is above 5%, so no step can exceed 5% of the gap.
            prop_assert!(m_high - m_low <= (high - low) * dec!(0.05) + TOLERANCE);
        }
    }

    /// Replaying any sequence of health readings: the state always matches
    /// the latest reading, `Closed` is final, a transition is reported
    /// exactly when the state changes, and a timestamp never moves while
    /// the state holds.
    #[test]
    fn lifecycle_follows_health(
        readings in proptest::collection::vec((-1_000i64..=5_000, 0i64..=2_000, any::<bool>()), 1..40),
    ) {
        let thresholds = Thresholds::new(dec!(1.5)).unwrap();
        let mut state = AccountState::Open;

        for (now, (equity, maintenance, has_positions)) in readings.into_iter().enumerate() {
            let (equity, maintenance) = (Decimal::from(equity), Decimal::from(maintenance));
            let health = Health {
                equity,
                collateral_value: dec!(0),
                unrealized_pnl: dec!(0),
                notional: dec!(0),
                maintenance_required: maintenance,
                initial_required: dec!(0),
                margin_available: equity - maintenance,
            };
            let (next, transition) = step(state, &health, has_positions, &thresholds, now as u64).unwrap();

            prop_assert_eq!(transition.is_some(), next != state);
            if let Some(t) = transition {
                prop_assert_eq!((t.from, t.to, t.at), (state, next, now as u64));
            }
            match (state, next) {
                (AccountState::Closed { .. }, _) => prop_assert_eq!(next, state),
                (AccountState::Liquidating { .. }, AccountState::Closed { .. }) => {
                    prop_assert!(!has_positions)
                }
                (_, AccountState::Closed { .. }) => prop_assert!(false, "closed without liquidating"),
                (_, AccountState::Liquidating { .. }) => {
                    prop_assert!(has_positions && equity < maintenance)
                }
                (_, AccountState::MarginCall { .. }) => prop_assert!(
                    has_positions && equity >= maintenance && equity < maintenance * dec!(1.5)
                ),
                (_, AccountState::Open) => {
                    prop_assert!(!has_positions || equity >= maintenance * dec!(1.5))
                }
            }
            let (again, repeat) = step(next, &health, has_positions, &thresholds, now as u64 + 1).unwrap();
            if !matches!((next, again), (AccountState::Liquidating { .. }, AccountState::Closed { .. })) {
                prop_assert_eq!(again, next);
                prop_assert_eq!(repeat, None);
            }
            state = next;
        }
    }

    /// A fill at the mark moves value between collateral and positions but
    /// never creates or destroys equity, and closing everything at the mark
    /// turns all of the equity into collateral.
    #[test]
    fn fills_at_the_mark_conserve_equity(
        (mut account, marks) in account_and_marks(),
        fills in proptest::collection::vec((0usize..COINS.len(), side(), size()), 1..8),
    ) {
        let schedule = schedule();
        let equity = |account: &Account| account_health(account, &marks, &schedule).unwrap().equity;
        let start = equity(&account);

        for (coin, side, size) in fills {
            let target = market(COINS[coin]);
            let before = (account.collateral, equity(&account));
            let fill = Position::new(side, size, marks[&target]).unwrap();
            let realized = account.apply_fill(target, fill).unwrap();
            prop_assert_eq!(account.collateral, before.0 + realized);
            prop_assert!((equity(&account) - before.1).abs() < TOLERANCE);
        }

        account.close_all(&marks).unwrap();
        prop_assert!(!account.has_positions());
        prop_assert!((account.collateral - start).abs() < TOLERANCE);
    }
}
