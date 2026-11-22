//! Checks the risk core against Hyperliquid's own numbers.
//!
//! The fixture holds the clearinghouse state of real accounts, recorded
//! from the venue's public info API on 2026-11-22, with addresses removed.
//! Every account has only cross-margined BTC and ETH positions and no
//! collateral outside its perp balance. For each one, the maintenance margin
//! and every liquidation price computed here must match what the venue
//! reported.

use cross_venue_margin::risk::{
    Account, LiquidationPrice, MarketId, Marks, Position, Schedule, Side, Venue, account_health,
    liquidation_price,
};
use cross_venue_margin::venue::hyperliquid::maintenance_table;
use rust_decimal::{Decimal, dec};
use serde::Deserialize;

const ACCOUNTS: &str = include_str!("fixtures/hl_clearinghouse.json");

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct VenueAccount {
    account_value: Decimal,
    cross_maintenance_margin_used: Decimal,
    positions: Vec<VenuePosition>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct VenuePosition {
    coin: String,
    /// Signed size: negative is short.
    szi: Decimal,
    entry_px: Decimal,
    position_value: Decimal,
    unrealized_pnl: Decimal,
    liquidation_px: Option<Decimal>,
}

fn market(coin: &str) -> MarketId {
    MarketId::new(Venue::Hyperliquid, coin)
}

#[test]
fn margin_and_liquidation_prices_match_the_venue() {
    let accounts: Vec<VenueAccount> = serde_json::from_str(ACCOUNTS).unwrap();
    assert!(accounts.len() >= 10, "fixture should hold a real sample");

    let mut schedule = Schedule::new();
    for coin in ["BTC", "ETH"] {
        schedule.insert(market(coin), maintenance_table(coin).unwrap());
    }

    let (mut prices, mut never) = (0, 0);
    for (n, venue) in accounts.iter().enumerate() {
        // The venue reports equity. Collateral is equity with the open
        // profit and loss taken back out.
        let unrealized: Decimal = venue.positions.iter().map(|p| p.unrealized_pnl).sum();
        let mut account = Account::new(venue.account_value - unrealized);
        let mut marks = Marks::new();
        for p in &venue.positions {
            let side = if p.szi > dec!(0) {
                Side::Long
            } else {
                Side::Short
            };
            let position = Position::new(side, p.szi.abs(), p.entry_px).unwrap();
            account.set_position(market(&p.coin), position);
            marks.insert(market(&p.coin), p.position_value / p.szi.abs());
        }

        let health = account_health(&account, &marks, &schedule).unwrap();
        let margin_gap = (health.maintenance_required - venue.cross_maintenance_margin_used).abs();
        assert!(
            margin_gap < dec!(0.00001),
            "account {n}: maintenance off by {margin_gap}"
        );

        for p in &venue.positions {
            let mine = liquidation_price(&account, &market(&p.coin), &marks, &schedule).unwrap();
            match (mine, p.liquidation_px) {
                (LiquidationPrice::At(mine), Some(theirs)) => {
                    // The venue rounds entry prices and position values, so
                    // the inputs here carry a small error of their own.
                    let gap = ((mine - theirs) / theirs).abs();
                    assert!(
                        gap < dec!(0.00002),
                        "account {n} {}: {mine} vs {theirs}",
                        p.coin
                    );
                    prices += 1;
                }
                (LiquidationPrice::Never, None) => never += 1,
                (mine, theirs) => panic!("account {n} {}: {mine:?} vs {theirs:?}", p.coin),
            }
        }
    }
    assert!(prices >= 5 && never >= 3, "{prices} prices, {never} never");
}
