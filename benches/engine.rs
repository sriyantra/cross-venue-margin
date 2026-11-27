//! How long the engine takes to apply one mark.
//!
//! `everyone` is the worst case: every account holds the market and has to
//! be rechecked. `one_in_100` is a market few accounts hold, where the
//! cost should follow the holders and not the number of accounts.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use cross_venue_margin::engine::Engine;
use cross_venue_margin::risk::{MarketId, Schedule, Side, Thresholds, TierTable, Venue};
use rust_decimal::dec;

fn market(coin: &str) -> MarketId {
    MarketId::new(Venue::Hyperliquid, coin)
}

/// `accounts` accounts, each long one market: BTC for one in every
/// `btc_every`, ETH for the rest.
fn engine(accounts: usize, btc_every: usize) -> Engine {
    let mut schedule = Schedule::new();
    schedule.insert(market("BTC"), TierTable::flat(dec!(0.0125)).unwrap());
    schedule.insert(market("ETH"), TierTable::flat(dec!(0.02)).unwrap());
    let mut engine = Engine::new(schedule, Thresholds::new(dec!(1.5)).unwrap());
    engine.set_mark(market("BTC"), Some(dec!(60_000)), 0);
    engine.set_mark(market("ETH"), Some(dec!(1_700)), 0);
    for n in 0..accounts {
        let (id, _) = engine.create_account(dec!(10_000), 0).unwrap();
        let (coin, size) = if n % btc_every == 0 {
            ("BTC", dec!(0.1))
        } else {
            ("ETH", dec!(1))
        };
        engine.fill(id, market(coin), Side::Long, size, 0).unwrap();
    }
    engine
}

fn set_mark(c: &mut Criterion) {
    for (name, btc_every) in [("everyone", 1), ("one_in_100", 100)] {
        let mut group = c.benchmark_group(format!("set_mark/{name}"));
        for accounts in [1, 1_000, 100_000] {
            let mut engine = engine(accounts, btc_every);
            let mut up = false;
            group.bench_function(BenchmarkId::from_parameter(accounts), |b| {
                b.iter(|| {
                    up = !up;
                    let mark = if up { dec!(60_010) } else { dec!(60_000) };
                    engine.set_mark(market("BTC"), Some(mark), 1)
                })
            });
        }
        group.finish();
    }
}

criterion_group!(benches, set_mark);
criterion_main!(benches);
