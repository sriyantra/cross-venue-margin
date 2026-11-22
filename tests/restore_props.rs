//! Property tests for rebuilding an engine from its event log. A random
//! history of accounts, fills and mark moves on two venues is run through
//! one engine, and a second engine is rebuilt from the events alone.

use std::collections::BTreeMap;

use cross_venue_margin::engine::{AccountId, Engine, Event};
use cross_venue_margin::risk::{MarketId, Schedule, Side, SizeTable, Thresholds, TierTable, Venue};
use proptest::prelude::*;
use rust_decimal::{Decimal, dec};

fn markets() -> [MarketId; 2] {
    [
        MarketId::new(Venue::Hyperliquid, "BTC"),
        MarketId::new(Venue::Okx, "BTC"),
    ]
}

fn parts() -> (Schedule, Thresholds) {
    let [hl, okx] = markets();
    let mut schedule = Schedule::new();
    schedule.insert(
        hl,
        TierTable::from_max_leverage(&[(dec!(0), dec!(40)), (dec!(5_000), dec!(15))]).unwrap(),
    );
    schedule.insert(
        okx,
        SizeTable::new(&[(dec!(10), dec!(0.004)), (dec!(10_000), dec!(0.01))]).unwrap(),
    );
    (schedule, Thresholds::new(dec!(1.5)).unwrap())
}

#[derive(Debug, Clone)]
enum Op {
    Open(Decimal),
    Fill {
        account: usize,
        market: usize,
        side: Side,
        size: Decimal,
    },
    /// `None` withdraws the mark.
    Mark(usize, Option<Decimal>),
}

/// Marks stay within 20% of 100, close enough for leveraged accounts to be
/// called and liquidated by a move.
fn op() -> impl Strategy<Value = Op> {
    let side = prop_oneof![Just(Side::Long), Just(Side::Short)];
    prop_oneof![
        1 => (0i64..=200_000).prop_map(|cents| Op::Open(Decimal::new(cents, 2))),
        4 => (0usize..4, 0usize..2, side, 1i64..=300_000).prop_map(
            |(account, market, side, units)| Op::Fill {
                account,
                market,
                side,
                size: Decimal::new(units, 4),
            }
        ),
        4 => (0usize..2, 8_000i64..=12_000)
            .prop_map(|(market, cents)| Op::Mark(market, Some(Decimal::new(cents, 2)))),
        1 => (0usize..2).prop_map(|market| Op::Mark(market, None)),
    ]
}

struct Run {
    engine: Engine,
    events: Vec<Event>,
    marks: BTreeMap<MarketId, Decimal>,
    accounts: Vec<AccountId>,
}

fn run(ops: &[Op]) -> Run {
    let (schedule, thresholds) = parts();
    let mut run = Run {
        engine: Engine::new(schedule, thresholds),
        events: Vec::new(),
        marks: BTreeMap::new(),
        accounts: Vec::new(),
    };
    for market in markets() {
        run.engine.set_mark(market.clone(), Some(dec!(100)), 0);
        run.marks.insert(market, dec!(100));
    }
    for (step, op) in ops.iter().enumerate() {
        let now = step as u64 + 1;
        match op {
            Op::Open(collateral) => {
                let (id, events) = run.engine.create_account(*collateral, now).unwrap();
                run.accounts.push(id);
                run.events.extend(events);
            }
            Op::Fill {
                account,
                market,
                side,
                size,
            } => {
                let Some(id) = run.accounts.get(*account) else {
                    continue;
                };
                let market = markets()[*market].clone();
                // A refused fill emits nothing and changes nothing.
                if let Ok(events) = run.engine.fill(*id, market, *side, *size, now) {
                    run.events.extend(events);
                }
            }
            Op::Mark(market, mark) => {
                let market = markets()[*market].clone();
                run.events
                    .extend(run.engine.set_mark(market.clone(), *mark, now));
                match mark {
                    Some(px) => run.marks.insert(market, *px),
                    None => run.marks.remove(&market),
                };
            }
        }
    }
    run
}

proptest! {
    /// An engine rebuilt from the log, given the marks the original ended
    /// with, reports every account exactly as the original does, and the
    /// marks cause no new events.
    #[test]
    fn restored_engine_matches_the_original(ops in proptest::collection::vec(op(), 1..60)) {
        let run = run(&ops);
        // Through JSON, the way the log stores them.
        let json = serde_json::to_string(&run.events).unwrap();
        let logged: Vec<Event> = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(&logged, &run.events);

        let (schedule, thresholds) = parts();
        let mut restored = Engine::restore(schedule, thresholds, &logged).unwrap();
        for (market, px) in &run.marks {
            let caused = restored.set_mark(market.clone(), Some(*px), 1_000);
            prop_assert!(caused.is_empty(), "restoring a mark caused {caused:?}");
        }
        for id in &run.accounts {
            prop_assert_eq!(restored.view(*id), run.engine.view(*id));
        }

        // New accounts continue the numbering.
        let mut original = run.engine;
        let next = original.create_account(dec!(1), 2_000).unwrap().0;
        prop_assert_eq!(restored.create_account(dec!(1), 2_000).unwrap().0, next);
    }

    /// A log cut off at any event is still a valid log.
    #[test]
    fn every_prefix_of_a_log_restores(
        ops in proptest::collection::vec(op(), 1..60),
        cut in 0usize..1_000,
    ) {
        let run = run(&ops);
        let cut = cut % (run.events.len() + 1);
        let (schedule, thresholds) = parts();
        let mut restored = Engine::restore(schedule, thresholds, &run.events[..cut]).unwrap();
        // Marks arriving afterwards must not fault any account.
        for market in markets() {
            let caused = restored.set_mark(market, Some(dec!(100)), 1_000);
            let faulted = caused.iter().any(|event| matches!(event, Event::Fault { .. }));
            prop_assert!(!faulted, "fault after restore: {:?}", caused);
        }
    }
}
