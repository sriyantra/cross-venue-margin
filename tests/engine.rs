//! Runs the engine task against hand-driven mark channels: commands in over
//! mpsc, marks in over watch, events out over mpsc.

use std::time::Duration;

use cross_venue_margin::engine::task::{self, Command, EngineHandle};
use cross_venue_margin::engine::{AccountView, Engine, EngineError, Event};
use cross_venue_margin::feed::{Feeds, MarkTx, MarkView};
use cross_venue_margin::risk::{
    AccountState, MarketId, Schedule, Side, Thresholds, TierTable, Venue,
};
use rust_decimal::{Decimal, dec};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

const WAIT: Duration = Duration::from_secs(5);

fn btc() -> MarketId {
    MarketId::new(Venue::Hyperliquid, "BTC")
}

fn publish(tx: &MarkTx, mark: Decimal) {
    tx.send_replace(Some(MarkView {
        venue: "hyperliquid",
        coin: "BTC".into(),
        mark,
    }));
}

struct Harness {
    engine: EngineHandle,
    mark: MarkTx,
    events: mpsc::Receiver<Event>,
    shutdown: CancellationToken,
}

fn start() -> Harness {
    let (feeds, mut publishers) = Feeds::channels(&["BTC".to_string()]);
    let mut schedule = Schedule::new();
    schedule.insert(btc(), TierTable::flat(dec!(0.0125)).unwrap());
    let engine = Engine::new(schedule, Thresholds::new(dec!(1.5)).unwrap());

    let (command_tx, command_rx) = mpsc::channel::<Command>(8);
    let (event_tx, events) = mpsc::channel(64);
    let shutdown = CancellationToken::new();
    let marks = vec![(btc(), feeds.subscribe_mark("BTC").unwrap())];
    tokio::spawn(task::run(
        engine,
        marks,
        command_rx,
        event_tx,
        shutdown.clone(),
    ));

    Harness {
        engine: EngineHandle::new(command_tx),
        mark: publishers.remove("BTC").unwrap().mark,
        events,
        shutdown,
    }
}

impl Harness {
    async fn next_event(&mut self) -> Event {
        tokio::time::timeout(WAIT, self.events.recv())
            .await
            .expect("timed out waiting for an event")
            .expect("engine stopped")
    }

    /// Polls the account until `want` holds. Marks reach the engine on its
    /// own schedule, so a test cannot assume one has landed yet.
    async fn view_when(&self, id: u64, want: impl Fn(&AccountView) -> bool) -> AccountView {
        let id = cross_venue_margin::engine::AccountId(id);
        tokio::time::timeout(WAIT, async {
            loop {
                let view = self.engine.get(id).await.unwrap().unwrap();
                if want(&view) {
                    return view;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("timed out waiting for account state")
    }

    /// Opens an account and fills a long once the first mark has landed.
    async fn long_one_btc(&mut self, collateral: Decimal) -> AccountView {
        let account = self.engine.create_account(collateral).await.unwrap();
        tokio::time::timeout(WAIT, async {
            loop {
                match self
                    .engine
                    .fill(account.id, btc(), Side::Long, dec!(1))
                    .await
                {
                    Ok(view) => return view,
                    Err(EngineError::NoMark(_)) => {
                        tokio::time::sleep(Duration::from_millis(5)).await
                    }
                    Err(other) => panic!("fill failed: {other}"),
                }
            }
        })
        .await
        .expect("timed out waiting for the first mark")
    }
}

#[tokio::test]
async fn mark_move_liquidates_and_every_event_is_delivered_in_order() {
    let mut h = start();
    publish(&h.mark, dec!(60_000));
    let opened = h.long_one_btc(dec!(3_000)).await;
    assert_eq!(opened.state, AccountState::Open);
    assert_eq!(opened.health.unwrap().maintenance_required, dec!(750));

    publish(&h.mark, dec!(57_700));
    let closed = h
        .view_when(opened.id.0, |view| {
            matches!(view.state, AccountState::Closed { .. })
        })
        .await;
    assert_eq!(closed.collateral, dec!(700));
    assert!(closed.positions.is_empty());

    assert!(matches!(h.next_event().await, Event::AccountOpened { .. }));
    assert!(matches!(h.next_event().await, Event::Filled { .. }));
    assert!(matches!(
        h.next_event().await,
        Event::StateChanged {
            to: AccountState::Liquidating { .. },
            ..
        }
    ));
    assert!(matches!(
        h.next_event().await,
        Event::Liquidated { px, .. } if px == dec!(57_700)
    ));
    assert!(matches!(
        h.next_event().await,
        Event::StateChanged {
            to: AccountState::Closed { .. },
            ..
        }
    ));

    h.shutdown.cancel();
}

#[tokio::test]
async fn withdrawn_mark_hides_health_until_it_returns() {
    let mut h = start();
    publish(&h.mark, dec!(60_000));
    let opened = h.long_one_btc(dec!(3_000)).await;

    h.mark.send_replace(None);
    let stale = h.view_when(opened.id.0, |view| view.health.is_none()).await;
    assert_eq!(stale.state, AccountState::Open);
    assert_eq!(
        h.engine.fill(opened.id, btc(), Side::Short, dec!(1)).await,
        Err(EngineError::NoMark(btc()))
    );

    publish(&h.mark, dec!(61_000));
    let live = h.view_when(opened.id.0, |view| view.health.is_some()).await;
    assert_eq!(live.health.unwrap().equity, dec!(4_000));

    h.shutdown.cancel();
}

#[tokio::test]
async fn closed_mark_channel_is_treated_as_a_lost_mark() {
    let mut h = start();
    publish(&h.mark, dec!(60_000));
    let opened = h.long_one_btc(dec!(3_000)).await;

    // Dropping the sender is what a dead connector looks like.
    let Harness { engine, mark, .. } = h;
    drop(mark);
    let id = opened.id;
    let stale = tokio::time::timeout(WAIT, async {
        loop {
            let view = engine.get(id).await.unwrap().unwrap();
            if view.health.is_none() {
                return view;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("timed out waiting for the mark to be dropped");
    assert_eq!(stale.positions[0].mark, None);
}

#[tokio::test]
async fn handle_reports_a_stopped_engine() {
    let h = start();
    h.shutdown.cancel();
    let result = tokio::time::timeout(WAIT, async {
        loop {
            match h.engine.create_account(dec!(1)).await {
                Err(EngineError::Unavailable) => return,
                _ => tokio::time::sleep(Duration::from_millis(5)).await,
            }
        }
    })
    .await;
    assert!(result.is_ok(), "engine never reported as stopped");
}
