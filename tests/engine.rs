//! Runs the engine task against hand-driven mark channels: commands in over
//! mpsc, marks in over watch, events out over mpsc.

use std::time::Duration;

use cross_venue_margin::engine::task::{self, Command, EngineHandle};
use cross_venue_margin::engine::{AccountView, Engine, EngineError, Event};
use cross_venue_margin::feed::{Feeds, FundingView, MarkTx, MarkView};
use cross_venue_margin::metrics::Metrics;
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
        venue: Venue::Hyperliquid,
        coin: "BTC".into(),
        mark,
        received: tokio::time::Instant::now(),
    }));
}

struct Harness {
    engine: EngineHandle,
    mark: MarkTx,
    events: mpsc::Receiver<Event>,
    metrics: Metrics,
    shutdown: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

fn start() -> Harness {
    start_with_event_queue(64)
}

fn start_with_event_queue(capacity: usize) -> Harness {
    let (feeds, mut publishers) = Feeds::channels(&[btc()]);
    let mut schedule = Schedule::new();
    schedule.insert(btc(), TierTable::flat(dec!(0.0125)).unwrap());
    let engine = Engine::new(schedule, Thresholds::new(dec!(1.5)).unwrap());

    let (command_tx, command_rx) = mpsc::channel::<Command>(8);
    let (event_tx, events) = mpsc::channel(capacity);
    let shutdown = CancellationToken::new();
    let marks = vec![(btc(), feeds.subscribe_mark(&btc()).unwrap())];
    let metrics = Metrics::new();
    let task = tokio::spawn(task::run(
        engine,
        marks,
        Vec::new(),
        Vec::new(),
        command_rx,
        event_tx,
        metrics.clone(),
        shutdown.clone(),
    ));

    Harness {
        engine: EngineHandle::new(command_tx),
        mark: publishers
            .take(Venue::Hyperliquid)
            .remove("BTC")
            .unwrap()
            .mark,
        events,
        metrics,
        shutdown,
        task,
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

    // Two marks reached the engine and five events left it.
    let text = h.metrics.render();
    assert!(
        text.contains("feed_to_decision_seconds_count{venue=\"hyperliquid\"} 2"),
        "{text}"
    );
    assert!(
        text.contains("engine_events_total{kind=\"state_changed\"} 2"),
        "{text}"
    );
    assert!(
        text.contains("engine_events_total{kind=\"liquidated\"} 1"),
        "{text}"
    );

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

#[tokio::test]
async fn command_never_runs_on_a_mark_that_was_replaced_or_withdrawn() {
    let mut h = start();
    publish(&h.mark, dec!(60_000));
    let opened = h.long_one_btc(dec!(30_000)).await;

    // Publish a new mark and send a fill straight after it, many times. The
    // engine must price every fill at the mark published just before it.
    let prices: Vec<Decimal> = (0..40).map(|i| dec!(60_000) + Decimal::from(i)).collect();
    for px in &prices {
        publish(&h.mark, *px);
        h.engine
            .fill(opened.id, btc(), Side::Long, dec!(0.001))
            .await
            .unwrap();
    }
    assert!(matches!(h.next_event().await, Event::AccountOpened { .. }));
    assert!(matches!(h.next_event().await, Event::Filled { px, .. } if px == dec!(60_000)));
    for want in prices {
        match h.next_event().await {
            Event::Filled { px, .. } => assert_eq!(px, want),
            other => panic!("expected a fill, got {other:?}"),
        }
    }

    h.mark.send_replace(None);
    assert_eq!(
        h.engine
            .fill(opened.id, btc(), Side::Short, dec!(0.001))
            .await,
        Err(EngineError::NoMark(btc()))
    );
    h.shutdown.cancel();
}

#[tokio::test]
async fn full_event_queue_slows_the_engine_and_loses_nothing() {
    let mut h = start_with_event_queue(1);
    let engine = h.engine.clone();
    let creator = tokio::spawn(async move {
        for _ in 0..6 {
            engine.create_account(dec!(1)).await.unwrap();
        }
    });

    let mut opened = Vec::new();
    for _ in 0..6 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        match h.next_event().await {
            Event::AccountOpened { account, .. } => opened.push(account.0),
            other => panic!("expected an account, got {other:?}"),
        }
    }
    assert_eq!(opened, [1, 2, 3, 4, 5, 6]);
    creator.await.unwrap();
    assert!(h.events.try_recv().is_err(), "an event arrived twice");
    h.shutdown.cancel();
}

#[tokio::test]
async fn shutdown_gets_through_a_full_event_queue() {
    let h = start_with_event_queue(1);
    // Nothing reads events. The first one fills the queue and the second
    // leaves the engine waiting for room.
    h.engine.create_account(dec!(1)).await.unwrap();
    h.engine.create_account(dec!(1)).await.unwrap();

    h.shutdown.cancel();
    tokio::time::timeout(WAIT, h.task)
        .await
        .expect("engine did not stop")
        .unwrap();
}

#[tokio::test]
async fn fill_is_priced_off_the_book_the_task_was_given() {
    use cross_venue_margin::book::Level;
    use cross_venue_margin::feed::BookView;
    use std::sync::Arc;

    let (feeds, mut publishers) = Feeds::channels(&[btc()]);
    let mut schedule = Schedule::new();
    schedule.insert(btc(), TierTable::flat(dec!(0.0125)).unwrap());
    let engine = Engine::new(schedule, Thresholds::new(dec!(1.5)).unwrap());
    let (command_tx, command_rx) = mpsc::channel::<Command>(8);
    let (event_tx, mut events) = mpsc::channel(64);
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(task::run(
        engine,
        vec![(btc(), feeds.subscribe_mark(&btc()).unwrap())],
        vec![(btc(), feeds.subscribe(&btc()).unwrap())],
        Vec::new(),
        command_rx,
        event_tx,
        Metrics::new(),
        shutdown.clone(),
    ));
    let handle = EngineHandle::new(command_tx);
    let publisher = publishers.take(Venue::Hyperliquid).remove("BTC").unwrap();

    publisher.book.send_replace(Some(Arc::new(BookView {
        venue: Venue::Hyperliquid,
        coin: "BTC".into(),
        time: 1,
        bids: vec![Level {
            px: dec!(59_990),
            sz: dec!(5),
        }],
        asks: vec![Level {
            px: dec!(60_010),
            sz: dec!(5),
        }],
        mid: Some(dec!(60_000)),
    })));
    publish(&publisher.mark, dec!(60_000));

    let account = handle.create_account(dec!(3_000)).await.unwrap();
    let view = tokio::time::timeout(WAIT, async {
        loop {
            match handle.fill(account.id, btc(), Side::Long, dec!(1)).await {
                Ok(view) => return view,
                Err(EngineError::NoMark(_)) => tokio::time::sleep(Duration::from_millis(5)).await,
                Err(other) => panic!("fill failed: {other}"),
            }
        }
    })
    .await
    .expect("timed out waiting for the first mark");
    assert_eq!(view.positions[0].entry_px, dec!(60_010));

    // A stale book is taken away, and the next fill is at the mark.
    publisher.book.send_replace(None);
    let view = handle
        .fill(account.id, btc(), Side::Short, dec!(1))
        .await
        .unwrap();
    assert!(view.positions.is_empty());
    assert_eq!(view.collateral, dec!(2_990));

    shutdown.cancel();
    task.await.unwrap();
    while events.recv().await.is_some() {}
}

#[tokio::test]
async fn funding_from_the_feed_is_charged_when_it_settles() {
    let (feeds, mut publishers) = Feeds::channels(&[btc()]);
    let mut schedule = Schedule::new();
    schedule.insert(btc(), TierTable::flat(dec!(0.0125)).unwrap());
    let engine = Engine::new(schedule, Thresholds::new(dec!(1.5)).unwrap());
    let (command_tx, command_rx) = mpsc::channel::<Command>(8);
    let (event_tx, mut events) = mpsc::channel(64);
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(task::run(
        engine,
        vec![(btc(), feeds.subscribe_mark(&btc()).unwrap())],
        Vec::new(),
        feeds
            .funding_receivers()
            .map(|(market, rx)| (market.clone(), rx))
            .collect(),
        command_rx,
        event_tx,
        Metrics::new(),
        shutdown.clone(),
    ));
    let handle = EngineHandle::new(command_tx);
    let publisher = publishers.take(Venue::Hyperliquid).remove("BTC").unwrap();

    publish(&publisher.mark, dec!(60_000));
    let account = handle.create_account(dec!(3_000)).await.unwrap();
    tokio::time::timeout(WAIT, async {
        loop {
            match handle.fill(account.id, btc(), Side::Long, dec!(1)).await {
                Ok(_) => return,
                Err(EngineError::NoMark(_)) => tokio::time::sleep(Duration::from_millis(5)).await,
                Err(other) => panic!("fill failed: {other}"),
            }
        }
    })
    .await
    .expect("timed out waiting for the first mark");

    // The venue settles two seconds from now. The mark does not move again,
    // so only the task's own timer can charge it.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    publisher.funding.send_replace(Some(FundingView {
        venue: Venue::Hyperliquid,
        coin: "BTC".into(),
        rate: dec!(0.0001),
        settles_at: now + 2_000,
    }));

    let paid = tokio::time::timeout(WAIT, async {
        loop {
            if let Some(Event::Funding { amount, at, .. }) = events.recv().await {
                return (amount, at);
            }
        }
    })
    .await
    .expect("timed out waiting for funding");
    assert_eq!(paid.0, dec!(-6));
    assert!(paid.1 >= now + 2_000);
    let view = handle.get(account.id).await.unwrap().unwrap();
    assert_eq!(view.collateral, dec!(2_994));

    shutdown.cancel();
    task.await.unwrap();
}
