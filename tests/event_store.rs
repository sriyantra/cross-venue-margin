//! Runs the event store against a real Postgres.
//!
//! These tests need `TEST_DATABASE_URL`, a URL for a server where the user
//! may create databases, for example
//! `postgres://postgres@127.0.0.1:5432/postgres`. Each test creates its own
//! database. Without the variable the tests report that and do nothing.

use std::sync::atomic::{AtomicU32, Ordering};

use cross_venue_margin::engine::{AccountId, Engine, Event};
use cross_venue_margin::metrics::Metrics;
use cross_venue_margin::risk::{MarketId, Schedule, Side, Thresholds, TierTable, Venue};
use cross_venue_margin::store::{self, EventStore};
use rust_decimal::dec;
use sqlx::{Connection, PgConnection};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

static NEXT: AtomicU32 = AtomicU32::new(0);

/// A store on a new, empty database, and that database's URL.
async fn fresh_store() -> Option<(EventStore, String)> {
    let Ok(admin_url) = std::env::var("TEST_DATABASE_URL") else {
        eprintln!("TEST_DATABASE_URL is not set, skipping");
        return None;
    };
    let name = format!(
        "cvm_test_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    );
    let mut admin = PgConnection::connect(&admin_url).await.unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE DATABASE {name}")))
        .execute(&mut admin)
        .await
        .unwrap();
    let (base, _) = admin_url.rsplit_once('/').unwrap();
    let url = format!("{base}/{name}");
    Some((EventStore::connect(&url).await.unwrap(), url))
}

fn btc() -> MarketId {
    MarketId::new(Venue::Hyperliquid, "BTC")
}

fn parts() -> (Schedule, Thresholds) {
    let mut schedule = Schedule::new();
    schedule.insert(btc(), TierTable::flat(dec!(0.0125)).unwrap());
    (schedule, Thresholds::new(dec!(1.5)).unwrap())
}

/// Two accounts. The first is liquidated, the second keeps a position
/// whose entry price needs every decimal digit.
fn history() -> (Engine, Vec<Event>) {
    let (schedule, thresholds) = parts();
    let mut engine = Engine::new(schedule, thresholds);
    engine.set_mark(btc(), Some(dec!(60_000)), 0);
    let (first, mut events) = engine.create_account(dec!(3_000), 1).unwrap();
    events.extend(engine.fill(first, btc(), Side::Long, dec!(1), 2).unwrap());
    let (second, opened) = engine.create_account(dec!(50_000.10), 3).unwrap();
    events.extend(opened);
    events.extend(engine.fill(second, btc(), Side::Long, dec!(1), 4).unwrap());
    events.extend(engine.set_mark(btc(), Some(dec!(57_700)), 5));
    events.extend(engine.fill(second, btc(), Side::Long, dec!(2), 6).unwrap());
    (engine, events)
}

#[tokio::test]
async fn log_restores_the_engine_after_a_restart() {
    let Some((store, url)) = fresh_store().await else {
        return;
    };
    let (mut original, events) = history();
    // Written in three transactions, as a running engine would.
    store.append(&events[..2]).await.unwrap();
    store.append(&events[2..5]).await.unwrap();
    store.append(&events[5..]).await.unwrap();

    // A second connection stands in for the next process. Connecting runs
    // the migrations again, which must change nothing.
    let reopened = EventStore::connect(&url).await.unwrap();
    let loaded = reopened.load().await.unwrap();
    assert_eq!(loaded, events);

    let (schedule, thresholds) = parts();
    let mut restored = Engine::restore(schedule, thresholds, &loaded).unwrap();
    original.set_mark(btc(), Some(dec!(57_700)), 7);
    assert!(restored.set_mark(btc(), Some(dec!(57_700)), 7).is_empty());
    for id in [AccountId(1), AccountId(2)] {
        assert_eq!(restored.view(id), original.view(id));
    }
    // (60,000 + 2 * 57,700) / 3 does not terminate. No digit may be lost.
    let entry = restored.view(AccountId(2)).unwrap().positions[0].entry_px;
    assert_eq!(entry.to_string(), "58466.666666666666666666666667");
}

#[tokio::test]
async fn rows_cannot_be_changed_or_removed() {
    let Some((store, url)) = fresh_store().await else {
        return;
    };
    let (_, events) = history();
    store.append(&events).await.unwrap();

    let mut conn = PgConnection::connect(&url).await.unwrap();
    for statement in [
        "UPDATE events SET at_ms = 0",
        "DELETE FROM events",
        "TRUNCATE events",
    ] {
        let error = sqlx::query(statement)
            .execute(&mut conn)
            .await
            .expect_err(statement);
        assert!(error.to_string().contains("append-only"), "{error}");
    }
    assert_eq!(store.load().await.unwrap(), events);
}

#[tokio::test]
async fn failed_append_writes_nothing() {
    let Some((store, _url)) = fresh_store().await else {
        return;
    };
    let (_, mut events) = history();
    // An account id too large for the column fails after earlier rows of
    // the same call were sent.
    events.push(Event::Fault {
        account: AccountId(u64::MAX),
        error: "x".into(),
        at: 9,
    });
    assert!(store.append(&events).await.is_err());
    assert!(store.load().await.unwrap().is_empty());
}

#[tokio::test]
async fn writer_task_stores_everything_it_receives_in_order() {
    let Some((store, _url)) = fresh_store().await else {
        return;
    };
    let (_, events) = history();
    let (tx, rx) = mpsc::channel(2);
    let shutdown = CancellationToken::new();
    let writer = tokio::spawn(store::run(store.clone(), rx, Metrics::new(), shutdown));

    for _ in 0..50 {
        for event in &events {
            tx.send(event.clone()).await.unwrap();
        }
    }
    // Closing the channel is how the engine tells the writer it is done.
    drop(tx);
    writer.await.unwrap();

    let loaded = store.load().await.unwrap();
    assert_eq!(loaded.len(), events.len() * 50);
    assert!(loaded.chunks(events.len()).all(|chunk| chunk == events));
}
