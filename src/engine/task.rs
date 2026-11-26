//! The Tokio task that owns the `Engine`.
//!
//! Nothing else can reach the engine. API handlers send it a `Command` over
//! a bounded channel and wait for the reply on a one-shot channel. Marks
//! arrive through watch channels. Events leave through a bounded channel, so
//! a slow consumer slows the engine down instead of losing an event.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rust_decimal::Decimal;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::info;

use super::{AccountId, AccountView, Engine, EngineError, Event, Funding};
use crate::feed::{BookRx, FundingRx, MarkRx};
use crate::metrics::Metrics;
use crate::risk::{MarketId, Side, Venue};

type Reply<T> = oneshot::Sender<T>;

#[derive(Debug)]
pub enum Command {
    CreateAccount {
        collateral: Decimal,
        reply: Reply<Result<AccountView, EngineError>>,
    },
    Deposit {
        account: AccountId,
        asset: String,
        amount: Decimal,
        reply: Reply<Result<AccountView, EngineError>>,
    },
    Fill {
        account: AccountId,
        market: MarketId,
        side: Side,
        size: Decimal,
        reply: Reply<Result<AccountView, EngineError>>,
    },
    /// Asks whether a fill would be accepted, and at what price, without
    /// applying it.
    CheckFill {
        account: AccountId,
        market: MarketId,
        side: Side,
        size: Decimal,
        reply: Reply<Result<Decimal, EngineError>>,
    },
    /// Records a fill a venue has already made.
    RecordFill {
        account: AccountId,
        market: MarketId,
        side: Side,
        size: Decimal,
        px: Decimal,
        reply: Reply<Result<AccountView, EngineError>>,
    },
    Get {
        account: AccountId,
        reply: Reply<Option<AccountView>>,
    },
    NetPositions {
        venue: Venue,
        reply: Reply<BTreeMap<String, Decimal>>,
    },
}

/// The API's way to talk to the engine task. Cheap to clone.
#[derive(Debug, Clone)]
pub struct EngineHandle {
    commands: mpsc::Sender<Command>,
}

impl EngineHandle {
    pub fn new(commands: mpsc::Sender<Command>) -> Self {
        Self { commands }
    }

    async fn request<T>(
        &self,
        command: impl FnOnce(Reply<T>) -> Command,
    ) -> Result<T, EngineError> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(command(reply))
            .await
            .map_err(|_| EngineError::Unavailable)?;
        response.await.map_err(|_| EngineError::Unavailable)
    }

    pub async fn create_account(&self, collateral: Decimal) -> Result<AccountView, EngineError> {
        self.request(|reply| Command::CreateAccount { collateral, reply })
            .await?
    }

    pub async fn deposit(
        &self,
        account: AccountId,
        asset: String,
        amount: Decimal,
    ) -> Result<AccountView, EngineError> {
        self.request(|reply| Command::Deposit {
            account,
            asset,
            amount,
            reply,
        })
        .await?
    }

    pub async fn fill(
        &self,
        account: AccountId,
        market: MarketId,
        side: Side,
        size: Decimal,
    ) -> Result<AccountView, EngineError> {
        self.request(|reply| Command::Fill {
            account,
            market,
            side,
            size,
            reply,
        })
        .await?
    }

    pub async fn check_fill(
        &self,
        account: AccountId,
        market: MarketId,
        side: Side,
        size: Decimal,
    ) -> Result<Decimal, EngineError> {
        self.request(|reply| Command::CheckFill {
            account,
            market,
            side,
            size,
            reply,
        })
        .await?
    }

    pub async fn record_fill(
        &self,
        account: AccountId,
        market: MarketId,
        side: Side,
        size: Decimal,
        px: Decimal,
    ) -> Result<AccountView, EngineError> {
        self.request(|reply| Command::RecordFill {
            account,
            market,
            side,
            size,
            px,
            reply,
        })
        .await?
    }

    pub async fn get(&self, account: AccountId) -> Result<Option<AccountView>, EngineError> {
        self.request(|reply| Command::Get { account, reply }).await
    }

    pub async fn net_positions(
        &self,
        venue: Venue,
    ) -> Result<BTreeMap<String, Decimal>, EngineError> {
        self.request(|reply| Command::NetPositions { venue, reply })
            .await
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// One market's mark channel, and the last value the engine was given.
struct Watched {
    market: MarketId,
    rx: MarkRx,
    applied: Option<Decimal>,
}

/// Resolves when any mark changes or any mark channel closes. Never
/// resolves when there is nothing to watch.
async fn any_mark_changed(marks: &mut [Watched]) {
    if marks.is_empty() {
        return std::future::pending().await;
    }
    let waits = marks.iter_mut().map(|w| Box::pin(w.rx.changed()));
    let _ = futures_util::future::select_all(waits).await;
}

/// How often the task wakes with nothing else to do, so that a funding
/// settlement is charged on time in a market whose mark is not moving.
const FUNDING_CHECK_EVERY: Duration = Duration::from_secs(1);

/// Runs the engine until `shutdown` is cancelled or every handle is dropped.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    mut engine: Engine,
    marks: Vec<(MarketId, MarkRx)>,
    books: Vec<(MarketId, BookRx)>,
    fundings: Vec<(MarketId, FundingRx)>,
    mut commands: mpsc::Receiver<Command>,
    events: mpsc::Sender<Event>,
    metrics: Metrics,
    shutdown: CancellationToken,
) {
    let mut marks: Vec<Watched> = marks
        .into_iter()
        .map(|(market, rx)| Watched {
            market,
            rx,
            applied: None,
        })
        .collect();

    let mut funding_check = tokio::time::interval(FUNDING_CHECK_EVERY);
    funding_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    'run: loop {
        let command = tokio::select! {
            _ = shutdown.cancelled() => break,
            command = commands.recv() => match command {
                Some(command) => Some(command),
                None => break,
            },
            _ = any_mark_changed(&mut marks) => None,
            _ = funding_check.tick() => None,
        };
        let now = now_ms();
        let mut out = Vec::new();

        // The engine prices fills and liquidations off the latest books. A
        // book change does not wake the task, so they are read here, before
        // anything that could use them. A book that is stale or whose
        // connector is gone is taken away.
        for (market, rx) in &books {
            let book = if rx.has_changed().is_err() {
                None
            } else {
                rx.borrow().clone()
            };
            engine.set_depth(market.clone(), book);
        }

        // Marks go first, so a command never runs against a mark the
        // connector has already replaced or withdrawn.
        //
        // `changed()` marks a value as seen when it resolves, so asking a
        // receiver afterwards whether it changed would miss the very update
        // that woke the task. Read every channel and compare with what the
        // engine already has. Each value is copied out, so no watch borrow
        // is alive at the await below.
        marks.retain_mut(|watched| {
            // A closed channel means the connector is gone and this mark
            // will never update again.
            let closed = watched.rx.has_changed().is_err();
            let current = if closed {
                None
            } else {
                watched
                    .rx
                    .borrow_and_update()
                    .as_ref()
                    .map(|view| (view.mark, view.received))
            };
            let mark = current.map(|(mark, _)| mark);
            if mark != watched.applied {
                out.extend(engine.set_mark(watched.market.clone(), mark, now));
                watched.applied = mark;
                if let Some((_, received)) = current {
                    metrics.mark_applied(watched.market.venue, received.elapsed());
                }
            }
            !closed
        });

        // Settlements that are due are charged at the marks just applied.
        // Only then are the venues' next settlements read, because a venue
        // announces the next one after the last has passed.
        out.extend(engine.settle_funding(now));
        for (market, rx) in &fundings {
            let next = rx.borrow().as_ref().map(|view| Funding {
                rate: view.rate,
                settles_at: view.settles_at,
            });
            if let Some(next) = next.filter(|next| next.settles_at > now) {
                engine.set_funding(market.clone(), next);
            }
        }

        if let Some(command) = command {
            out.extend(handle(&mut engine, command, now));
        }

        // A full queue makes the engine wait here, which is the point: an
        // event is never dropped. Shutdown must still get through.
        for event in out {
            metrics.event(event.kind());
            tokio::select! {
                _ = shutdown.cancelled() => break 'run,
                sent = events.send(event) => if sent.is_err() {
                    break 'run;
                },
            }
        }
    }
    info!("engine stopped");
}

fn handle(engine: &mut Engine, command: Command, now: u64) -> Vec<Event> {
    // A reply can fail only if the caller stopped waiting. The command has
    // still been applied, so its events are returned either way.
    match command {
        Command::CreateAccount { collateral, reply } => {
            let (result, events) = match engine.create_account(collateral, now) {
                Ok((id, events)) => (
                    engine.view(id).ok_or(EngineError::UnknownAccount(id)),
                    events,
                ),
                Err(error) => (Err(error), Vec::new()),
            };
            let _ = reply.send(result);
            events
        }
        Command::Deposit {
            account,
            asset,
            amount,
            reply,
        } => {
            let (result, events) = match engine.deposit(account, &asset, amount, now) {
                Ok(events) => (
                    engine
                        .view(account)
                        .ok_or(EngineError::UnknownAccount(account)),
                    events,
                ),
                Err(error) => (Err(error), Vec::new()),
            };
            let _ = reply.send(result);
            events
        }
        Command::Fill {
            account,
            market,
            side,
            size,
            reply,
        } => {
            let (result, events) = match engine.fill(account, market, side, size, now) {
                Ok(events) => (
                    engine
                        .view(account)
                        .ok_or(EngineError::UnknownAccount(account)),
                    events,
                ),
                Err(error) => (Err(error), Vec::new()),
            };
            let _ = reply.send(result);
            events
        }
        Command::CheckFill {
            account,
            market,
            side,
            size,
            reply,
        } => {
            let _ = reply.send(engine.check_fill(account, &market, side, size));
            Vec::new()
        }
        Command::RecordFill {
            account,
            market,
            side,
            size,
            px,
            reply,
        } => {
            let (result, events) = match engine.record_fill(account, market, side, size, px, now) {
                Ok(events) => (
                    engine
                        .view(account)
                        .ok_or(EngineError::UnknownAccount(account)),
                    events,
                ),
                Err(error) => (Err(error), Vec::new()),
            };
            let _ = reply.send(result);
            events
        }
        Command::Get { account, reply } => {
            let _ = reply.send(engine.view(account));
            Vec::new()
        }
        Command::NetPositions { venue, reply } => {
            let _ = reply.send(engine.net_positions(venue));
            Vec::new()
        }
    }
}
