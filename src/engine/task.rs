//! The Tokio task that owns the `Engine`.
//!
//! Nothing else can reach the engine. API handlers send it a `Command` over
//! a bounded channel and wait for the reply on a one-shot channel. Marks
//! arrive through watch channels. Events leave through a bounded channel, so
//! a slow consumer slows the engine down instead of losing an event.

use std::time::{SystemTime, UNIX_EPOCH};

use rust_decimal::Decimal;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::info;

use super::{AccountId, AccountView, Engine, EngineError, Event};
use crate::feed::MarkRx;
use crate::risk::{MarketId, Side};

type Reply<T> = oneshot::Sender<T>;

#[derive(Debug)]
pub enum Command {
    CreateAccount {
        collateral: Decimal,
        reply: Reply<Result<AccountView, EngineError>>,
    },
    Fill {
        account: AccountId,
        market: MarketId,
        side: Side,
        size: Decimal,
        reply: Reply<Result<AccountView, EngineError>>,
    },
    Get {
        account: AccountId,
        reply: Reply<Option<AccountView>>,
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

    pub async fn get(&self, account: AccountId) -> Result<Option<AccountView>, EngineError> {
        self.request(|reply| Command::Get { account, reply }).await
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

/// Runs the engine until `shutdown` is cancelled or every handle is dropped.
pub async fn run(
    mut engine: Engine,
    marks: Vec<(MarketId, MarkRx)>,
    mut commands: mpsc::Receiver<Command>,
    events: mpsc::Sender<Event>,
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

    loop {
        let mut out = Vec::new();
        tokio::select! {
            _ = shutdown.cancelled() => break,
            command = commands.recv() => match command {
                Some(command) => out = handle(&mut engine, command, now_ms()),
                None => break,
            },
            _ = any_mark_changed(&mut marks) => {}
        }

        // `changed()` marks a value as seen when it resolves, so asking a
        // receiver afterwards whether it changed would miss the very update
        // that woke the task. Read every channel and compare with what the
        // engine already has. Each value is copied out, so no watch borrow
        // is alive at the await below.
        let now = now_ms();
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
                    .map(|view| view.mark)
            };
            if current != watched.applied {
                out.extend(engine.set_mark(watched.market.clone(), current, now));
                watched.applied = current;
            }
            !closed
        });

        for event in out {
            if events.send(event).await.is_err() {
                return;
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
        Command::Get { account, reply } => {
            let _ = reply.send(engine.view(account));
            Vec::new()
        }
    }
}
