# cross-venue-margin

A Rust service that keeps live order books from more than one trading venue and
computes margin for accounts whose positions span those venues on a single
collateral balance.

Status: stage 3 of 4 is done. Two venues are connected, Hyperliquid and
OKX, with live books and mark prices from both. A paper account can hold
positions on both venues against one collateral balance, each margined by
its own venue's rules and marked at its own venue's price. Every account
event goes to an append-only log in Postgres, and the service rebuilds its
accounts from that log when it starts.

Accounts are paper accounts. The service places no orders and holds no funds.

## What works today

- Hyperliquid connector on the `l2` channel: one snapshot per market, then
  diffs (base64 plus raw DEFLATE), applied to an in-memory book.
- Desync detection. A diff that cannot be applied, a crossed book, or a
  timestamp that moves backwards clears the book and triggers a resubscribe
  for that market.
- Reconnect with exponential backoff, an application ping, and a staleness
  watchdog that drops a connection that has gone quiet.
- Readers never see a book that is known to be wrong. While a book is being
  rebuilt, the API reports it as stale.
- The venue's mark price per market (`activeAssetCtx`), withdrawn if the
  venue stops refreshing it.
- OKX connector for USDT perpetuals on the `books` channel: one 400-level
  snapshot, then updates keyed by price. Every update must name the
  sequence number of the message before it, or the book is cleared and
  resubscribed. Sizes are converted from contracts to coin.
- OKX mark price per market (`mark-price`), with the same staleness rule.
- HTTP and WebSocket API over the current books and marks.
- Paper accounts with one collateral balance. Orders fill at the venue's
  mark. A fill that would leave the account in a margin call is refused.
- Account health and each position's liquidation price, recomputed when a
  mark changes.
- An account state machine: open, margin call, liquidating, closed. An
  account whose equity falls below maintenance margin is closed out at the
  mark.
- Every account event (opened, filled, state changed, liquidated) is
  written to an append-only Postgres table through a bounded channel. A
  slow or failing database slows the engine down. It does not drop events.
- On start the service reads the log in order and rebuilds every account
  from it. A log that does not describe a possible history is rejected.
- Metrics for a Prometheus scraper, including a histogram of the time from
  a mark price arriving to the engine having acted on it.

## Margin model

- Gross cross margin. Every position pays its own maintenance margin, and
  positions offset each other only through profit and loss in account equity.
- Maintenance margin per position is `notional * rate - deduction`, from a
  tier table per market. The Hyperliquid tables for BTC and ETH are built
  from the max-leverage brackets in its documentation (read on 2026-11-22),
  using its rule that the maintenance rate is half the initial margin rate
  at max leverage. Other markets have no table and positions in them are
  refused.
- OKX keys its tiers by position size instead. The rate of the tier a
  position's size falls in applies to its whole notional, so the
  requirement steps up when a fill crosses a tier bound. The BTC and ETH
  tables hold the first 12 tiers from its public position tiers endpoint
  (read on 2026-11-22), which covers 2,000 BTC and 25,000 ETH. Larger
  positions are refused.
- OKX perpetuals settle in USDT. One USDT is counted as one dollar of the
  account's collateral, with no conversion rate.
- OKX adds a liquidation fee to the maintenance requirement. That fee is
  not modeled, so an OKX position here liquidates slightly later than it
  would on the venue.
- The Hyperliquid numbers are checked against the venue's own (see Test).
  OKX account data is not public, so the OKX numbers are checked against
  its documented formula only.
- Equity is collateral plus unrealized profit and loss at the venue's mark.
  Funding and fees are not modeled.
- Liquidation closes every position at the mark in one step. Real venues
  liquidate in stages and at worse prices.
- A mark that gaps past the liquidation price leaves the closed account
  with negative collateral. There is no insurance fund or bad-debt handling.
- While any of an account's markets has no current mark, its health is
  unknown and its state is frozen. It is not liquidated on an old price,
  even if its other positions are losing.

## Run

```sh
cargo run --release
```

Environment:

| Variable    | Default                        | Meaning                    |
| ----------- | ------------------------------ | -------------------------- |
| `HL_WS_URL` | `wss://api.hyperliquid.xyz/ws` | Hyperliquid WebSocket URL  |
| `OKX_WS_URL` | `wss://ws.okx.com:443/ws/v5/public` | OKX public WebSocket URL |
| `COINS`     | `BTC,ETH`                      | Coins to track on each venue that lists them |
| `BIND`      | `127.0.0.1:8080`               | API listen address         |
| `DATABASE_URL` | unset                       | Postgres URL for the event log. Without it events are only logged and accounts are lost on restart |
| `MARGIN_CALL_MULTIPLE` | `1.5`               | Margin call starts when equity is below maintenance margin times this |
| `RUST_LOG`  | `info`                         | Log filter                 |

## API

| Route              | Returns                                              |
| ------------------ | ---------------------------------------------------- |
| `GET /health`      | Per-market liveness and age. 503 if any book or mark is stale |
| `GET /book/{venue}/{coin}` | The current book. 503 with `{"type":"stale"}` if not live |
| `GET /mark/{venue}/{coin}` | The venue's mark price. 503 with `{"type":"stale"}` if not live |
| `GET /ws/{venue}/{coin}`   | WebSocket: the current book, then every update |

| `POST /accounts`            | Open a paper account. Body: `{"collateral":"5000"}` |
| `GET /accounts/{id}`        | State, health, and positions with liquidation prices |
| `POST /accounts/{id}/fills` | Fill at the mark. Body: `{"venue":"hyperliquid","coin":"BTC","side":"long","size":"0.5"}` |
| `GET /metrics`              | Metrics in the OpenMetrics text format |

Amounts are JSON strings, so no value passes through a binary float.

```sh
curl -s localhost:8080/book/hyperliquid/BTC
curl -s -H 'content-type: application/json' -d '{"collateral":"5000"}' localhost:8080/accounts
curl -s -H 'content-type: application/json' \
  -d '{"venue":"hyperliquid","coin":"BTC","side":"long","size":"0.5"}' \
  localhost:8080/accounts/1/fills
```

## Design

```
venue task (owns socket + books) --watch: book, mark--> API handlers
                                 --watch: mark-------> engine task (owns accounts)
API handlers --bounded mpsc: commands, one-shot replies--> engine task
engine task  --bounded mpsc: events--> log writer task --> Postgres (append-only)
Postgres     --events in order, at startup--> engine (rebuilt before it runs)
```

- `book` is pure. It has no I/O and no clock, so it is tested by replaying
  frames recorded from the live feed (`tests/fixtures`).
- Each venue connector is one Tokio task that owns its socket and its books.
  Nothing else can mutate a book, so there are no locks around book state.
- Books are published through `tokio::sync::watch`, which keeps only the
  latest value. A slow reader skips to the newest book and cannot build a
  backlog.
- `risk` and `engine` are pure too. The engine takes the time as an
  argument and returns the events it caused, so the same inputs always
  produce the same events.
- One Tokio task owns the engine and its accounts. Handlers reach it only
  by sending a command and waiting for the reply.
- State where only the latest value matters (books, marks) goes through
  watch channels. Events that must not be lost go through a bounded queue,
  so a slow consumer slows the engine instead of dropping an event.
- The event log holds what happened to accounts, not the marks that
  caused it. Replaying it rebuilds collateral, positions and state exactly.
  It does not re-run the decisions, because mark prices are not recorded.
  The database rejects any update, delete or truncate on the table.
- An API reply can reach the client before its event is committed to
  Postgres. A crash in that window loses the event.
- Prices and sizes are `rust_decimal::Decimal`, parsed from the venue's
  strings. No binary floats.

## Metrics

| Metric | Meaning |
| ------ | ------- |
| `feed_to_decision_seconds{venue}` | Histogram. Starts when a mark price frame is read off the socket. Stops when the engine has rechecked every account against that price |
| `engine_events_total{kind}` | Account events the engine emitted |
| `feed_reconnects_total{venue}` | Venue sessions that ended and were reconnected |
| `book_resyncs_total{venue}` | Books cleared and resubscribed after a desync |
| `event_log_write_seconds` | Histogram. Time to commit one batch of events |
| `event_log_write_failures_total` | Event log writes that failed and were retried |

- `feed_to_decision_seconds` covers this process only: decoding the frame,
  the hand-off to the engine task, and the margin check. It does not
  include the network path from the venue or the time to write the
  resulting events.
- A mark that is replaced before the engine wakes is never applied, so it
  is not in the histogram. A frame that repeats the current price is not
  in it either.

## Test

```sh
cargo test
```

- Unit tests for book mutation and frame decoding.
- `tests/replay.rs` replays a recorded BTC session and checks ordering,
  depth, and uncrossed invariants after every message.
- `tests/connector.rs` runs the connector against a local fake venue to
  cover desync recovery, reconnect, and mark publication.
- `tests/replay_okx.rs` replays 120 recorded OKX frames and checks the
  sequence chain and the same book invariants.
- `tests/connector_okx.rs` runs the OKX connector against a local fake
  venue: sizes in coin, a sequence gap, and a reconnect.
- `tests/risk_props.rs` holds property tests for the risk core: health is
  additive over positions, margin available is zero at the liquidation
  price, fills at the mark conserve equity, and the state machine always
  matches the latest health reading.
- `tests/venue_parity.rs` compares the risk core with Hyperliquid's own
  numbers for 11 real accounts recorded from its public API on 2026-11-22
  (addresses removed). Maintenance margin and every liquidation price must
  match. The sample holds only accounts with BTC and ETH cross positions and
  no collateral outside the perp balance.
- `tests/restore_props.rs` runs random histories of accounts, fills and
  mark moves on both venues, rebuilds a second engine from the events
  alone, and requires it to report every account exactly as the first one
  does. It also requires a log cut off at any event to restore.
- `tests/event_store.rs` runs the store against a real Postgres: restore
  after a restart, exact decimals, all-or-nothing appends, and rejected
  updates and deletes. It needs `TEST_DATABASE_URL` and reports a skip
  without it:
  `TEST_DATABASE_URL=postgres://postgres@127.0.0.1:5432/postgres cargo test`
- `tests/engine.rs` runs the engine task against hand-driven mark channels:
  liquidation on a mark move, a withdrawn mark, and a dead connector.

## Roadmap

1. Hyperliquid connector, book, API. Done.
2. Risk engine: paper accounts, account health, liquidation price, and an
   explicit account state machine. Done.
3. Second venue, an append-only event log in Postgres, and rebuilding
   state from that log. Done.
4. Docker image, metrics for feed-to-decision latency, deployment.
