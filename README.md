# cross-venue-margin

A Rust service that keeps live order books from more than one trading venue and
computes margin for accounts whose positions span those venues on a single
collateral balance.

Status: stage 2 of 4, in progress. One venue (Hyperliquid) is connected. The
risk calculations exist as a tested library but are not yet wired to the live
feed or the API. The second venue is not built yet.

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
- HTTP and WebSocket API over the current books and marks.
- Risk core (`src/risk`), library only for now: account health under gross
  cross margin, tiered maintenance schedules, liquidation price per position,
  and an account state machine (open, margin call, liquidating, closed).

## Run

```sh
cargo run --release
```

Environment:

| Variable    | Default                        | Meaning                    |
| ----------- | ------------------------------ | -------------------------- |
| `HL_WS_URL` | `wss://api.hyperliquid.xyz/ws` | Hyperliquid WebSocket URL  |
| `COINS`     | `BTC,ETH`                      | Markets to track           |
| `BIND`      | `127.0.0.1:8080`               | API listen address         |
| `RUST_LOG`  | `info`                         | Log filter                 |

## API

| Route              | Returns                                              |
| ------------------ | ---------------------------------------------------- |
| `GET /health`      | Per-market liveness and age. 503 if any book or mark is stale |
| `GET /book/{coin}` | The current book. 503 with `{"type":"stale"}` if not live |
| `GET /mark/{coin}` | The venue's mark price. 503 with `{"type":"stale"}` if not live |
| `GET /ws/{coin}`   | WebSocket: the current book, then every update       |

```sh
curl -s localhost:8080/book/BTC
```

## Design

```
venue task (owns socket + books) --watch channel per market--> API handlers
```

- `book` is pure. It has no I/O and no clock, so it is tested by replaying
  frames recorded from the live feed (`tests/fixtures`).
- Each venue connector is one Tokio task that owns its socket and its books.
  Nothing else can mutate a book, so there are no locks around book state.
- Books are published through `tokio::sync::watch`, which keeps only the
  latest value. A slow reader skips to the newest book and cannot build a
  backlog.
- Prices and sizes are `rust_decimal::Decimal`, parsed from the venue's
  strings. No binary floats.

## Test

```sh
cargo test
```

- Unit tests for book mutation and frame decoding.
- `tests/replay.rs` replays a recorded BTC session and checks ordering,
  depth, and uncrossed invariants after every message.
- `tests/connector.rs` runs the connector against a local fake venue to
  cover desync recovery, reconnect, and mark publication.
- `tests/risk_props.rs` holds property tests for the risk core: health is
  additive over positions, margin available is zero at the liquidation
  price, and the state machine always matches the latest health reading.

## Roadmap

1. Hyperliquid connector, book, API. Done.
2. Risk engine: paper accounts, account health, liquidation price, and an
   explicit position state machine.
3. Second venue (CEX perps), an append-only event log in Postgres, and
   deterministic replay from that log.
4. Docker image, metrics for feed-to-decision latency, deployment.
