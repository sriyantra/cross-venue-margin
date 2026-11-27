# cross-venue-margin

One margin account across Hyperliquid and OKX: one pool of collateral,
hedged positions netted, and margin and liquidation prices that match
Hyperliquid's own for 11 real accounts.

A Rust service that keeps live order books and mark prices from Hyperliquid
and OKX and runs a risk engine over accounts whose positions sit on both,
backed by a single pool of collateral in USDC, BTC or ETH. A long on one
venue against a short on the other is margined as a hedge, not as two
outright positions. Every account event is appended to Postgres, and the
service rebuilds its state from that log when it starts.

Accounts trade on paper against the live books. With a testnet key, orders
on Hyperliquid markets are signed and sent to the venue's testnet instead.

## Try it

A read-only instance runs this repository's image:

```sh
BASE=https://margin-production-1f4db33a.up.railway.app
curl -s $BASE/accounts/1        # long BTC on one venue, short on the other
curl -s $BASE/book/okx/BTC
curl -s $BASE/metrics
```

Locally:

```sh
cargo run --release             # live feeds, accounts kept in memory
docker compose up --build       # the same, with Postgres for the event log
```

```sh
curl -s -H 'content-type: application/json' localhost:8080/accounts \
  -d '{"collateral":"5000"}'
curl -s -H 'content-type: application/json' localhost:8080/accounts/1/deposits \
  -d '{"asset":"BTC","amount":"0.5"}'
curl -s -H 'content-type: application/json' localhost:8080/accounts/1/fills \
  -d '{"venue":"hyperliquid","coin":"BTC","side":"long","size":"0.5"}'
curl -s -H 'content-type: application/json' localhost:8080/accounts/1/fills \
  -d '{"venue":"okx","coin":"BTC","side":"short","size":"0.5"}'
```

The second fill lowers the account's margin requirement, because the two
positions now hedge each other.

## Design

```
venue task (owns socket + books) --watch: book, mark--> API handlers
                                 --watch: book, mark--> engine task (owns accounts)
API handlers --bounded mpsc: commands, one-shot replies--> engine task
engine task  --bounded mpsc: events--> log writer --> Postgres (append-only)
Postgres     --events in order, at startup--> engine
```

- Each venue connector is one Tokio task that owns its socket and its
  books. One more task owns the engine and every account. Nothing is
  shared, so there are no locks around books or accounts.
- Books and marks go through `watch` channels, which keep only the latest
  value. A slow reader skips ahead and cannot build a backlog. Events go
  through a bounded queue, so a slow database slows the engine down and
  never drops an event.
- `book`, `risk` and `engine` have no I/O and no clock. The engine takes
  the time as an argument and returns the events it caused, which is what
  makes it testable by replay and by property tests.
- A book that fails a consistency check (a diff that does not apply, a
  crossed book, a sequence gap) is cleared and resubscribed. Until it is
  rebuilt the API reports it as stale, and a mark the venue stops
  refreshing is withdrawn. An account with a missing mark is frozen, not
  liquidated on an old price.
- Prices and sizes are decimals parsed from the venue's strings. Nothing
  passes through a binary float.

## Margin model

|             | Hyperliquid                                   | OKX                                              |
| ----------- | --------------------------------------------- | ------------------------------------------------ |
| Maintenance | Tiers by notional: `notional * rate - deduction` | Tiers by size: the tier's rate on the whole notional |
| Initial     | Twice the maintenance rate                    | The venue's published rate per tier              |
| Markets     | BTC, ETH                                      | BTC, ETH USDT perpetuals, first 12 tiers         |

- Equity is the dollar balance, plus coins held as collateral, plus
  unrealized profit and loss at each venue's own mark.
- Collateral in BTC or ETH stays a coin. It counts for
  `amount * mark * (1 - haircut)`, with haircuts of 10% and 15%, and is
  revalued on every mark. A long backed by its own coin liquidates sooner.
  A short covered by the coin has no liquidation price.
- The part of a position matched by an opposite position in
  the same coin on the other venue pays a hedge rate (0.25% of notional,
  `HEDGE_MARGIN_RATE`) for the gap between the two marks. The rest pays
  its venue's table.
- Fills walk the live book and take the average price, and pay the
  venue's base taker fee (0.045% on Hyperliquid, 0.05% on OKX). An order
  larger than the book, or one that would leave equity below initial
  margin, is refused.
- Funding is charged when each venue settles it: hourly on Hyperliquid,
  at the venue's announced time on OKX. A position pays or receives
  `size * mark * rate` in dollars at the rate the venue published.
- An account moves through open, margin call, liquidating
  and closed. Below maintenance margin every position is closed against
  the book.

The Hyperliquid numbers are checked against the venue's own: maintenance
margin and every liquidation price match for 11 real accounts recorded
from its public API. OKX account data is not public, so those numbers are
checked against its documented formula.

Not modeled: fee tiers and maker fees, OKX's liquidation fee, a USDT/USD rate,
offsets between different coins, the time it takes to move collateral
between venues, staged liquidation, an insurance fund, and selling coin
collateral. The haircuts and the hedge rate are my own numbers.
[`study/`](study/README.md) checks the hedge rate against 208 days of
prices from both venues: the gap between them never moved more than 19.3
basis points in any window, against the 50 a hedged pair holds at
maintenance. That period has no day on which one venue broke away. Paper
fills do not move the book. Funding is charged on the mark, where
Hyperliquid uses its oracle price, and a settlement that passes while the
service is down is not charged afterwards.

## Testnet execution

```sh
HL_TESTNET_KEY=0x... cargo run --release
```

With a key, a fill on a Hyperliquid market becomes a real order:

1. The engine checks the order against the account, the margin rules and
   the testnet book, and logs it under a new id. No position changes yet.
2. The service signs an immediate-or-cancel limit order, 5% through the
   expected price, and sends it with that id.
3. The engine records the size and average price the venue reports.

If the answer is lost, the order stays open and its account takes no other
fill. A repair task asks the venue about every open order by its id, at
startup and every 15 seconds. A fill is recorded on the account that sent
the order, and an order that did not fill is dropped.

The engine task never waits on the venue: the API handler makes the call
between the two engine commands. All accounts trade through one wallet, and
`GET /venues/hyperliquid/reconciliation` compares the sum of their
positions with what the wallet holds and lists the open orders. A position the engine liquidates is
closed on the venue by a separate task.

Signing (MessagePack action hash, EIP-712, secp256k1) is tested against the
venue's reference client: same key, order and nonce, same signature. The
signature is bound to testnet.

Limits: OKX fills stay on paper. A liquidation close whose answer is lost
is reported by reconciliation and not repaired. An order the venue does
not know 60 seconds after it was sent is taken as never sent. An order is
logged before it is sent, but the engine does not wait for that write to
reach Postgres. Accounts pay the venue's base taker rate, not the fee the venue
reports for each fill. Hyperliquid prices come from testnet in this mode,
and testnet is not mainnet.

## Configuration

| Variable               | Default          | Meaning |
| ---------------------- | ---------------- | ------- |
| `COINS`                | `BTC,ETH`        | Coins to track on each venue that lists them |
| `BIND`                 | `127.0.0.1:8080` | API listen address |
| `DATABASE_URL`         | unset            | Postgres URL for the event log. Without it accounts are lost on restart |
| `MARGIN_CALL_MULTIPLE` | `1.5`            | A margin call starts when equity is below maintenance margin times this |
| `HEDGE_MARGIN_RATE`    | `0.0025`         | Maintenance rate on the matched part of a hedged pair. `off` for gross margin |
| `READ_ONLY`            | `false`          | Refuse every route that changes an account |
| `HL_TESTNET_KEY`       | unset            | Testnet wallet key. Turns on testnet execution |
| `HL_WS_URL`            | mainnet          | Hyperliquid WebSocket URL. Cannot be set with `HL_TESTNET_KEY` |
| `OKX_WS_URL`           | mainnet          | OKX public WebSocket URL |

## API

| Route | Returns |
| ----- | ------- |
| `GET /health` | Liveness and age of every book and mark. 503 if any is stale |
| `GET /book/{venue}/{coin}` | The current book |
| `GET /mark/{venue}/{coin}` | The venue's mark price |
| `GET /ws/{venue}/{coin}` | WebSocket: the current book, then every update |
| `POST /accounts` | Open an account. `{"collateral":"5000"}` |
| `GET /accounts/{id}` | State, health, collateral and positions with liquidation prices |
| `POST /accounts/{id}/deposits` | Add collateral. `{"asset":"BTC","amount":"0.5"}` |
| `POST /accounts/{id}/fills` | Fill an order. `{"venue":"okx","coin":"BTC","side":"short","size":"0.5"}` |
| `GET /venues/hyperliquid/reconciliation` | Engine positions against the testnet wallet, and orders with no answer yet |
| `GET /metrics` | Prometheus metrics |

`/metrics` includes `feed_to_decision_seconds`, a histogram from a mark
price frame being read off the socket to the engine having rechecked every
account against it. It covers this process only, not the network path.

The engine keeps, per market, the accounts its mark can change, so a mark
costs what its holders cost. `cargo bench` on an Apple M5 Pro,
100,000 accounts: 0.11 ms for a mark when 1 account in 100 holds the
market (2.3 ms before the index), 12 ms when every account does.

## Tests

```sh
cargo test
TEST_DATABASE_URL=postgres://postgres@127.0.0.1:5432/postgres cargo test
```

- Sessions recorded from both venues are replayed through the
  book code, with ordering and uncrossed-book checks after every message.
- Margin and liquidation prices are compared with Hyperliquid's own
  numbers for 11 recorded accounts.
- Property tests: margin available is zero at the liquidation price,
  netting never raises a requirement, and an engine rebuilt from its own
  event log reports every account exactly as the original does.
- The connectors run against local fake venues: desync recovery, sequence
  gaps, reconnects.
- Execution runs the API against a stand-in venue: the order sent,
  partial fills, refusals from either side, lost answers, reconciliation.
- The event store runs against a real Postgres when `TEST_DATABASE_URL` is
  set: restore after restart, all-or-nothing appends, rejected updates and
  deletes.

## License

MIT
