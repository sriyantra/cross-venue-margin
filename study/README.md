# Is 0.25% enough for a hedged pair?

The engine margins the matched part of a long on one venue and a short on
the other at `HEDGE_MARGIN_RATE`, 0.25% of each leg's notional. That number
was a guess. This measures it against recorded prices.

A matched pair does not care where the price goes. It loses money only
when the gap between the two venues' prices moves against it. Each leg
pays 0.25%, so at maintenance margin the pair holds 50 basis points of one
leg's notional, and 100 at initial margin. A pair found at maintenance
leaves a shortfall if the gap moves more than 50 basis points before both
legs are closed.

## Data

`data/` holds closing prices of the BTC and ETH perpetuals on both venues
at the same times, 4,999 candles per file: 1-minute candles over 3 days,
5-minute over 17 days and 1-hour over 208 days (2026-05-03 to 2026-11-27).
`python3 study/fetch.py` records a fresh set. Hyperliquid serves the most
recent 5,000 candles of each length, which sets the spans.

```sh
cargo run --release --example hedge_study
```

## The gap

`okx / hyperliquid - 1`, in basis points:

| Coin | Candle | Covers | Median | 1% | 99% | Min | Max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| BTC | 1m | 3 d | -0.1 | -4.9 | 3.8 | -14.4 | 6.7 |
| BTC | 5m | 17 d | -0.6 | -4.5 | 2.9 | -15.8 | 5.5 |
| BTC | 1h | 208 d | -0.9 | -8.9 | 2.7 | -15.6 | 10.7 |
| ETH | 1m | 3 d | 0.1 | -4.9 | 4.1 | -12.8 | 7.5 |
| ETH | 5m | 17 d | -0.2 | -4.4 | 4.0 | -16.1 | 8.4 |
| ETH | 1h | 208 d | -1.1 | -10.6 | 3.3 | -23.2 | 15.2 |

## How far the gap moves

The change in the gap over a window, in basis points. The last column
counts windows where it moved more than the 50 a pair holds at maintenance.

| Coin | Candle | Window | 99% | 99.9% | Largest | Over 50 |
| --- | --- | --- | --- | --- | --- | --- |
| BTC | 1m | 1 min | 4.7 | 7.6 | 15.6 | 0 |
| BTC | 1m | 24 min | 5.3 | 7.7 | 15.5 | 0 |
| BTC | 5m | 5 min | 4.3 | 7.5 | 15.1 | 0 |
| BTC | 5m | 2 h | 5.2 | 7.6 | 15.3 | 0 |
| BTC | 1h | 1 h | 4.4 | 6.7 | 14.1 | 0 |
| BTC | 1h | 1 d | 7.4 | 11.4 | 13.0 | 0 |
| ETH | 1m | 1 min | 6.2 | 9.9 | 13.3 | 0 |
| ETH | 1m | 24 min | 6.5 | 9.3 | 11.9 | 0 |
| ETH | 5m | 5 min | 6.1 | 10.4 | 19.3 | 0 |
| ETH | 5m | 2 h | 6.2 | 9.5 | 14.7 | 0 |
| ETH | 1h | 1 h | 5.5 | 11.2 | 17.6 | 0 |
| ETH | 1h | 1 d | 8.8 | 13.6 | 18.5 | 0 |

The example prints every window. The largest move in any of them is 19.3
basis points.

The gap does not follow the price. In the hour each coin moved most over
the 208 days, the gap moved little:

| Coin | Hour (UTC) | Price move | Gap change (bp) |
| --- | --- | --- | --- |
| BTC | 2026-06-25 13:00 | -4.87% | 5.2 |
| ETH | 2026-06-25 13:00 | -6.00% | 3.3 |
| ETH | 2026-08-19 15:00 | +5.85% | 1.3 |

## What this says

Over these 208 days no window moved the gap by more than 19.3 basis points,
against the 50 a pair holds at maintenance and the 100 it must hold to
open. In this data 0.25% was never short, with room of about 2.5 times.

## What it does not say

- The period has no day on which one venue halted, lost its price feed or
  traded far from the other. The largest hourly move is 6%. A rate set
  from calm data says nothing about that day, and that day is what the
  margin is for.
- These are last-trade closes, not the mark prices the engine margins on,
  and a close misses whatever happened inside the candle. Only the 3 days
  of 1-minute candles look inside an hour.
- The OKX contract is quoted in USDT and the Hyperliquid one in USD. The
  gap above includes the USDT rate. The engine has no USDT rate, so a
  larger move in it than this period held would reach a hedged pair as a
  gap this study never saw.
- Closing both legs takes time and crosses two books. The cost of that is
  not in a price series.
