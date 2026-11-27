//! Measures how far the same perpetual's price on the two venues drifts
//! apart, from the closes recorded in `study/data`, and compares that with
//! the margin a hedged pair holds. Prints the tables in `study/README.md`.
//!
//! These are statistics over recorded prices, so they are in floats.

use std::fs;

/// The maintenance rate each leg of a hedged pair pays, as in
/// `HEDGE_MARGIN_RATE`. A pair holds twice this at maintenance and four
/// times this at initial margin, as a share of one leg's notional.
const HEDGE_RATE: f64 = 0.0025;
const FILES: [(&str, &str, u64); 6] = [
    ("BTC", "1m", 1),
    ("BTC", "5m", 5),
    ("BTC", "1h", 60),
    ("ETH", "1m", 1),
    ("ETH", "5m", 5),
    ("ETH", "1h", 60),
];
/// How many candles a gap is given to move, for each table row.
const STEPS: [usize; 4] = [1, 3, 12, 24];

/// The gap between the venues at each recorded time: `okx / hyperliquid - 1`.
fn gaps(coin: &str, interval: &str) -> Vec<f64> {
    let path = format!("study/data/{coin}_{interval}.csv");
    let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
    text.lines()
        .skip(1)
        .filter_map(|line| {
            let mut fields = line.split(',').skip(1);
            let ours: f64 = fields.next()?.parse().ok()?;
            let theirs: f64 = fields.next()?.parse().ok()?;
            Some(theirs / ours - 1.0)
        })
        .collect()
}

/// Hyperliquid's close at each recorded time.
fn closes(coin: &str, interval: &str) -> Vec<(u64, f64)> {
    let path = format!("study/data/{coin}_{interval}.csv");
    let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
    text.lines()
        .skip(1)
        .filter_map(|line| {
            let mut fields = line.split(',');
            Some((fields.next()?.parse().ok()?, fields.next()?.parse().ok()?))
        })
        .collect()
}

/// The value `share` of the way through `sorted`.
fn quantile(sorted: &[f64], share: f64) -> f64 {
    let last = sorted.len() - 1;
    sorted[((last as f64) * share).round() as usize]
}

fn sorted(mut values: Vec<f64>) -> Vec<f64> {
    values.sort_by(f64::total_cmp);
    values
}

fn bps(value: f64) -> String {
    format!("{:.1}", value * 10_000.0)
}

fn span(minutes: u64) -> String {
    match minutes {
        m if m < 60 => format!("{m} min"),
        m if m < 1_440 => format!("{} h", m / 60),
        m => format!("{} d", m / 1_440),
    }
}

fn main() {
    println!("Gap between the venues, in basis points of price:\n");
    println!("| Coin | Candle | Covers | Median | 1% | 99% | Min | Max |");
    println!("| --- | --- | --- | --- | --- | --- | --- | --- |");
    for (coin, interval, minutes) in FILES {
        let gaps = sorted(gaps(coin, interval));
        println!(
            "| {coin} | {interval} | {} | {} | {} | {} | {} | {} |",
            span(gaps.len() as u64 * minutes),
            bps(quantile(&gaps, 0.5)),
            bps(quantile(&gaps, 0.01)),
            bps(quantile(&gaps, 0.99)),
            bps(gaps[0]),
            bps(gaps[gaps.len() - 1]),
        );
    }

    let maintenance = 2.0 * HEDGE_RATE;
    println!("\nChange in the gap over a window, in basis points, and the share of");
    println!(
        "windows where it is more than the {} a pair holds at maintenance:\n",
        bps(maintenance)
    );
    println!(
        "| Coin | Window | Windows | 99% | 99.9% | Largest | Over {} |",
        bps(maintenance)
    );
    println!("| --- | --- | --- | --- | --- | --- | --- |");
    for (coin, interval, minutes) in FILES {
        let gaps = gaps(coin, interval);
        for steps in STEPS {
            let moves = sorted(
                gaps.windows(steps + 1)
                    .map(|window| (window[steps] - window[0]).abs())
                    .collect(),
            );
            let over = moves.iter().filter(|m| **m > maintenance).count();
            println!(
                "| {coin} | {} | {} | {} | {} | {} | {} |",
                span(steps as u64 * minutes),
                moves.len(),
                bps(quantile(&moves, 0.99)),
                bps(quantile(&moves, 0.999)),
                bps(moves[moves.len() - 1]),
                over,
            );
        }
    }

    println!("\nThe hours the price moved most, and the gap's change in each:\n");
    println!("| Coin | Hour starting (ms) | Price move | Gap change (bp) |");
    println!("| --- | --- | --- | --- |");
    for coin in ["BTC", "ETH"] {
        let closes = closes(coin, "1h");
        let gaps = gaps(coin, "1h");
        let mut hours: Vec<usize> = (1..closes.len()).collect();
        let change = |i: usize| closes[i].1 / closes[i - 1].1 - 1.0;
        hours.sort_by(|a, b| change(*b).abs().total_cmp(&change(*a).abs()));
        for &i in &hours[..3] {
            println!(
                "| {coin} | {} | {:+.2}% | {} |",
                closes[i].0,
                change(i) * 100.0,
                bps(gaps[i] - gaps[i - 1]),
            );
        }
    }
}
