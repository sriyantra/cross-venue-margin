//! L2 order book state.
//!
//! This module is pure: no I/O, no async, no clock. It takes decoded venue
//! messages and either produces a new book state or reports that the local
//! copy can no longer be trusted. Everything here can be tested by replaying
//! recorded frames.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// One price level: a price and the total resting size at that price.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Level {
    pub px: Decimal,
    pub sz: Decimal,
}

/// The average price of taking `size` from `levels`, best level first.
///
/// With `partial` unset the answer is `None` when the levels hold less than
/// `size`. With it set, whatever the levels cannot fill is priced at the
/// last level, which is the worst price the book shows.
pub fn average_price(levels: &[Level], size: Decimal, partial: bool) -> Option<Decimal> {
    if size <= Decimal::ZERO {
        return None;
    }
    let mut left = size;
    let mut cost = Decimal::ZERO;
    for level in levels {
        let take = level.sz.min(left);
        cost = cost.checked_add(take.checked_mul(level.px)?)?;
        left -= take;
        if left.is_zero() {
            break;
        }
    }
    if left > Decimal::ZERO {
        if !partial {
            return None;
        }
        cost = cost.checked_add(left.checked_mul(levels.last()?.px)?)?;
    }
    cost.checked_div(size)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Bid,
    Ask,
}

/// A change to one side of the book, in the form Hyperliquid's `l2` channel
/// sends it: indexes of levels to drop from the previous book, then levels to
/// insert or overwrite by price.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SideDiff {
    pub removed: Vec<usize>,
    pub upserts: Vec<Level>,
}

/// Why a diff could not be applied. Any of these means the local book has
/// drifted from the venue's book and must be rebuilt from a fresh snapshot.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum Desync {
    #[error("diff arrived before any snapshot")]
    NoSnapshot,
    #[error("{side:?} removal index {index} is out of bounds for {len} levels")]
    RemovalOutOfBounds {
        side: Side,
        index: usize,
        len: usize,
    },
    #[error("diff time {diff} is older than book time {book}")]
    TimeWentBackwards { book: u64, diff: u64 },
    #[error("book is crossed: best bid {bid} >= best ask {ask}")]
    Crossed { bid: Decimal, ask: Decimal },
}

/// One side of the book, stored best price first.
///
/// Bids are sorted descending and asks ascending, which is the order the
/// venue uses and the order its removal indexes refer to. The venue sends at
/// most 20 levels per side, so a sorted `Vec` is the right structure: it is
/// contiguous in memory, and a binary search plus a short shift costs less
/// than a tree at this size.
#[derive(Debug, Clone, PartialEq, Eq)]
struct BookSide {
    side: Side,
    levels: Vec<Level>,
}

impl BookSide {
    fn new(side: Side) -> Self {
        Self {
            side,
            levels: Vec::new(),
        }
    }

    fn replace(&mut self, mut levels: Vec<Level>) {
        levels.retain(|l| !l.sz.is_zero());
        match self.side {
            Side::Bid => levels.sort_by_key(|l| std::cmp::Reverse(l.px)),
            Side::Ask => levels.sort_by_key(|a| a.px),
        }
        levels.dedup_by(|a, b| a.px == b.px);
        self.levels = levels;
    }

    /// Finds where `px` is, or where it would be inserted to keep the side
    /// sorted best price first.
    fn position(&self, px: Decimal) -> Result<usize, usize> {
        match self.side {
            Side::Bid => self.levels.binary_search_by(|l| px.cmp(&l.px)),
            Side::Ask => self.levels.binary_search_by(|l| l.px.cmp(&px)),
        }
    }

    fn apply(&mut self, diff: &SideDiff) -> Result<(), Desync> {
        // Removal indexes refer to positions in the book as it was before this
        // diff. Validate all of them first so a bad diff changes nothing.
        let len = self.levels.len();
        if let Some(&index) = diff.removed.iter().find(|&&i| i >= len) {
            return Err(Desync::RemovalOutOfBounds {
                side: self.side,
                index,
                len,
            });
        }

        // Remove from the highest index down. Removing a low index first would
        // shift every later level and make the remaining indexes point at the
        // wrong levels.
        let mut removed = diff.removed.clone();
        removed.sort_unstable();
        removed.dedup();
        for index in removed.into_iter().rev() {
            self.levels.remove(index);
        }

        for level in &diff.upserts {
            match self.position(level.px) {
                Ok(i) if level.sz.is_zero() => {
                    self.levels.remove(i);
                }
                Ok(i) => self.levels[i].sz = level.sz,
                Err(_) if level.sz.is_zero() => {}
                Err(i) => self.levels.insert(i, *level),
            }
        }
        Ok(())
    }
}

/// The local copy of one market's order book.
///
/// The book is a small state machine. It starts empty, becomes live on the
/// first snapshot, and stays live while every diff applies cleanly. Any
/// failed diff clears it, so a caller can never read a book that is known to
/// be wrong.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Book {
    coin: String,
    state: State,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum State {
    AwaitingSnapshot,
    Live {
        time: u64,
        bids: BookSide,
        asks: BookSide,
    },
}

impl Book {
    pub fn new(coin: impl Into<String>) -> Self {
        Self {
            coin: coin.into(),
            state: State::AwaitingSnapshot,
        }
    }

    pub fn coin(&self) -> &str {
        &self.coin
    }

    pub fn is_live(&self) -> bool {
        matches!(self.state, State::Live { .. })
    }

    /// Drops the current state. The next message must be a snapshot.
    pub fn reset(&mut self) {
        self.state = State::AwaitingSnapshot;
    }

    /// Replaces the whole book. A snapshot is always accepted: it is the
    /// venue's full statement of the book and does not depend on local state.
    pub fn apply_snapshot(
        &mut self,
        time: u64,
        bids: Vec<Level>,
        asks: Vec<Level>,
    ) -> Result<(), Desync> {
        let mut bid_side = BookSide::new(Side::Bid);
        let mut ask_side = BookSide::new(Side::Ask);
        bid_side.replace(bids);
        ask_side.replace(asks);
        self.state = State::Live {
            time,
            bids: bid_side,
            asks: ask_side,
        };
        self.check_uncrossed()
    }

    /// Applies one diff on top of the current book.
    ///
    /// On any error the book is reset before returning, so the only two
    /// outcomes are "book advanced by exactly this diff" and "book is empty
    /// and waiting for a snapshot".
    pub fn apply_diff(
        &mut self,
        time: u64,
        bids: &SideDiff,
        asks: &SideDiff,
    ) -> Result<(), Desync> {
        let result = self.try_apply_diff(time, bids, asks);
        if result.is_err() {
            self.reset();
        }
        result
    }

    fn try_apply_diff(
        &mut self,
        time: u64,
        bids: &SideDiff,
        asks: &SideDiff,
    ) -> Result<(), Desync> {
        let State::Live {
            time: book_time,
            bids: bid_side,
            asks: ask_side,
        } = &mut self.state
        else {
            return Err(Desync::NoSnapshot);
        };
        if time < *book_time {
            return Err(Desync::TimeWentBackwards {
                book: *book_time,
                diff: time,
            });
        }
        bid_side.apply(bids)?;
        ask_side.apply(asks)?;
        *book_time = time;
        self.check_uncrossed()
    }

    fn check_uncrossed(&mut self) -> Result<(), Desync> {
        if let (Some(bid), Some(ask)) = (self.best_bid(), self.best_ask())
            && bid.px >= ask.px
        {
            let err = Desync::Crossed {
                bid: bid.px,
                ask: ask.px,
            };
            self.reset();
            return Err(err);
        }
        Ok(())
    }

    pub fn time(&self) -> Option<u64> {
        match &self.state {
            State::Live { time, .. } => Some(*time),
            State::AwaitingSnapshot => None,
        }
    }

    /// Bids, best (highest) price first. Empty until the first snapshot.
    pub fn bids(&self) -> &[Level] {
        match &self.state {
            State::Live { bids, .. } => &bids.levels,
            State::AwaitingSnapshot => &[],
        }
    }

    /// Asks, best (lowest) price first. Empty until the first snapshot.
    pub fn asks(&self) -> &[Level] {
        match &self.state {
            State::Live { asks, .. } => &asks.levels,
            State::AwaitingSnapshot => &[],
        }
    }

    pub fn best_bid(&self) -> Option<Level> {
        self.bids().first().copied()
    }

    pub fn best_ask(&self) -> Option<Level> {
        self.asks().first().copied()
    }

    /// Midpoint of best bid and best ask, when both sides have a level.
    pub fn mid(&self) -> Option<Decimal> {
        let (bid, ask) = (self.best_bid()?, self.best_ask()?);
        Some((bid.px + ask.px) / Decimal::TWO)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::dec;

    fn lv(px: Decimal, sz: Decimal) -> Level {
        Level { px, sz }
    }

    fn live_book() -> Book {
        let mut book = Book::new("BTC");
        book.apply_snapshot(
            100,
            vec![
                lv(dec!(99), dec!(1)),
                lv(dec!(98), dec!(2)),
                lv(dec!(97), dec!(3)),
            ],
            vec![
                lv(dec!(101), dec!(1)),
                lv(dec!(102), dec!(2)),
                lv(dec!(103), dec!(3)),
            ],
        )
        .unwrap();
        book
    }

    fn none() -> SideDiff {
        SideDiff::default()
    }

    #[test]
    fn snapshot_sorts_each_side_best_first() {
        let mut book = Book::new("BTC");
        book.apply_snapshot(
            1,
            vec![
                lv(dec!(97), dec!(1)),
                lv(dec!(99), dec!(1)),
                lv(dec!(98), dec!(1)),
            ],
            vec![
                lv(dec!(103), dec!(1)),
                lv(dec!(101), dec!(1)),
                lv(dec!(102), dec!(1)),
            ],
        )
        .unwrap();
        let bids: Vec<_> = book.bids().iter().map(|l| l.px).collect();
        let asks: Vec<_> = book.asks().iter().map(|l| l.px).collect();
        assert_eq!(bids, [dec!(99), dec!(98), dec!(97)]);
        assert_eq!(asks, [dec!(101), dec!(102), dec!(103)]);
        assert_eq!(book.mid(), Some(dec!(100)));
    }

    #[test]
    fn diff_before_snapshot_is_a_desync() {
        let mut book = Book::new("BTC");
        assert_eq!(
            book.apply_diff(1, &none(), &none()),
            Err(Desync::NoSnapshot)
        );
        assert!(!book.is_live());
    }

    #[test]
    fn removals_use_indexes_from_the_previous_book() {
        let mut book = live_book();
        // Remove bid indexes 0 and 2 (99 and 97). If index 0 were removed
        // first and index 2 looked up afterwards, it would be out of bounds.
        let bids = SideDiff {
            removed: vec![0, 2],
            upserts: vec![],
        };
        book.apply_diff(101, &bids, &none()).unwrap();
        assert_eq!(book.bids(), [lv(dec!(98), dec!(2))]);
    }

    #[test]
    fn upsert_overwrites_existing_price_and_inserts_new_price_in_order() {
        let mut book = live_book();
        let bids = SideDiff {
            removed: vec![],
            upserts: vec![lv(dec!(98), dec!(9)), lv(dec!(98.5), dec!(4))],
        };
        let asks = SideDiff {
            removed: vec![],
            upserts: vec![lv(dec!(100.5), dec!(7))],
        };
        book.apply_diff(101, &bids, &asks).unwrap();
        let bids: Vec<_> = book.bids().iter().map(|l| (l.px, l.sz)).collect();
        assert_eq!(
            bids,
            [
                (dec!(99), dec!(1)),
                (dec!(98.5), dec!(4)),
                (dec!(98), dec!(9)),
                (dec!(97), dec!(3))
            ]
        );
        assert_eq!(book.best_ask(), Some(lv(dec!(100.5), dec!(7))));
    }

    #[test]
    fn out_of_bounds_removal_resets_the_book() {
        let mut book = live_book();
        let bids = SideDiff {
            removed: vec![3],
            upserts: vec![],
        };
        assert_eq!(
            book.apply_diff(101, &bids, &none()),
            Err(Desync::RemovalOutOfBounds {
                side: Side::Bid,
                index: 3,
                len: 3
            })
        );
        assert!(!book.is_live());
        assert!(book.bids().is_empty());
    }

    #[test]
    fn crossed_book_resets() {
        let mut book = live_book();
        let bids = SideDiff {
            removed: vec![],
            upserts: vec![lv(dec!(101), dec!(1))],
        };
        assert_eq!(
            book.apply_diff(101, &bids, &none()),
            Err(Desync::Crossed {
                bid: dec!(101),
                ask: dec!(101)
            })
        );
        assert!(!book.is_live());
    }

    #[test]
    fn older_diff_is_rejected() {
        let mut book = live_book();
        assert_eq!(
            book.apply_diff(99, &none(), &none()),
            Err(Desync::TimeWentBackwards {
                book: 100,
                diff: 99
            })
        );
    }

    #[test]
    fn snapshot_recovers_a_reset_book() {
        let mut book = live_book();
        book.reset();
        book.apply_snapshot(
            200,
            vec![lv(dec!(50), dec!(1))],
            vec![lv(dec!(51), dec!(1))],
        )
        .unwrap();
        assert!(book.is_live());
        assert_eq!(book.time(), Some(200));
    }

    #[test]
    fn average_price_walks_the_levels() {
        let level = |px, sz| Level { px, sz };
        let asks = [
            level(dec!(100), dec!(1)),
            level(dec!(101), dec!(2)),
            level(dec!(103), dec!(5)),
        ];
        // Inside the first level.
        assert_eq!(average_price(&asks, dec!(0.5), false), Some(dec!(100)));
        // 1 at 100 and 1 at 101.
        assert_eq!(average_price(&asks, dec!(2), false), Some(dec!(100.5)));
        // Exactly the whole side: 100 + 202 + 515 over 8.
        assert_eq!(average_price(&asks, dec!(8), false), Some(dec!(102.125)));
        // More than the side holds.
        assert_eq!(average_price(&asks, dec!(9), false), None);
        // The same, with the last unit priced at the last level.
        assert_eq!(
            average_price(&asks, dec!(9), true),
            Some((dec!(817) + dec!(103)) / dec!(9))
        );
        assert_eq!(average_price(&[], dec!(1), true), None);
        assert_eq!(average_price(&asks, dec!(0), false), None);
    }
}
