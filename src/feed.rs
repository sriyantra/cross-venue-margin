//! The hand-off between venue connectors and everything that reads books.
//!
//! Each market gets one `tokio::sync::watch` channel. A watch channel holds a
//! single value, the latest one. The connector overwrites it after every
//! applied message, and any number of readers can look at the current value
//! or wait for the next change. A slow reader never builds a queue: it just
//! skips straight to the newest book, which is the only one worth acting on.

use std::collections::HashMap;
use std::sync::Arc;

use rust_decimal::Decimal;
use serde::Serialize;
use tokio::sync::watch;

use crate::book::{Book, Level};

/// An immutable copy of a book at one moment, in a venue-neutral shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BookView {
    pub venue: &'static str,
    pub coin: String,
    /// Venue timestamp of the last applied message, in milliseconds.
    pub time: u64,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
    pub mid: Option<Decimal>,
}

impl BookView {
    /// Copies a live book. Returns `None` for a book that is waiting for a
    /// snapshot, so stale state cannot be published by accident.
    pub fn from_book(venue: &'static str, book: &Book) -> Option<Self> {
        Some(Self {
            venue,
            coin: book.coin().to_string(),
            time: book.time()?,
            bids: book.bids().to_vec(),
            asks: book.asks().to_vec(),
            mid: book.mid(),
        })
    }
}

/// `None` means the book is not currently trustworthy: not yet received,
/// disconnected, or being rebuilt after a desync.
pub type Slot = Option<Arc<BookView>>;
pub type BookTx = watch::Sender<Slot>;
pub type BookRx = watch::Receiver<Slot>;

/// Read side: one receiver per market, shared by the API handlers.
#[derive(Debug, Clone, Default)]
pub struct Feeds {
    books: HashMap<String, BookRx>,
}

impl Feeds {
    /// Creates one channel per coin. Returns the read side and the write
    /// side separately so each can be moved into the task that owns it.
    pub fn channels(coins: &[String]) -> (Self, HashMap<String, BookTx>) {
        let mut books = HashMap::new();
        let mut publishers = HashMap::new();
        for coin in coins {
            let (tx, rx) = watch::channel(None);
            publishers.insert(coin.clone(), tx);
            books.insert(coin.clone(), rx);
        }
        (Self { books }, publishers)
    }

    pub fn subscribe(&self, coin: &str) -> Option<BookRx> {
        self.books.get(coin).cloned()
    }

    pub fn latest(&self, coin: &str) -> Option<Slot> {
        self.books.get(coin).map(|rx| rx.borrow().clone())
    }

    pub fn coins(&self) -> impl Iterator<Item = &str> {
        self.books.keys().map(String::as_str)
    }
}
