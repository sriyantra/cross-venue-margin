//! The hand-off between venue connectors and everything that reads books
//! and mark prices.
//!
//! Each market gets two `tokio::sync::watch` channels, one for its book and
//! one for its mark price. A watch channel holds a single value, the latest
//! one. The connector overwrites it after every applied message, and any
//! number of readers can look at the current value or wait for the next
//! change. A slow reader never builds a queue: it just skips straight to the
//! newest value, which is the only one worth acting on.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use rust_decimal::Decimal;
use serde::Serialize;
use tokio::sync::watch;
use tokio::time::Instant;

use crate::book::{Book, Level};
use crate::risk::{MarketId, Venue};

/// An immutable copy of a book at one moment, in a venue-neutral shape.
/// Sizes are in units of the coin on every venue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BookView {
    pub venue: Venue,
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
    pub fn from_book(venue: Venue, book: &Book) -> Option<Self> {
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

/// The venue's own mark price for one market. This is the price the venue
/// uses for margin and liquidation, which is not the book's mid.
#[derive(Debug, Clone, Serialize)]
pub struct MarkView {
    pub venue: Venue,
    pub coin: String,
    pub mark: Decimal,
    /// When the frame that carried this price was read off the socket.
    #[serde(skip)]
    pub received: Instant,
}

/// `None` means there is no trustworthy mark: not yet received,
/// disconnected, or not refreshed recently enough.
pub type MarkSlot = Option<MarkView>;
pub type MarkTx = watch::Sender<MarkSlot>;
pub type MarkRx = watch::Receiver<MarkSlot>;

/// Write side for one market, owned by that market's venue connector.
#[derive(Debug)]
pub struct Publisher {
    pub book: BookTx,
    pub mark: MarkTx,
}

/// Write sides for every market, grouped by the venue whose connector will
/// own them and keyed by coin.
#[derive(Debug, Default)]
pub struct Publishers {
    by_venue: HashMap<Venue, HashMap<String, Publisher>>,
}

impl Publishers {
    /// Hands one venue's publishers to its connector. A second call for the
    /// same venue returns an empty map.
    pub fn take(&mut self, venue: Venue) -> HashMap<String, Publisher> {
        self.by_venue.remove(&venue).unwrap_or_default()
    }
}

/// Read side: one receiver pair per market, shared by the API handlers.
#[derive(Debug, Clone, Default)]
pub struct Feeds {
    books: BTreeMap<MarketId, BookRx>,
    marks: BTreeMap<MarketId, MarkRx>,
}

impl Feeds {
    /// Creates the channels for each market. Returns the read side and the
    /// write side separately so each can be moved into the task that owns it.
    pub fn channels(markets: &[MarketId]) -> (Self, Publishers) {
        let mut feeds = Self::default();
        let mut publishers = Publishers::default();
        for market in markets {
            let (book, book_rx) = watch::channel(None);
            let (mark, mark_rx) = watch::channel(None);
            publishers
                .by_venue
                .entry(market.venue)
                .or_default()
                .insert(market.coin.clone(), Publisher { book, mark });
            feeds.books.insert(market.clone(), book_rx);
            feeds.marks.insert(market.clone(), mark_rx);
        }
        (feeds, publishers)
    }

    pub fn subscribe(&self, market: &MarketId) -> Option<BookRx> {
        self.books.get(market).cloned()
    }

    pub fn latest(&self, market: &MarketId) -> Option<Slot> {
        self.books.get(market).map(|rx| rx.borrow().clone())
    }

    pub fn subscribe_mark(&self, market: &MarketId) -> Option<MarkRx> {
        self.marks.get(market).cloned()
    }

    pub fn latest_mark(&self, market: &MarketId) -> Option<MarkSlot> {
        self.marks.get(market).map(|rx| rx.borrow().clone())
    }

    /// Every market's mark receiver, for a task that watches all of them.
    pub fn mark_receivers(&self) -> impl Iterator<Item = (&MarketId, MarkRx)> {
        self.marks.iter().map(|(market, rx)| (market, rx.clone()))
    }

    /// Every tracked market, ordered by venue then coin.
    pub fn markets(&self) -> impl Iterator<Item = &MarketId> {
        self.books.keys()
    }
}
