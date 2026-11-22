//! One market's state inside a connector task. Every venue's connector
//! keeps the same things per market: a book, the channels it publishes to,
//! and the timers that decide when a resubscribe is allowed and when a mark
//! is too old to show.

use std::sync::Arc;
use std::time::Duration;

use rust_decimal::Decimal;
use tokio::time::Instant;
use tracing::warn;

use crate::book::Book;
use crate::feed::{BookView, MarkView, Publisher};
use crate::risk::Venue;

/// A mark price that has not been refreshed for this long is withdrawn.
pub(crate) const MARK_STALE_AFTER: Duration = Duration::from_secs(10);
/// How often a connector looks for marks that have gone stale.
pub(crate) const MARK_CHECK_EVERY: Duration = Duration::from_secs(1);
/// Minimum gap between resubscribes for one market.
const RESYNC_THROTTLE: Duration = Duration::from_secs(1);

pub(crate) struct Market {
    venue: Venue,
    pub(crate) book: Book,
    publisher: Publisher,
    last_resync: Option<Instant>,
    /// When the current mark arrived. `None` while no mark is published.
    last_mark: Option<Instant>,
}

impl Market {
    pub(crate) fn new(venue: Venue, coin: String, publisher: Publisher) -> Self {
        Self {
            venue,
            book: Book::new(coin),
            publisher,
            last_resync: None,
            last_mark: None,
        }
    }

    pub(crate) fn publish(&self) {
        let view = BookView::from_book(self.venue, &self.book).map(Arc::new);
        // `send_replace` stores the value even when nobody is listening yet.
        self.publisher.book.send_replace(view);
    }

    pub(crate) fn invalidate(&mut self) {
        self.book.reset();
        self.publisher.book.send_replace(None);
    }

    /// Clears the book and reports whether a resubscribe should be sent
    /// now. A market that was resubscribed a moment ago is left to finish.
    pub(crate) fn resync(&mut self) -> bool {
        self.invalidate();
        let now = Instant::now();
        if self
            .last_resync
            .is_some_and(|at| now.duration_since(at) < RESYNC_THROTTLE)
        {
            return false;
        }
        self.last_resync = Some(now);
        true
    }

    /// Publishes a mark. Venues repeat an unchanged mark, so readers are
    /// only woken when the price is different.
    pub(crate) fn set_mark(&mut self, mark: Decimal, now: Instant) {
        if mark <= Decimal::ZERO {
            warn!(venue = %self.venue, coin = self.book.coin(), %mark, "mark is not a price");
            self.invalidate_mark();
            return;
        }
        self.last_mark = Some(now);
        let view = MarkView {
            venue: self.venue,
            coin: self.book.coin().to_string(),
            mark,
        };
        self.publisher.mark.send_if_modified(|slot| {
            if slot.as_ref() == Some(&view) {
                return false;
            }
            *slot = Some(view);
            true
        });
    }

    pub(crate) fn invalidate_mark(&mut self) {
        self.last_mark = None;
        self.publisher
            .mark
            .send_if_modified(|slot| slot.take().is_some());
    }

    /// Withdraws a mark the venue has stopped refreshing. Returns whether it
    /// did.
    pub(crate) fn expire_mark(&mut self, now: Instant) -> bool {
        let stale = self
            .last_mark
            .is_some_and(|at| now.duration_since(at) >= MARK_STALE_AFTER);
        if stale {
            self.invalidate_mark();
        }
        stale
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feed::{Feeds, MarkRx};
    use crate::risk::MarketId;
    use rust_decimal::dec;

    fn market() -> (Market, MarkRx) {
        let id = MarketId::new(Venue::Hyperliquid, "BTC");
        let (feeds, mut publishers) = Feeds::channels(std::slice::from_ref(&id));
        let publisher = publishers.take(Venue::Hyperliquid).remove("BTC").unwrap();
        let market = Market::new(Venue::Hyperliquid, "BTC".into(), publisher);
        (market, feeds.subscribe_mark(&id).unwrap())
    }

    #[test]
    fn unchanged_mark_does_not_wake_readers() {
        let (mut market, mut rx) = market();
        let now = Instant::now();

        market.set_mark(dec!(100), now);
        assert!(rx.has_changed().unwrap());
        assert_eq!(rx.borrow_and_update().as_ref().unwrap().mark, dec!(100));

        market.set_mark(dec!(100), now);
        assert!(!rx.has_changed().unwrap());

        market.set_mark(dec!(101), now);
        assert!(rx.has_changed().unwrap());
    }

    #[test]
    fn mark_expires_when_the_venue_stops_refreshing_it() {
        let (mut market, rx) = market();
        let start = Instant::now();
        market.set_mark(dec!(100), start);

        assert!(!market.expire_mark(start + MARK_STALE_AFTER - Duration::from_millis(1)));
        assert!(rx.borrow().is_some());

        // A repeat of the same price still counts as a refresh.
        market.set_mark(dec!(100), start + Duration::from_secs(5));
        assert!(!market.expire_mark(start + MARK_STALE_AFTER));

        assert!(market.expire_mark(start + Duration::from_secs(5) + MARK_STALE_AFTER));
        assert!(rx.borrow().is_none());
        assert!(!market.expire_mark(start + Duration::from_secs(60)));
    }

    #[test]
    fn non_positive_mark_withdraws_the_mark() {
        let (mut market, rx) = market();
        market.set_mark(dec!(100), Instant::now());
        market.set_mark(dec!(0), Instant::now());
        assert!(rx.borrow().is_none());
    }

    #[test]
    fn resync_is_throttled() {
        let (mut market, _rx) = market();
        assert!(market.resync());
        assert!(!market.resync());
    }
}
