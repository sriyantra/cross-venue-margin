//! Venue connectors. Each venue gets its own module that turns the venue's
//! wire format into `book` operations and publishes `feed::BookView`s.

pub mod hyperliquid;
