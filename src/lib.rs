//! Cross-venue margin monitor.
//!
//! One venue connector (Hyperliquid) keeps an in-memory L2 book and the
//! venue's mark price per market, served over an HTTP/WebSocket API. The
//! `risk` module computes account health and liquidation prices.

pub mod book;
pub mod config;
pub mod feed;
pub mod risk;
pub mod server;
pub mod venue;
