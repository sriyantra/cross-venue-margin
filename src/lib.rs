//! Cross-venue margin monitor.
//!
//! One venue connector (Hyperliquid) keeps an in-memory L2 book and the
//! venue's mark price per market, served over an HTTP/WebSocket API. The
//! `risk` module computes account health and liquidation prices, and the
//! `engine` module runs paper accounts against the live marks.

pub mod book;
pub mod config;
pub mod engine;
pub mod feed;
pub mod risk;
pub mod server;
pub mod venue;
