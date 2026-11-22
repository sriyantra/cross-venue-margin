//! Cross-venue margin monitor.
//!
//! Stage 1: one venue connector (Hyperliquid), an in-memory L2 book per
//! market, and an HTTP/WebSocket API that serves the current book.

pub mod book;
pub mod config;
pub mod feed;
pub mod server;
pub mod venue;
