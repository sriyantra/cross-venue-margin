//! Cross-venue margin monitor.
//!
//! One connector per venue keeps an in-memory L2 book and the venue's mark
//! price per market, served over an HTTP/WebSocket API. The `risk` module
//! computes account health and liquidation prices, the `engine` module
//! runs paper accounts against the live marks, and the `store` module keeps
//! the engine's events in Postgres.

pub mod book;
pub mod config;
pub mod engine;
pub mod execution;
pub mod feed;
pub mod metrics;
pub mod risk;
pub mod server;
pub mod store;
pub mod venue;
