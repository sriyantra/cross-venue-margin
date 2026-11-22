//! Runtime configuration, read once from the environment at startup.

use std::net::SocketAddr;

use anyhow::{Context, Result, bail};
use rust_decimal::Decimal;

#[derive(Debug, Clone)]
pub struct Config {
    /// Hyperliquid WebSocket endpoint.
    pub hl_ws_url: String,
    /// OKX public WebSocket endpoint.
    pub okx_ws_url: String,
    /// Coins to track on every venue that lists them (for example `BTC`).
    pub coins: Vec<String>,
    /// Address the HTTP/WebSocket API listens on.
    pub bind: SocketAddr,
    /// A margin call starts when equity falls below maintenance margin
    /// times this number.
    pub margin_call_multiple: Decimal,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let hl_ws_url =
            std::env::var("HL_WS_URL").unwrap_or_else(|_| "wss://api.hyperliquid.xyz/ws".into());

        let okx_ws_url = std::env::var("OKX_WS_URL")
            .unwrap_or_else(|_| "wss://ws.okx.com:443/ws/v5/public".into());

        let coins: Vec<String> = std::env::var("COINS")
            .unwrap_or_else(|_| "BTC,ETH".into())
            .split(',')
            .map(|c| c.trim().to_string())
            .filter(|c| !c.is_empty())
            .collect();
        if coins.is_empty() {
            bail!("COINS must name at least one market");
        }

        let bind = std::env::var("BIND")
            .unwrap_or_else(|_| "127.0.0.1:8080".into())
            .parse()
            .context("BIND must be a socket address such as 127.0.0.1:8080")?;

        let margin_call_multiple = std::env::var("MARGIN_CALL_MULTIPLE")
            .unwrap_or_else(|_| "1.5".into())
            .parse()
            .context("MARGIN_CALL_MULTIPLE must be a decimal such as 1.5")?;

        Ok(Self {
            hl_ws_url,
            okx_ws_url,
            coins,
            bind,
            margin_call_multiple,
        })
    }
}
