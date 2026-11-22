//! Runtime configuration, read once from the environment at startup.

use std::net::SocketAddr;

use anyhow::{Context, Result, bail};

#[derive(Debug, Clone)]
pub struct Config {
    /// Hyperliquid WebSocket endpoint.
    pub hl_ws_url: String,
    /// Markets to track, in Hyperliquid's coin naming (for example `BTC`).
    pub coins: Vec<String>,
    /// Address the HTTP/WebSocket API listens on.
    pub bind: SocketAddr,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let hl_ws_url =
            std::env::var("HL_WS_URL").unwrap_or_else(|_| "wss://api.hyperliquid.xyz/ws".into());

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

        Ok(Self {
            hl_ws_url,
            coins,
            bind,
        })
    }
}
