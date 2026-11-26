//! Runtime configuration, read once from the environment at startup.

use std::net::SocketAddr;

use anyhow::{Context, Result, bail};
use rust_decimal::Decimal;

/// A value that must not reach the log. Its `Debug` output hides it.
#[derive(Clone)]
pub struct Secret(String);

impl Secret {
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[set]")
    }
}

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
    /// Maintenance rate for the matched part of a long and a short in the
    /// same coin on different venues. `None` turns netting off.
    pub hedge_margin_rate: Option<Decimal>,
    /// Postgres URL for the event log. Without it events are only logged,
    /// and accounts do not survive a restart.
    pub database_url: Option<Secret>,
    /// When set, the API refuses every request that would change an
    /// account. Used for a deployment that is open to the public.
    pub read_only: bool,
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

        let hedge_margin_rate = match std::env::var("HEDGE_MARGIN_RATE") {
            Err(_) => Some(Decimal::new(25, 4)),
            Ok(value) if value == "off" => None,
            Ok(value) => Some(
                value
                    .parse()
                    .context("HEDGE_MARGIN_RATE must be a decimal such as 0.0025, or off")?,
            ),
        };

        let database_url = std::env::var("DATABASE_URL")
            .ok()
            .filter(|url| !url.is_empty())
            .map(Secret);

        let read_only = match std::env::var("READ_ONLY") {
            Err(_) => false,
            Ok(value) => match value.as_str() {
                "" | "0" | "false" => false,
                "1" | "true" => true,
                _ => bail!("READ_ONLY must be true or false"),
            },
        };

        Ok(Self {
            hl_ws_url,
            okx_ws_url,
            coins,
            bind,
            margin_call_multiple,
            hedge_margin_rate,
            database_url,
            read_only,
        })
    }
}

/// The haircut on a coin held as collateral, or `None` if the coin is not
/// accepted. These are this service's own numbers, not a venue's.
pub fn collateral_haircut(coin: &str) -> Option<Decimal> {
    match coin {
        "BTC" => Some(Decimal::new(10, 2)),
        "ETH" => Some(Decimal::new(15, 2)),
        _ => None,
    }
}
