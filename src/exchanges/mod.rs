mod bybit;
mod kucoin;

pub use bybit::BybitExchange;
pub use kucoin::KuCoinExchange;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ExchangeError {
    #[error("API request failed: {0}")]
    RequestError(#[from] reqwest::Error),
    #[error("Invalid response: {0}")]
    InvalidResponse(String),
    #[error("Rate limit exceeded")]
    RateLimitExceeded,
    #[error("Authentication failed")]
    AuthenticationFailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderType {
    Limit,
    Market,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeFee {
    pub maker_fee: f64,
    pub taker_fee: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArbitrageOpportunity {
    pub symbol: String,
    pub buy_exchange: String,
    pub sell_exchange: String,
    pub buy_price: f64,
    pub sell_price: f64,
    pub potential_profit: f64,
    pub total_fees: f64,
}

#[async_trait]
pub trait Exchange: Send + Sync {
    fn name(&self) -> &'static str;
    async fn get_futures_tickers(&self) -> Result<Vec<String>, ExchangeError>;
    async fn get_ticker_price(&self, symbol: &str) -> Result<f64, ExchangeError>;
    fn get_fees(&self, order_type: OrderType) -> ExchangeFee;
} 