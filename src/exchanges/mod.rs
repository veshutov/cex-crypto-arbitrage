pub mod bybit;
pub mod gate;
pub mod kucoin;
pub mod gateway;

use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::mpsc;


#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub enum ExchangeName {
    Bybit,
    Kucoin,
    Gate,
}

#[async_trait]
pub trait Exchange: Send + Sync {
    fn name(&self) -> ExchangeName;
    fn config(&self) -> ExchangeConfig;
    async fn get_futures_tickers(&self) -> Result<Vec<TickerData>, ExchangeError>;
    async fn subscribe_orderbook(
        &mut self,
        config: SubscriptionConfig,
        sender: mpsc::UnboundedSender<OrderBookData>,
    ) -> Result<(), ExchangeError>;
}

#[derive(Clone, Debug)]
pub struct ExchangeConfig {
    pub maker_fee: Decimal,
    pub taker_fee: Decimal,
}

#[derive(Debug, Clone)]
pub struct SubscriptionConfig {
    pub symbols: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TickerData {
    pub symbol: String,
    pub best_bid_price: Decimal,
    pub best_ask_price: Decimal,
    pub volume_24h: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBookData {
    pub exchange_name: ExchangeName,
    pub symbol: String,
    pub best_bid_amount: Decimal,
    pub best_bid_price: Decimal,
    pub best_ask_price: Decimal,
    pub best_ask_amount: Decimal,
    pub timestamp: u64,
}

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
