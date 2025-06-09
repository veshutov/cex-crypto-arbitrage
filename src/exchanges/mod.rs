pub mod bybit;
pub mod gate;
pub mod kucoin;
pub mod gateway;

use async_trait::async_trait;
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
    pub maker_fee: f64,
    pub taker_fee: f64,
}

#[derive(Debug, Clone)]
pub struct SubscriptionConfig {
    pub symbols: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TickerData {
    pub symbol: String,
    pub best_bid_price: f64,
    pub best_ask_price: f64,
    pub volume_24h: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBookData {
    pub exchange_name: ExchangeName,
    pub symbol: String,
    pub best_bid_amount: f64,
    pub best_bid_price: f64,
    pub best_ask_price: f64,
    pub best_ask_amount: f64,
    pub timestamp: u64,
}

#[async_trait]
pub trait ExchangeClient: Send + Sync {
    async fn get_futures_tickers(&self) -> Result<Vec<TickerData>, ExchangeError>;

    async fn subscribe_orderbook<C, Fut>(
        &self,
        symbol: String,
        callback: C,
    ) -> Result<(), ExchangeError>
    where
        C: FnMut(OrderBookData) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send;
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
