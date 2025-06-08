mod bingx;
mod bitget;
mod bybit;
mod gate;
mod htx;
mod kucoin;
mod mexc;
mod okx;

pub use bybit::BybitExchange;
pub use kucoin::KuCoinExchange;
// pub use okx::OkxExchange;
// pub use bitget::BitgetExchange;
// pub use htx::HtxExchange;
pub use gate::GateExchange;
// pub use mexc::MexcExchange;
// pub use bingx::BingxExchange;

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
    pub buy_exchange: ExchangeName,
    pub sell_exchange: ExchangeName,
    pub buy_price: f64,
    pub sell_price: f64,
    pub potential_profit: f64,
    pub total_fees: f64,
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
    pub symbol: String,
    pub data_type: OrderBookDataType,
    pub bids: Vec<(f64, f64)>, // (price, size)
    pub asks: Vec<(f64, f64)>, // (price, size)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrderBookDataType {
    Snapshot,
    Delta
}

#[async_trait]
pub trait Exchange: Send + Sync {
    fn name(&self) -> ExchangeName;

    fn get_fees(&self) -> ExchangeFee;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExchangeName {
    Bybit,
    Kucoin,
    Gate,
    Bingx,
    BitGet,
    Htx,
    Mexc,
    Okx,
}
