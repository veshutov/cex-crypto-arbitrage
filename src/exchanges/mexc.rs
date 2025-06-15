use async_trait::async_trait;
use rust_decimal::Decimal;
use tokio::sync::mpsc;
use reqwest::Client;
use serde_json::Value;

use crate::exchanges::{
    Exchange, ExchangeConfig, ExchangeError, ExchangeName, OrderBook, OrderRequest, OrderResponse,
    Position, SubscriptionConfig, TickerData,
};

pub struct Mexc {
    config: ExchangeConfig,
    client: Client,
}

impl Mexc {
    pub fn new(config: ExchangeConfig) -> Self {
        Self { 
            config,
            client: Client::new(),
        }
    }
}

#[async_trait]
impl Exchange for Mexc {
    fn name(&self) -> ExchangeName {
        ExchangeName::Mexc
    }

    fn config(&self) -> ExchangeConfig {
        self.config.clone()
    }

    async fn get_futures_tickers(&self) -> Result<Vec<TickerData>, ExchangeError> {
        let response = self
            .client
            .get("https://contract.mexc.com/api/v1/contract/ticker")
            .send()
            .await?;

        let data: Value = response.json().await?;

        // Check if the response is successful
        if data["code"].as_i64().unwrap() != 0 {
            println!("Eror response from mexc {}", data);
            return Err(ExchangeError::InvalidResponse(format!(
                "API error: {}",
                data["msg"].as_str().unwrap_or("Unknown error")
            )));
        }

        let tickers = data["data"]
            .as_array()
            .ok_or_else(|| ExchangeError::InvalidResponse("Invalid response format".to_string()))?
            .iter()
            .filter_map(|item| {
                let symbol = item["symbol"].to_string();
                let best_bid = item["bid1"].to_string().parse::<Decimal>().ok()?;
                let best_ask = item["ask1"].to_string().parse::<Decimal>().ok()?;
                let volume_24h = item["volume24"].to_string().parse::<Decimal>().ok()?;

                if !symbol.contains("USDT") {
                    return None;
                }

                Some(TickerData {
                    symbol: symbol.to_string(),
                    best_bid_price: best_bid,
                    best_ask_price: best_ask,
                    volume_24h,
                })
            })
            .collect();

        Ok(tickers)
    }

    async fn subscribe_orderbook(
        &self,
        _config: SubscriptionConfig,
        _sender: mpsc::UnboundedSender<OrderBook>,
    ) -> Result<(), ExchangeError> {
        todo!("Implement MEXC orderbook subscription")
    }

    async fn place_order(&self, _order: OrderRequest) -> Result<OrderResponse, ExchangeError> {
        todo!("Implement MEXC place order")
    }

    async fn close_position(
        &self,
        _position: &Position,
    ) -> Result<OrderResponse, ExchangeError> {
        todo!("Implement MEXC close position")
    }

    async fn get_open_positions(&self) -> Result<Vec<Position>, ExchangeError> {
        todo!("Implement MEXC get open positions")
    }
} 