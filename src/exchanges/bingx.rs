use async_trait::async_trait;
use rust_decimal::Decimal;
use tokio::sync::mpsc;
use reqwest::Client;
use serde_json::Value;

use crate::exchanges::{
    Exchange, ExchangeConfig, ExchangeError, ExchangeName, OrderBook, OrderRequest, OrderResponse,
    Position, SubscriptionConfig, TickerData,
};

pub struct Bingx {
    config: ExchangeConfig,
    client: Client,
}

impl Bingx {
    pub fn new(config: ExchangeConfig) -> Self {
        Self { 
            config,
            client: Client::new(),
        }
    }
}

#[async_trait]
impl Exchange for Bingx {
    fn name(&self) -> ExchangeName {
        ExchangeName::Bingx
    }

    fn config(&self) -> ExchangeConfig {
        self.config.clone()
    }

    async fn get_futures_tickers(&self) -> Result<Vec<TickerData>, ExchangeError> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let response = self
            .client
            .get(format!("https://open-api.bingx.com/openApi/swap/v1/ticker/price?timestamp={}", timestamp))
            .send()
            .await?;

        let data: Value = response.json().await?;

        // Check if the response is successful
        if data["code"].as_i64().unwrap() != 0 {
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
                let symbol = item["symbol"].as_str()?;
                let best_bid = item["price"].as_str()?.parse::<Decimal>().ok()?;
                let best_ask = item["price"].as_str()?.parse::<Decimal>().ok()?;
                let volume_24h = Decimal::from(10);

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
        todo!("Implement Bing orderbook subscription")
    }

    async fn place_order(&self, _order: OrderRequest) -> Result<OrderResponse, ExchangeError> {
        todo!("Implement Bing place order")
    }

    async fn close_position(
        &self,
        _order_id: &str,
        _symbol: &str,
        _place_order_side: crate::exchanges::OrderSide,
    ) -> Result<OrderResponse, ExchangeError> {
        todo!("Implement Bing close position")
    }

    async fn get_open_positions(&self) -> Result<Vec<Position>, ExchangeError> {
        todo!("Implement Bing get open positions")
    }
} 