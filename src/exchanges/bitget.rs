use async_trait::async_trait;
use rust_decimal::Decimal;
use tokio::sync::mpsc;
use reqwest::Client;
use serde_json::Value;

use crate::exchanges::{
    Exchange, ExchangeConfig, ExchangeError, ExchangeName, OrderBook, OrderRequest, OrderResponse,
    Position, SubscriptionConfig, TickerData,
};

pub struct Bitget {
    config: ExchangeConfig,
    client: Client,
}

impl Bitget {
    pub fn new(config: ExchangeConfig) -> Self {
        Self { 
            config,
            client: Client::new(),
        }
    }
}

#[async_trait]
impl Exchange for Bitget {
    fn name(&self) -> ExchangeName {
        ExchangeName::Bitget
    }

    fn config(&self) -> ExchangeConfig {
        self.config.clone()
    }

    async fn get_futures_tickers(&self) -> Result<Vec<TickerData>, ExchangeError> {
        let response = self
            .client
            .get("https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES")
            .send()
            .await?;

        let data: Value = response.json().await?;

        // Check if the response is successful
        if data["code"].as_str().unwrap_or("1") != "00000" {
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
                let best_bid = item["bidPr"].as_str()?.parse::<Decimal>().ok()?;
                let best_ask = item["askPr"].as_str()?.parse::<Decimal>().ok()?;
                let volume_24h = item["usdtVolume"].as_str()?.parse::<Decimal>().ok()?;

                Some(TickerData {
                    symbol: symbol.to_string(),
                    best_bid_price: best_bid,
                    best_ask_price: best_ask,
                    volume_24h,
                    multiplier: Decimal::from(1),
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
        todo!("Implement Bitget orderbook subscription")
    }

    async fn place_order(&self, _order: OrderRequest) -> Result<OrderResponse, ExchangeError> {
        todo!("Implement Bitget place order")
    }

    async fn close_position(
        &self,
        _position: &Position,
    ) -> Result<OrderResponse, ExchangeError> {
        todo!("Implement Bitget close position")
    }

    async fn get_open_positions(&self) -> Result<Vec<Position>, ExchangeError> {
        todo!("Implement Bitget get open positions")
    }
} 